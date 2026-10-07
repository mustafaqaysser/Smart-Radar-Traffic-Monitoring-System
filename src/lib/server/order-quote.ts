import 'server-only';
import { and, count, eq, gt, inArray, ne, or } from 'drizzle-orm';
import restaurantConfig from '@config';
import { db } from '@/lib/db/client';
import * as s from '@/lib/db/schema';
import { checkGiftCard, normalizeGiftCardCode, type GiftCardFailure } from '@/lib/domain/gift-cards';
import { scheduleForDate } from '@/lib/domain/hours';
import { maxRedeemablePoints, redemptionValue } from '@/lib/domain/loyalty';
import { activeSeasons, isServing, menuAvailable } from '@/lib/domain/menus';
import { lineUnitPrice, priceCart, validateModifiers, type ModifierGroupDef, type PricingResult } from '@/lib/domain/pricing';
import { evaluatePromotion, normalizePromoCode, type PromotionFailure } from '@/lib/domain/promotions';
import { earliestAsap, prepMinutes, scheduleSlots, type OrderingInterval, type ThrottleRules } from '@/lib/domain/throttle';
import { tr, type LocalizedText } from '@/lib/i18n/localized';
import { getSeasonalModes, hoursFor } from '@/lib/queries/branches';
import { getMenuCatalog } from '@/lib/queries/catalog';
import type { BranchDTO, MenuCatalog, MenuItemDTO, SeasonalModeDTO, ZoneDTO } from '@/lib/queries/types';
import { getSettings, type SiteSettings } from '@/lib/server/settings';
import { distanceKm } from '@/lib/services/maps';
import { addDays, localToUtc, toDateString } from '@/lib/time/zoned';

const MINUTE = 60_000;
/** Orders that still occupy the kitchen (their promise counts against capacity). */
export const ACTIVE_ORDER_STATUSES: s.OrderStatus[] = ['pending_payment', 'placed', 'accepted', 'preparing', 'ready', 'out_for_delivery'];

export interface CartLineInput {
  slug: string;
  qty: number;
  optionIds: string[];
  note: string;
}

export type TipInput = { kind: 'percent'; value: number } | { kind: 'amount'; value: number } | null;

export interface QuoteInput {
  branch: BranchDTO;
  channel: s.OrderChannel;
  lines: CartLineInput[];
  /** ASAP, or the time the guest wants the food (delivery arrival or pickup). */
  when: { asap: true } | { asap: false; at: Date };
  zoneId?: string | null;
  location?: { lat: number; lng: number } | null;
  promoCode?: string | null;
  giftCardCode?: string | null;
  loyaltyPoints?: number;
  tip?: TipInput;
  user?: { id: string; email: string } | null;
  email?: string | null;
  now: Date;
  locale: string;
}

export type LineProblem = 'unknown' | 'unavailable' | 'sold_out' | 'not_served' | 'modifiers' | 'not_orderable';

export interface QuotedLine {
  key: string;
  slug: string;
  itemId: string | null;
  name: string;
  nameLocalized: LocalizedText;
  qty: number;
  unitPrice: number;
  lineTotal: number;
  options: { groupId: string; optionId: string; name: LocalizedText; priceDelta: number }[];
  note: string;
  prepMinutes: number;
  problem: LineProblem | null;
}

export type TimingProblem = 'paused' | 'closed' | 'full' | 'not_served';

export interface TimeOption {
  /** What the guest sees: arrival for delivery, collection for pickup (ISO). */
  at: string;
  available: boolean;
  /** Every item in the order is served when the kitchen cooks it. */
  servesAll: boolean;
}

export interface Quote {
  lines: QuotedLine[];
  pricing: PricingResult;
  promo: { code: string; ok: true; discount: number } | { code: string; ok: false; reason: PromotionFailure | 'unknown'; minOrder?: number } | null;
  promotionId: string | null;
  giftCard: { code: string; ok: true; id: string; balance: number; applied: number } | { code: string; ok: false; reason: GiftCardFailure } | null;
  loyalty: { balance: number; maxPoints: number; maxValue: number; points: number; value: number } | null;
  zone: ZoneDTO | null;
  zoneProblem: 'required' | 'out_of_range' | 'unknown' | 'location_required' | null;
  timing: {
    prepMinutes: number;
    travelMinutes: number;
    /** Kitchen-ready time of the chosen option (ISO). */
    readyAt: string | null;
    /** Guest-facing time of the chosen option (ISO). */
    promisedAt: string | null;
    asapAvailable: boolean;
    asapAt: string | null;
    options: TimeOption[];
    problem: TimingProblem | null;
  };
  tipAllowed: boolean;
  serviceChargeRate: number;
  taxRate: number;
  problems: string[];
  canPlace: boolean;
}

/** Kitchen ordering intervals from now over the next days, in the branch's zone (after-midnight ranges included). */
export function orderingIntervals(branch: BranchDTO, modes: SeasonalModeDTO[], now: Date, days: number): OrderingInterval[] {
  const hours = hoursFor(branch, modes, 'kitchen');
  const today = toDateString(now, branch.timeZone);
  const out: OrderingInterval[] = [];
  for (let i = -1; i <= days; i++) {
    const date = addDays(today, i);
    const schedule = scheduleForDate(hours, date);
    if (schedule.closed) continue;
    for (const r of schedule.ranges) {
      const start = localToUtc(date, r.start, branch.timeZone, true);
      const end = localToUtc(date, r.end, branch.timeZone, true);
      if (start && end && end > now) out.push({ start, end });
    }
  }
  return out.sort((a, b) => a.start.getTime() - b.start.getTime());
}

/** Item slugs that can be ordered when the kitchen cooks at `at` (menus served then, at this branch and season). */
export function servedSlugs(catalog: MenuCatalog, branch: BranchDTO, modes: SeasonalModeDTO[], at: Date): Set<string> {
  const date = toDateString(at, branch.timeZone);
  const seasons = new Set(activeSeasons(modes, date).map((m) => m.id));
  const slugs = new Set<string>();
  for (const menu of catalog.menus) {
    if (menu.isTasting || !menuAvailable(menu, branch.id, seasons) || !isServing(menu, at, branch.timeZone)) continue;
    for (const c of menu.categories) for (const slug of c.items) slugs.add(slug);
  }
  return slugs;
}

export function itemOrderable(item: MenuItemDTO | undefined, branchId: string, allowAlcohol: boolean): LineProblem | null {
  if (!item || !item.isActive) return 'unknown';
  if (!item.orderable || (item.isAlcoholic && !allowAlcohol)) return 'not_orderable';
  const state = item.branches[branchId];
  if (state && !state.available) return 'unavailable';
  if (state?.soldOut) return 'sold_out';
  return null;
}

export function throttleRules(branch: BranchDTO): ThrottleRules {
  const o = branch.orderingSettings;
  return { maxOrdersPerWindow: o.maxOrdersPerWindow, windowMinutes: o.windowMinutes, basePrepMinutes: o.basePrepMinutes, busyExtraMinutes: o.busyExtraMinutes };
}

type Tx = Parameters<Parameters<typeof db.transaction>[0]>[0];

/** Kitchen-ready times promised to active orders at the branch (pass the transaction when checking inside a write). */
export async function kitchenPromises(branch: BranchDTO, now: Date, exec: typeof db | Tx = db): Promise<Date[]> {
  const rows = await exec
    .select({ id: s.orders.id, promisedAt: s.orders.promisedAt, channel: s.orders.channel, zoneId: s.orders.zoneId })
    .from(s.orders)
    .where(and(eq(s.orders.branchId, branch.id), inArray(s.orders.status, ACTIVE_ORDER_STATUSES), gt(s.orders.promisedAt, new Date(now.getTime() - 3 * 60 * MINUTE))));
  const eta = new Map(branch.zones.map((z) => [z.id, z.etaMinutes]));
  return rows
    .filter((r) => r.promisedAt)
    .map((r) => new Date((r.promisedAt as Date).getTime() - (r.channel === 'delivery' ? (eta.get(r.zoneId ?? '') ?? 0) : 0) * MINUTE));
}

function resolveZone(branch: BranchDTO, zoneId: string | null | undefined, location: { lat: number; lng: number } | null | undefined): { zone: ZoneDTO | null; problem: Quote['zoneProblem'] } {
  if (!zoneId) return { zone: null, problem: 'required' };
  const zone = branch.zones.find((z) => z.id === zoneId);
  if (!zone) return { zone: null, problem: 'unknown' };
  if (zone.kind === 'radius') {
    if (!location) return { zone, problem: 'location_required' };
    if (distanceKm({ lat: branch.lat, lng: branch.lng }, location) > (zone.radiusKm ?? 0)) return { zone, problem: 'out_of_range' };
  }
  return { zone, problem: null };
}

function channelEnabled(branch: BranchDTO, channel: s.OrderChannel, settings: SiteSettings): boolean {
  if (!settings.features.ordering) return false;
  if (channel === 'delivery') return settings.features.delivery && branch.deliveryEnabled;
  if (channel === 'pickup') return settings.features.pickup && branch.pickupEnabled;
  return settings.features.dineInQr && branch.dineInEnabled;
}

/**
 * Prices and checks an order against the live catalogue, the kitchen's hours and capacity, the delivery
 * zone, and any promo code, gift card and loyalty points. Used for the live checkout summary and again,
 * inside the write, when the order is placed.
 */
export async function quoteOrder(input: QuoteInput, preloaded?: { catalog?: MenuCatalog; modes?: SeasonalModeDTO[]; settings?: SiteSettings }): Promise<Quote> {
  const { branch, channel, now } = input;
  const [catalog, modes, settings] = await Promise.all([preloaded?.catalog ?? getMenuCatalog(), preloaded?.modes ?? getSeasonalModes(), preloaded?.settings ?? getSettings()]);
  const allowAlcohol = settings.features.alcohol;
  const problems: string[] = [];

  // ——— Lines
  const lines: QuotedLine[] = input.lines.slice(0, 60).map((l) => {
    const item = catalog.items[l.slug];
    const qty = Math.max(1, Math.min(20, Math.round(l.qty)));
    const key = `${l.slug}|${[...l.optionIds].sort().join('.')}|${l.note.trim().toLowerCase()}`;
    if (!item) {
      return { key, slug: l.slug, itemId: null, name: l.slug, nameLocalized: { ar: l.slug, en: l.slug }, qty, unitPrice: 0, lineTotal: 0, options: [], note: l.note, prepMinutes: 0, problem: 'unknown' as const };
    }
    const groups: ModifierGroupDef[] = item.modifierGroups.map((g) => ({ id: g.id, minSelect: g.minSelect, maxSelect: g.maxSelect, options: g.options.map((o) => ({ id: o.id, priceDelta: o.priceDelta, isAvailable: o.isAvailable })) }));
    const base = item.branches[branch.id]?.price ?? item.price;
    const unitPrice = lineUnitPrice({ unitPrice: base, optionIds: l.optionIds, groups });
    const options = l.optionIds.flatMap((id) => {
      for (const g of item.modifierGroups) {
        const o = g.options.find((x) => x.id === id);
        if (o) return [{ groupId: g.id, optionId: o.id, name: o.name, priceDelta: o.priceDelta }];
      }
      return [];
    });
    let problem: LineProblem | null = itemOrderable(item, branch.id, allowAlcohol);
    if (!problem && validateModifiers(groups, l.optionIds).length) problem = 'modifiers';
    return { key, slug: l.slug, itemId: item.id, name: tr(item.name, input.locale), nameLocalized: item.name, qty, unitPrice, lineTotal: unitPrice * qty, options, note: l.note.trim().slice(0, 140), prepMinutes: item.prepMinutes, problem };
  });
  if (!lines.length) problems.push('empty');

  // ——— Channel and zone
  if (!channelEnabled(branch, channel, settings)) problems.push('channel');
  let zone: ZoneDTO | null = null;
  let zoneProblem: Quote['zoneProblem'] = null;
  if (channel === 'delivery') {
    ({ zone, problem: zoneProblem } = resolveZone(branch, input.zoneId, input.location));
    if (zoneProblem) problems.push(`zone_${zoneProblem}`);
  }
  const travel = channel === 'delivery' && zone ? zone.etaMinutes : 0;

  // ——— Timing: kitchen hours, capacity (throttling) and what is served then.
  const rules = throttleRules(branch);
  const prep = prepMinutes(rules, branch.busyMode, Math.max(0, ...lines.map((l) => l.prepMinutes)));
  const intervals = orderingIntervals(branch, modes, now, restaurantConfig.ordering.scheduleDaysAhead);
  const promises = await kitchenPromises(branch, now);
  const asapBuffer = restaurantConfig.ordering.asapBufferMinutes;
  const current = intervals.find((iv) => iv.start <= now && now < iv.end);
  const asapReady = current ? earliestAsap(now, prep, rules, promises, [current]) : null;
  const slugsAt = (at: Date) => servedSlugs(catalog, branch, modes, at);
  const servesAll = (ready: Date) => {
    const served = slugsAt(ready);
    return lines.every((l) => served.has(l.slug));
  };
  const options: TimeOption[] = scheduleSlots(now, prep + asapBuffer, rules, promises, intervals, restaurantConfig.ordering.slotMinutes).map((slot) => ({
    at: new Date(slot.at.getTime() + travel * MINUTE).toISOString(),
    available: slot.available,
    servesAll: servesAll(slot.at),
  }));

  let readyAt: Date | null = null;
  let timingProblem: TimingProblem | null = null;
  if (branch.orderingPaused) timingProblem = 'paused';
  else if (input.when.asap) {
    if (!asapReady) timingProblem = intervals.length ? 'full' : 'closed';
    else {
      readyAt = asapReady;
      if (!servesAll(asapReady)) timingProblem = 'not_served';
    }
  } else {
    const wanted = input.when.at.getTime();
    const ready = new Date(wanted - travel * MINUTE);
    const match = options.find((o) => Math.abs(Date.parse(o.at) - wanted) < MINUTE);
    if (!match) timingProblem = 'closed';
    else if (!match.available) timingProblem = 'full';
    else {
      readyAt = ready;
      if (!match.servesAll) timingProblem = 'not_served';
    }
  }
  if (timingProblem) problems.push(`time_${timingProblem}`);
  // Lines that will not be served at the chosen time.
  if (readyAt) {
    const served = slugsAt(readyAt);
    for (const l of lines) if (!l.problem && !served.has(l.slug)) l.problem = 'not_served';
  }
  if (lines.some((l) => l.problem)) problems.push('lines');

  // ——— Promo code
  const email = (input.user?.email ?? input.email ?? '').toLowerCase();
  const subtotal = lines.filter((l) => !l.problem || l.problem === 'not_served').reduce((sum, l) => sum + l.lineTotal, 0);
  let promo: Quote['promo'] = null;
  let promotionId: string | null = null;
  if (input.promoCode) {
    const code = normalizePromoCode(input.promoCode);
    const row = code ? await db.query.promotions.findFirst({ where: eq(s.promotions.code, code) }) : null;
    if (!row) promo = { code, ok: false, reason: 'unknown' };
    else {
      const [prior, uses] = await Promise.all([
        email || input.user
          ? db
              .select({ n: count() })
              .from(s.orders)
              .where(and(input.user ? or(eq(s.orders.userId, input.user.id), eq(s.orders.email, email)) : eq(s.orders.email, email), ne(s.orders.status, 'cancelled'), ne(s.orders.status, 'rejected')))
          : Promise.resolve([{ n: 0 }]),
        email ? db.select({ n: count() }).from(s.promotionRedemptions).where(and(eq(s.promotionRedemptions.promotionId, row.id), eq(s.promotionRedemptions.email, email))) : Promise.resolve([{ n: 0 }]),
      ]);
      const result = evaluatePromotion(
        { ...row, maxDiscount: row.maxDiscount, branchIds: row.branchIds, channels: row.channels },
        { now, subtotal, branchId: branch.id, channel, isFirstOrder: (prior[0]?.n ?? 0) === 0, customerUses: uses[0]?.n ?? 0 },
      );
      promo = result.ok ? { code, ok: true, discount: result.discount } : { code, ok: false, reason: result.reason, minOrder: result.minOrder };
      if (result.ok) promotionId = row.id;
    }
  }
  const promoDiscount = promo?.ok ? promo.discount : 0;

  // ——— Loyalty points (signed-in guests)
  let loyalty: Quote['loyalty'] = null;
  if (input.user && settings.features.loyalty) {
    const u = await db.query.users.findFirst({ where: eq(s.users.id, input.user.id), columns: { loyaltyPoints: true } });
    const balance = u?.loyaltyPoints ?? 0;
    const loyaltyRules = { ...restaurantConfig.loyalty, tiers: [...restaurantConfig.loyalty.tiers] };
    const maxPoints = maxRedeemablePoints(balance, Math.max(0, subtotal - promoDiscount), loyaltyRules);
    const step = restaurantConfig.loyalty.pointsPerUnitRedeemed;
    const points = Math.min(maxPoints, Math.floor(Math.max(0, input.loyaltyPoints ?? 0) / step) * step);
    loyalty = { balance, maxPoints, maxValue: redemptionValue(maxPoints, loyaltyRules), points, value: redemptionValue(points, loyaltyRules) };
  }

  // ——— Gift card
  let giftCard: Quote['giftCard'] = null;
  let giftBalance = 0;
  let giftCardId: string | null = null;
  if (input.giftCardCode && settings.features.giftCards) {
    const code = normalizeGiftCardCode(input.giftCardCode);
    const card = await db.query.giftCards.findFirst({ where: eq(s.giftCards.code, code) });
    const check = checkGiftCard(card ? { balance: card.balance, status: card.status, expiresAt: card.expiresAt } : null, now);
    if (!check.ok || !card) giftCard = { code, ok: false, reason: check.ok ? 'not_found' : check.reason };
    else {
      giftBalance = card.balance;
      giftCardId = card.id;
    }
  }

  // ——— Tip, service charge, tax
  const tipAllowed = settings.tips.enabled && restaurantConfig.tips.channels.includes(channel);
  const tip = tipAllowed && input.tip ? (input.tip.kind === 'percent' ? { kind: 'percent' as const, value: Math.min(0.3, Math.max(0, input.tip.value)) } : { kind: 'amount' as const, value: Math.min(100_000, Math.max(0, Math.round(input.tip.value))) }) : null;
  const serviceApplies = settings.serviceCharge.channels.includes(channel);

  const pricing = priceCart({
    lines: lines
      .filter((l) => !l.problem || l.problem === 'not_served')
      .map((l) => ({ key: l.key, unitPrice: l.unitPrice, quantity: l.qty, optionIds: [], groups: [] })),
    config: { taxRate: settings.tax.rate, pricesIncludeTax: settings.tax.pricesIncludeTax, serviceChargeRate: settings.serviceCharge.rate, serviceChargeApplies: serviceApplies },
    zone: zone && !zoneProblem ? { fee: zone.fee, minOrder: zone.minOrder } : null,
    promoDiscount,
    loyaltyDiscount: loyalty?.value ?? 0,
    tip,
    giftCardBalance: giftBalance,
  });
  if (giftCardId) giftCard = { code: normalizeGiftCardCode(input.giftCardCode ?? ''), ok: true, id: giftCardId, balance: giftBalance, applied: pricing.giftCardAmount };
  if (pricing.belowMinimum) problems.push('below_minimum');

  return {
    lines,
    pricing,
    promo,
    promotionId,
    giftCard,
    loyalty,
    zone,
    zoneProblem,
    timing: {
      prepMinutes: prep,
      travelMinutes: travel,
      readyAt: readyAt?.toISOString() ?? null,
      promisedAt: readyAt ? new Date(readyAt.getTime() + travel * MINUTE).toISOString() : null,
      asapAvailable: Boolean(asapReady) && !branch.orderingPaused,
      asapAt: asapReady ? new Date(asapReady.getTime() + travel * MINUTE).toISOString() : null,
      options,
      problem: timingProblem,
    },
    tipAllowed,
    serviceChargeRate: serviceApplies ? settings.serviceCharge.rate : 0,
    taxRate: settings.tax.rate,
    problems,
    canPlace: problems.length === 0,
  };
}

export interface KitchenStatus {
  paused: boolean;
  busy: boolean;
  /** The kitchen is cooking now and can take an ASAP order. */
  open: boolean;
  /** Minutes until an ASAP order would be ready (pickup), when open. */
  readyInMinutes: number | null;
  /** Start of the next kitchen opening when closed now (ISO). */
  nextOpen: string | null;
}

/** What the order page says about the kitchen right now. */
export async function kitchenStatus(branch: BranchDTO, now: Date): Promise<KitchenStatus> {
  const modes = await getSeasonalModes();
  const rules = throttleRules(branch);
  const intervals = orderingIntervals(branch, modes, now, restaurantConfig.ordering.scheduleDaysAhead);
  const current = intervals.find((iv) => iv.start <= now && now < iv.end);
  const prep = prepMinutes(rules, branch.busyMode);
  const ready = current && !branch.orderingPaused ? earliestAsap(now, prep, rules, await kitchenPromises(branch, now), [current]) : null;
  const next = intervals.find((iv) => iv.start > now);
  return {
    paused: branch.orderingPaused,
    busy: branch.busyMode,
    open: Boolean(ready),
    readyInMinutes: ready ? Math.max(prep, Math.round((ready.getTime() - now.getTime()) / MINUTE)) : null,
    nextOpen: !ready && next ? next.start.toISOString() : null,
  };
}
