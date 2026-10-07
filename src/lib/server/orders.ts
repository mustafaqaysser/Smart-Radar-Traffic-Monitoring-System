import 'server-only';
import { and, asc, desc, eq, gte, inArray, isNull, like, lt, or, sql } from 'drizzle-orm';
import { getTranslations } from 'next-intl/server';
import restaurantConfig from '@config';
import { db } from '@/lib/db/client';
import * as s from '@/lib/db/schema';
import { pointsEarned } from '@/lib/domain/loyalty';
import { countInWindow, windowStart } from '@/lib/domain/throttle';
import { formatDateTime, formatMoney, formatNumber, joinParts } from '@/lib/i18n/format';
import { tr } from '@/lib/i18n/localized';
import { getBranch } from '@/lib/queries/branches';
import { getMenuCatalog } from '@/lib/queries/catalog';
import type { BranchDTO } from '@/lib/queries/types';
import { audit, notifyStaff } from '@/lib/server/audit';
import { mail } from '@/lib/server/mail';
import { linkToken, verifyLinkToken } from '@/lib/server/tokens';
import { absoluteUrl } from '@/lib/site/url';
import { createId, sha256 } from '@/lib/utils/id';
import { itemOrderable, kitchenPromises, quoteOrder, throttleRules, type CartLineInput, type Quote, type QuoteInput } from './order-quote';
import type { StartedPayment } from './payments';

type Tx = Parameters<Parameters<typeof db.transaction>[0]>[0];
export type Order = typeof s.orders.$inferSelect;
export type OrderItem = typeof s.orderItems.$inferSelect;
export type PaymentMethod = Order['paymentMethod'];

const MINUTE = 60_000;
/** An order waiting for card payment keeps its kitchen slot, promo, gift card and points this long. */
export const PAYMENT_WINDOW_MINUTES = 30;

class OrderConflict extends Error {
  constructor(readonly code: 'time_full' | 'promo_limit' | 'gift_balance' | 'loyalty_balance') {
    super(code);
  }
}

// ————————————————————————————————————————— links —————————————————————————————————————————

export function trackingPath(o: Pick<Order, 'id' | 'number'>): string {
  return `/order/track/${o.number}?token=${linkToken('order', o.id)}`;
}

export function trackingUrl(o: Pick<Order, 'id' | 'number' | 'locale'>): string {
  return absoluteUrl(`/${o.locale}${trackingPath(o)}`);
}

export async function orderByNumber(number: string, token: string | null | undefined): Promise<Order | null> {
  if (!/^[A-Z]{1,4}-\d{3,8}$/.test(number)) return null;
  const order = await db.query.orders.findFirst({ where: eq(s.orders.number, number) });
  return order && verifyLinkToken('order', order.id, token) ? order : null;
}

// ————————————————————————————————————————— placing an order —————————————————————————————————————————

/** Next number in the house's sequence (e.g. BL-1044), read inside the write transaction. */
async function nextOrderNumber(tx: Tx, branch: BranchDTO): Promise<string> {
  const latest = await tx.select({ number: s.orders.number }).from(s.orders).where(eq(s.orders.branchId, branch.id)).orderBy(desc(s.orders.createdAt)).limit(1);
  const fallback = branch.slug
    .split('-')
    .map((part) => part[0] ?? '')
    .join('')
    .toUpperCase()
    .slice(0, 3) || 'Z';
  const prefix = latest[0]?.number.split('-')[0] ?? fallback;
  const rows = await tx.select({ number: s.orders.number }).from(s.orders).where(like(s.orders.number, `${prefix}-%`));
  const max = rows.reduce((m, r) => Math.max(m, Number(r.number.split('-')[1]) || 0), 1000);
  return `${prefix}-${max + 1}`;
}

export interface PlaceOrderInput extends QuoteInput {
  name: string;
  email: string;
  phone: string;
  address: s.OrderAddress | null;
  notes: string | null;
  paymentMethod: 'card' | 'cash' | 'pay_at_venue';
  tableId?: string | null;
  /** Save the delivery address to the guest's address book under this label. */
  saveAddressAs?: string | null;
}

export type PlaceResult = { ok: true; order: Order; payment: StartedPayment | null } | { ok: false; error: string; quote?: Quote };

/**
 * Places an order. The quote is recomputed, then a single write transaction re-checks kitchen capacity and
 * reserves the promo use, gift card amount and loyalty points before the order exists — two guests can never
 * spend the same balance or overfill the same kitchen window.
 */
export async function placeOrder(input: PlaceOrderInput): Promise<PlaceResult> {
  const quote = await quoteOrder(input);
  if (!quote.canPlace || !quote.timing.readyAt || !quote.timing.promisedAt) return { ok: false, error: 'quote', quote };
  const due = quote.pricing.amountDue;
  const method: PaymentMethod = due === 0 ? 'gift_card' : input.paymentMethod;
  if (method === 'cash' && input.channel !== 'delivery') return { ok: false, error: 'payment_method' };
  if (method === 'pay_at_venue' && input.channel === 'delivery') return { ok: false, error: 'payment_method' };

  const { branch, now } = input;
  const id = createId();
  const ready = new Date(quote.timing.readyAt);
  const email = input.email.toLowerCase();
  const pendingPayment = method === 'card';
  try {
    const order = await db.transaction(async (tx) => {
      const rules = throttleRules(branch);
      const promises = await kitchenPromises(branch, now, tx);
      if (countInWindow(promises, windowStart(ready, rules.windowMinutes), rules.windowMinutes) >= rules.maxOrdersPerWindow) throw new OrderConflict('time_full');
      const number = await nextOrderNumber(tx, branch);

      if (quote.promotionId && quote.promo?.ok) {
        const used = await tx
          .update(s.promotions)
          .set({ usedCount: sql`${s.promotions.usedCount} + 1` })
          .where(and(eq(s.promotions.id, quote.promotionId), or(isNull(s.promotions.usageLimit), lt(s.promotions.usedCount, s.promotions.usageLimit))))
          .returning({ id: s.promotions.id });
        if (!used[0]) throw new OrderConflict('promo_limit');
        await tx.insert(s.promotionRedemptions).values({ id: createId(), promotionId: quote.promotionId, orderId: id, email, amount: quote.pricing.discount });
      }

      const giftApplied = quote.giftCard?.ok ? quote.pricing.giftCardAmount : 0;
      if (quote.giftCard?.ok && giftApplied > 0) {
        const spent = await tx
          .update(s.giftCards)
          .set({
            balance: sql`${s.giftCards.balance} - ${giftApplied}`,
            status: sql`case when ${s.giftCards.balance} - ${giftApplied} <= 0 then 'redeemed' else ${s.giftCards.status} end`,
            updatedAt: now,
          })
          .where(and(eq(s.giftCards.id, quote.giftCard.id), gte(s.giftCards.balance, giftApplied)))
          .returning({ id: s.giftCards.id });
        if (!spent[0]) throw new OrderConflict('gift_balance');
        await tx.insert(s.giftCardTransactions).values({ id: createId(), giftCardId: quote.giftCard.id, kind: 'redeem', amount: -giftApplied, orderId: id, note: number });
      }

      const points = quote.loyalty?.points ?? 0;
      if (points > 0 && input.user) {
        const taken = await tx
          .update(s.users)
          .set({ loyaltyPoints: sql`${s.users.loyaltyPoints} - ${points}` })
          .where(and(eq(s.users.id, input.user.id), gte(s.users.loyaltyPoints, points)))
          .returning({ id: s.users.id });
        if (!taken[0]) throw new OrderConflict('loyalty_balance');
        await tx.insert(s.loyaltyTransactions).values({ id: createId(), userId: input.user.id, kind: 'redeem', points: -points, orderId: id, note: number });
      }

      const p = quote.pricing;
      const rows = await tx
        .insert(s.orders)
        .values({
          id,
          number,
          tokenHash: await sha256(linkToken('order', id)),
          branchId: branch.id,
          userId: input.user?.id ?? null,
          channel: input.channel,
          tableId: input.tableId ?? null,
          status: pendingPayment ? 'pending_payment' : 'placed',
          asap: input.when.asap,
          scheduledFor: input.when.asap ? null : input.when.at,
          promisedAt: new Date(quote.timing.promisedAt as string),
          prepMinutes: quote.timing.prepMinutes,
          name: input.name,
          email,
          phone: input.phone,
          address: input.channel === 'delivery' ? input.address : null,
          zoneId: input.channel === 'delivery' ? (quote.zone?.id ?? null) : null,
          notes: input.notes,
          locale: input.locale,
          subtotal: p.subtotal,
          discount: p.discount,
          promoCode: quote.promo?.ok ? quote.promo.code : null,
          deliveryFee: p.deliveryFee,
          serviceCharge: p.serviceCharge,
          tax: p.tax,
          tip: p.tip,
          giftCardAmount: giftApplied,
          giftCardId: quote.giftCard?.ok ? quote.giftCard.id : null,
          loyaltyPointsRedeemed: points,
          loyaltyDiscount: p.loyaltyDiscount,
          total: p.total,
          paymentMethod: method,
          paymentStatus: method === 'gift_card' ? 'paid' : pendingPayment ? 'pending' : 'unpaid',
          createdAt: now,
          updatedAt: now,
        })
        .returning();
      await tx.insert(s.orderItems).values(
        quote.lines.map((l) => ({
          id: createId(),
          orderId: id,
          itemId: l.itemId,
          name: l.nameLocalized,
          unitPrice: l.unitPrice,
          quantity: l.qty,
          modifiers: l.options,
          notes: l.note || null,
          lineTotal: l.lineTotal,
        })),
      );
      await tx.insert(s.orderEvents).values({ id: createId(), orderId: id, status: pendingPayment ? 'pending_payment' : 'placed', at: now });
      return rows[0] as Order;
    });

    if (input.user && input.saveAddressAs && input.address && input.channel === 'delivery') {
      await db.insert(s.addresses).values({ id: createId(), userId: input.user.id, label: input.saveAddressAs.slice(0, 40), zoneId: input.address.zoneId, area: input.address.area, street: input.address.street, building: input.address.building ?? null, floor: input.address.floor ?? null, notes: input.address.notes ?? null, lat: input.address.lat ?? null, lng: input.address.lng ?? null });
    }
    if (input.user) await db.delete(s.carts).where(eq(s.carts.userId, input.user.id));

    if (pendingPayment) {
      const { startPayment } = await import('./payments');
      const payment = await startPayment({ purpose: 'order', referenceId: order.id, amount: due, description: `${branch.shortName.en} · ${order.number}`, email, locale: input.locale, returnPath: `/${input.locale}${trackingPath(order)}` });
      return { ok: true, order, payment };
    }
    await announceOrder(order);
    return { ok: true, order, payment: null };
  } catch (error) {
    if (error instanceof OrderConflict) return { ok: false, error: error.code };
    throw error;
  }
}

/** Card payment settled: the order goes to the kitchen. A payment that arrives after the order lapsed is refunded. */
export async function confirmOrderPaid(orderId: string, paymentId: string): Promise<void> {
  const now = new Date();
  const updated = await db
    .update(s.orders)
    .set({ status: 'placed', paymentStatus: 'paid', paymentId, updatedAt: now })
    .where(and(eq(s.orders.id, orderId), eq(s.orders.status, 'pending_payment')))
    .returning();
  if (updated[0]) {
    await db.insert(s.orderEvents).values({ id: createId(), orderId, status: 'placed', at: now });
    await announceOrder(updated[0]);
    return;
  }
  const order = await db.query.orders.findFirst({ where: eq(s.orders.id, orderId) });
  if (order && order.status === 'cancelled' && order.paymentStatus !== 'paid' && order.paymentStatus !== 'refunded') {
    const { refundPayment } = await import('./payments');
    await refundPayment(paymentId);
    await db.update(s.orders).set({ paymentStatus: 'refunded', paymentId, updatedAt: now }).where(eq(s.orders.id, orderId));
  }
}

/** Gives back what an order had reserved: promo use, gift card amount, loyalty points. */
async function releaseReservations(tx: Tx, order: Order, now: Date): Promise<void> {
  if (order.promoCode) {
    const redemption = await tx.query.promotionRedemptions.findFirst({ where: eq(s.promotionRedemptions.orderId, order.id) });
    if (redemption) {
      await tx.delete(s.promotionRedemptions).where(eq(s.promotionRedemptions.id, redemption.id));
      await tx.update(s.promotions).set({ usedCount: sql`max(0, ${s.promotions.usedCount} - 1)` }).where(eq(s.promotions.id, redemption.promotionId));
    }
  }
  if (order.giftCardId && order.giftCardAmount > 0) {
    await tx
      .update(s.giftCards)
      .set({ balance: sql`${s.giftCards.balance} + ${order.giftCardAmount}`, status: sql`case when ${s.giftCards.status} = 'redeemed' then 'active' else ${s.giftCards.status} end`, updatedAt: now })
      .where(eq(s.giftCards.id, order.giftCardId));
    await tx.insert(s.giftCardTransactions).values({ id: createId(), giftCardId: order.giftCardId, kind: 'refund', amount: order.giftCardAmount, orderId: order.id, note: order.number });
  }
  if (order.userId && order.loyaltyPointsRedeemed > 0) {
    await tx.update(s.users).set({ loyaltyPoints: sql`${s.users.loyaltyPoints} + ${order.loyaltyPointsRedeemed}` }).where(eq(s.users.id, order.userId));
    await tx.insert(s.loyaltyTransactions).values({ id: createId(), userId: order.userId, kind: 'reverse', points: order.loyaltyPointsRedeemed, orderId: order.id, note: order.number });
  }
}

/** Orders whose card payment never completed are cancelled and their reservations released. */
export async function expireUnpaidOrders(now = new Date()): Promise<number> {
  const stale = await db.query.orders.findMany({ where: and(eq(s.orders.status, 'pending_payment'), lt(s.orders.createdAt, new Date(now.getTime() - PAYMENT_WINDOW_MINUTES * MINUTE))) });
  let expired = 0;
  for (const order of stale) {
    const done = await db.transaction(async (tx) => {
      const claimed = await tx.update(s.orders).set({ status: 'cancelled', updatedAt: now }).where(and(eq(s.orders.id, order.id), eq(s.orders.status, 'pending_payment'))).returning();
      if (!claimed[0]) return false;
      await releaseReservations(tx, order, now);
      await tx.insert(s.orderEvents).values({ id: createId(), orderId: order.id, status: 'cancelled', note: 'payment_timeout', at: now });
      return true;
    });
    if (done) expired++;
  }
  return expired;
}

// ————————————————————————————————————————— status changes —————————————————————————————————————————

const NEXT: Record<s.OrderStatus, s.OrderStatus[]> = {
  pending_payment: ['cancelled'],
  placed: ['accepted', 'rejected', 'cancelled'],
  accepted: ['preparing', 'ready', 'cancelled'],
  preparing: ['ready', 'out_for_delivery', 'cancelled'],
  ready: ['out_for_delivery', 'completed'],
  out_for_delivery: ['completed'],
  completed: [],
  rejected: [],
  cancelled: [],
};

export function allowedNext(order: Pick<Order, 'status' | 'channel'>): s.OrderStatus[] {
  return NEXT[order.status].filter((st) => st !== 'out_for_delivery' || order.channel === 'delivery');
}

/**
 * Moves an order along (kitchen, staff, or the system), with the guest's email, refunds for rejected or
 * cancelled paid orders, and loyalty points once it is completed.
 */
export async function setOrderStatus(orderId: string, next: s.OrderStatus, actor: { id: string; email: string } | null, options: { note?: string | null; prepMinutes?: number | null } = {}): Promise<Order | null> {
  const now = new Date();
  const order = await db.query.orders.findFirst({ where: eq(s.orders.id, orderId) });
  if (!order || !allowedNext(order).includes(next)) return null;
  const extraPrep = next === 'accepted' && options.prepMinutes ? Math.max(5, Math.min(180, options.prepMinutes)) : null;
  const updated = await db.transaction(async (tx) => {
    const rows = await tx
      .update(s.orders)
      .set({
        status: next,
        updatedAt: now,
        ...(next === 'accepted' ? { acceptedAt: now } : {}),
        ...(next === 'ready' || next === 'out_for_delivery' ? { readyAt: order.readyAt ?? now } : {}),
        ...(next === 'completed' ? { completedAt: now } : {}),
        ...(next === 'rejected' ? { rejectReason: options.note ?? null } : {}),
        ...(extraPrep ? { prepMinutes: extraPrep, promisedAt: order.asap ? new Date(now.getTime() + extraPrep * MINUTE) : order.promisedAt } : {}),
      })
      .where(and(eq(s.orders.id, orderId), eq(s.orders.status, order.status)))
      .returning();
    const row = rows[0];
    if (!row) return null;
    await tx.insert(s.orderEvents).values({ id: createId(), orderId, status: next, note: options.note ?? null, actorId: actor?.id ?? null, at: now });
    if (next === 'rejected' || next === 'cancelled') await releaseReservations(tx, order, now);
    return row;
  });
  if (!updated) return null;
  if ((next === 'rejected' || next === 'cancelled') && order.paymentStatus === 'paid' && order.paymentId) {
    const { refundPayment } = await import('./payments');
    await refundPayment(order.paymentId);
    await db.update(s.orders).set({ paymentStatus: 'refunded' }).where(eq(s.orders.id, orderId));
  }
  if (next === 'completed') await awardLoyalty(updated);
  if (['accepted', 'ready', 'out_for_delivery', 'completed', 'rejected', 'cancelled'].includes(next)) await sendStatusEmail(updated, next);
  if (actor) await audit({ actor, action: `order.${next}`, entity: 'order', entityId: orderId, summary: order.number });
  return updated;
}

/** Loyalty points for a completed order, on what was spent on food (tips and gift card value excluded). */
async function awardLoyalty(order: Order): Promise<void> {
  if (!order.userId || order.loyaltyPointsEarned > 0) return;
  const user = await db.query.users.findFirst({ where: eq(s.users.id, order.userId), columns: { lifetimePoints: true } });
  if (!user) return;
  const base = Math.max(0, order.subtotal - order.discount - order.loyaltyDiscount);
  const points = pointsEarned(base, user.lifetimePoints, { ...restaurantConfig.loyalty, tiers: [...restaurantConfig.loyalty.tiers] });
  if (points <= 0) return;
  await db.transaction(async (tx) => {
    const claimed = await tx.update(s.orders).set({ loyaltyPointsEarned: points }).where(and(eq(s.orders.id, order.id), eq(s.orders.loyaltyPointsEarned, 0))).returning({ id: s.orders.id });
    if (!claimed[0]) return;
    await tx.update(s.users).set({ loyaltyPoints: sql`${s.users.loyaltyPoints} + ${points}`, lifetimePoints: sql`${s.users.lifetimePoints} + ${points}` }).where(eq(s.users.id, order.userId as string));
    await tx.insert(s.loyaltyTransactions).values({ id: createId(), userId: order.userId as string, kind: 'earn', points, orderId: order.id, note: order.number });
  });
}

// ————————————————————————————————————————— emails & notices —————————————————————————————————————————

export async function orderItems(orderId: string): Promise<OrderItem[]> {
  return db.select().from(s.orderItems).where(eq(s.orderItems.orderId, orderId)).orderBy(asc(s.orderItems.id));
}

/** The guest-facing "when": arrival for delivery, collection for pickup. */
export function promisedLabel(order: Order, branch: BranchDTO, locale: string): string {
  return order.promisedAt ? formatDateTime(order.promisedAt, locale, branch.timeZone) : '';
}

/** Rows of the price breakdown, in the order's language. */
export async function totalsRows(order: Order, locale: string): Promise<{ label: string; value: string; strong?: boolean }[]> {
  const t = await getTranslations({ locale, namespace: 'order.totals' });
  const money = (v: number) => formatMoney(v, locale);
  const rows: { label: string; value: string; strong?: boolean }[] = [{ label: t('subtotal'), value: money(order.subtotal) }];
  if (order.discount) rows.push({ label: order.promoCode ? t('discountCode', { code: order.promoCode }) : t('discount'), value: `−${money(order.discount)}` });
  if (order.loyaltyDiscount) rows.push({ label: t('loyalty'), value: `−${money(order.loyaltyDiscount)}` });
  if (order.deliveryFee) rows.push({ label: t('delivery'), value: money(order.deliveryFee) });
  if (order.serviceCharge) rows.push({ label: t('service'), value: money(order.serviceCharge) });
  if (order.tip) rows.push({ label: t('tip'), value: money(order.tip) });
  rows.push({ label: t('total'), value: money(order.total), strong: true });
  rows.push({ label: restaurantConfig.tax.pricesIncludeTax ? t('vatIncluded', { rate: formatNumber(restaurantConfig.tax.rate * 100, locale) }) : t('vat'), value: money(order.tax) });
  if (order.giftCardAmount) rows.push({ label: t('giftCard'), value: `−${money(order.giftCardAmount)}` });
  if (order.giftCardAmount) rows.push({ label: t('due'), value: money(order.total - order.giftCardAmount), strong: true });
  return rows;
}

async function announceOrder(order: Order): Promise<void> {
  const branch = await getBranch(order.branchId);
  if (!branch) return;
  const t = await getTranslations({ locale: order.locale, namespace: 'order' });
  const items = await orderItems(order.id);
  const address = order.address ? joinParts([order.address.area, order.address.street, order.address.building, order.address.floor], order.locale) : null;
  // Orders from a table QR may come without an email: the kitchen still hears about them.
  if (order.email) {
    await mail(
      order.email,
      {
        name: 'order-receipt',
        props: {
          locale: order.locale,
          name: order.name,
          number: order.number,
          branchName: tr(branch.name, order.locale),
          channelLabel: t(`channels.${order.channel}`),
          whenLabel: order.asap ? t('receipt.asap', { time: promisedLabel(order, branch, order.locale) }) : t('receipt.scheduled', { time: promisedLabel(order, branch, order.locale) }),
          addressLabel: address,
          lines: items.map((i) => ({
            name: tr(i.name, order.locale),
            quantity: formatNumber(i.quantity, order.locale),
            details: [...i.modifiers.map((m) => tr(m.name, order.locale)), i.notes ? `“${i.notes}”` : null].filter(Boolean).join(' · '),
            total: formatMoney(i.lineTotal, order.locale),
          })),
          totals: await totalsRows(order, order.locale),
          paymentLabel: t(`payment.${order.paymentMethod}`),
          trackUrl: trackingUrl(order),
        },
      },
      { meta: { order: order.number } },
    );
  }
  const count = items.reduce((n, i) => n + i.quantity, 0);
  const [tar, ten] = await Promise.all([getTranslations({ locale: 'ar', namespace: 'order' }), getTranslations({ locale: 'en', namespace: 'order' })]);
  const table = order.tableId ? await db.query.diningTables.findFirst({ where: eq(s.diningTables.id, order.tableId), columns: { label: true } }) : null;
  await notifyStaff({
    role: 'kitchen',
    branchId: order.branchId,
    kind: 'order',
    title: table ? { ar: `طلبٌ جديد ${order.number} · طاولة ${table.label}`, en: `New order ${order.number} · table ${table.label}` } : { ar: `طلبٌ جديد ${order.number}`, en: `New order ${order.number}` },
    body: {
      ar: `${tar(`channels.${order.channel}`)} · ${tar('itemsCount', { count, n: formatNumber(count, 'ar') })} · ${formatMoney(order.total, 'ar')}`,
      en: `${ten(`channels.${order.channel}`)} · ${ten('itemsCount', { count, n: formatNumber(count, 'en') })} · ${formatMoney(order.total, 'en')}`,
    },
    href: '/admin/orders',
  });
}

async function sendStatusEmail(order: Order, status: s.OrderStatus): Promise<void> {
  if (!order.email) return;
  const t = await getTranslations({ locale: order.locale, namespace: 'order.statusEmail' });
  const key = status === 'ready' && order.channel === 'dine_in' ? 'served' : status;
  if (!t.has(`${key}.headline`)) return;
  const branch = await getBranch(order.branchId);
  await mail(order.email, {
    name: 'order-status',
    props: {
      locale: order.locale,
      name: order.name,
      number: order.number,
      headline: t(`${key}.headline`),
      message: t(`${key}.message`, { house: branch ? tr(branch.name, order.locale) : '', reason: order.rejectReason ?? '' }),
      trackUrl: trackingUrl(order),
    },
  });
}

// ————————————————————————————————————————— tracking & reorder —————————————————————————————————————————

export interface TrackingSnapshot {
  status: s.OrderStatus;
  promisedAt: string | null;
  updatedAt: string;
  events: { status: s.OrderStatus; at: string }[];
}

export async function trackingSnapshot(order: Order): Promise<TrackingSnapshot> {
  const events = await db.select({ status: s.orderEvents.status, at: s.orderEvents.at }).from(s.orderEvents).where(eq(s.orderEvents.orderId, order.id)).orderBy(asc(s.orderEvents.at));
  return { status: order.status, promisedAt: order.promisedAt?.toISOString() ?? null, updatedAt: order.updatedAt.toISOString(), events: events.map((e) => ({ status: e.status, at: e.at.toISOString() })) };
}

/** Lines to put back in the basket for "order again" (dishes no longer orderable are left out). */
export async function reorderLines(order: Order): Promise<{ lines: CartLineInput[]; skipped: number }> {
  const [items, catalog] = await Promise.all([orderItems(order.id), getMenuCatalog()]);
  const byId = new Map(Object.values(catalog.items).map((i) => [i.id, i]));
  const lines: CartLineInput[] = [];
  let skipped = 0;
  for (const i of items) {
    const item = i.itemId ? byId.get(i.itemId) : undefined;
    if (!item || itemOrderable(item, order.branchId, false)) {
      skipped++;
      continue;
    }
    const optionIds = i.modifiers.map((m) => m.optionId).filter((oid) => item.modifierGroups.some((g) => g.options.some((o) => o.id === oid && o.isAvailable)));
    lines.push({ slug: item.slug, qty: i.quantity, optionIds, note: i.notes ?? '' });
  }
  return { lines, skipped };
}

/** A signed-in guest's recent orders (account history). */
export async function ordersForUser(userId: string, limit = 30): Promise<Order[]> {
  return db.query.orders.findMany({ where: eq(s.orders.userId, userId), orderBy: [desc(s.orders.createdAt)], limit });
}

export async function activeOrderCount(branchId: string): Promise<number> {
  const rows = await db.select({ id: s.orders.id }).from(s.orders).where(and(eq(s.orders.branchId, branchId), inArray(s.orders.status, ['placed', 'accepted', 'preparing'])));
  return rows.length;
}
