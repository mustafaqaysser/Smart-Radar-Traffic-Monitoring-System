/**
 * 90 days of realistic synthetic history, relative to the day the seed runs:
 * orders (priced by the real pricing engine), order events, reservations (today and upcoming ones placed
 * through the real availability engine so they never conflict), waitlist, loyalty, gift cards, event
 * tickets, newsletter, inquiries, applications, table requests, analytics, audit log and notifications.
 */
import { mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import * as s from '../../src/lib/db/schema';
import type { LocalizedText } from '../../src/lib/i18n/localized';
import { computeAvailability, type BookingInfo } from '../../src/lib/domain/availability';
import { priceCart } from '../../src/lib/domain/pricing';
import { pointsEarned } from '../../src/lib/domain/loyalty';
import { addDays, localToUtc, parseTime, toDateString, weekdayOf, formatTime } from '../../src/lib/time/zoned';
import { CODE_PREFIX, createCode, createGiftCardCode, createId, sha256 } from '../../src/lib/utils/id';
import restaurantConfig from '../../restaurant.config';
import { branches as branchSeeds } from './content/branches';
import { items as itemSeeds, menus as menuSeeds, modifierGroups } from './content/menu';
import { events as eventSeeds } from './content/events';
import { promotions as promoSeeds } from './content/promotions';
import { OCCASIONS } from './names';
import { ids } from './world';
import type { SeededCustomer } from './people';
import type { SeedContext } from './types';

const TZ = 'Asia/Riyadh';

/** Inserts rows in chunks (SQLite limits bound parameters per statement). */
async function insertChunked<T>(rows: T[], insert: (chunk: T[]) => Promise<unknown>, size = 150) {
  for (let i = 0; i < rows.length; i += size) {
    if (rows.length) await insert(rows.slice(i, i + size));
  }
}

interface ItemInfo {
  slug: string;
  id: string;
  price: number;
  name: LocalizedText;
  branches: string[] | null;
  groups: string[];
  prep: number;
}

const WINDOWS = [
  { key: 'breakfast', start: '08:00', end: '11:15', weight: 15, menus: ['morning'] },
  { key: 'lunch', start: '12:30', end: '15:45', weight: 34, menus: ['noon', 'sweets'] },
  { key: 'afternoon', start: '16:00', end: '18:20', weight: 11, menus: ['afternoon'] },
  { key: 'dinner', start: '19:00', end: '23:30', weight: 40, menus: ['night', 'sweets'] },
] as const;

export async function seedHistory(ctx: SeedContext, people: { customers: SeededCustomer[]; demoCustomer: SeededCustomer }) {
  const { db, random, now, log } = ctx;
  const today = toDateString(now, TZ);
  const accountIds = new Set((await db.select({ id: s.users.id }).from(s.users)).map((r) => r.id));
  const customers = people.customers;
  const demo = people.demoCustomer;

  // ——— lookups ———
  const itemBySlug = new Map<string, ItemInfo>();
  for (const it of itemSeeds) {
    itemBySlug.set(it.slug, { slug: it.slug, id: ids.item(it.slug), price: Math.round(it.price * 100), name: it.name, branches: it.branches ?? null, groups: it.modifiers ?? [], prep: it.prep ?? 15 });
  }
  const orderableFor = (menuSlugs: readonly string[], branch: string): ItemInfo[] => {
    const slugs = new Set<string>();
    for (const m of menuSeeds.filter((mm) => menuSlugs.includes(mm.slug))) for (const c of m.categories) for (const sl of c.items) slugs.add(sl);
    return [...slugs]
      .map((sl) => itemBySlug.get(sl) as ItemInfo)
      .filter((it) => it && it.price > 0 && (!it.branches || it.branches.includes(branch)));
  };
  const drinks = orderableFor(['cups'], 'al-balad');
  const groupDefs = new Map(
    modifierGroups.map((g) => [
      g.key,
      { id: ids.group(g.key), minSelect: g.min, maxSelect: g.max, options: g.options.map((o) => ({ id: ids.option(g.key, o.key), priceDelta: Math.round(o.delta * 100), isAvailable: true, default: o.default ?? false, name: o.name })) },
    ]),
  );
  const zones = new Map(branchSeeds.map((b) => [b.slug, b.zones.map((z, i) => ({ id: `dz_${b.slug}_${i}`, fee: Math.round(z.fee * 100), minOrder: Math.round(z.minOrder * 100), eta: z.eta, name: z.name, areas: z.areas }))]));
  const activePromo = promoSeeds.find((p) => p.active && p.kind === 'percent' && !p.firstOrderOnly) ?? promoSeeds[0];

  const orders: (typeof s.orders.$inferInsert)[] = [];
  const orderItems: (typeof s.orderItems.$inferInsert)[] = [];
  const orderEvents: (typeof s.orderEvents.$inferInsert)[] = [];
  const loyaltyTx: (typeof s.loyaltyTransactions.$inferInsert)[] = [];
  const pointsByUser = new Map<string, { balance: number; lifetime: number }>();
  const promoUses: (typeof s.promotionRedemptions.$inferInsert)[] = [];
  const counters = new Map<string, number>(branchSeeds.map((b) => [b.slug, 1000]));
  const tokenHash = await sha256('seeded-order-without-a-public-token');

  // The demo guest keeps the history of a real regular: about a dozen orders and a handful of visits in 90 days.
  const pickCustomer = (branch: string, demoChance = 0.003): SeededCustomer => {
    if (random.chance(demoChance)) return demo;
    const local = customers.filter((c) => c.branch === branch);
    return random.pick(local.length ? local : customers);
  };

  // ——— orders ———
  for (let d = -90; d <= 0; d++) {
    const date = addDays(today, d);
    const weekday = weekdayOf(date);
    const weekend = weekday === 4 || weekday === 5;
    for (const branch of branchSeeds) {
      const base = weekend ? random.int(30, 44) : random.int(18, 30);
      const count = Math.round(base * (branch.slug === 'al-balad' ? 1 : 0.85) * (d > -30 ? 1.08 : 1));
      for (let n = 0; n < count; n++) {
        const window = random.weighted(WINDOWS.map((w) => [w, w.weight] as const));
        const minutes = random.int(parseTime(window.start), parseTime(window.end));
        const createdAt = localToUtc(date, minutes, TZ, true) as Date;
        if (createdAt > now) continue;
        const ageMin = (now.getTime() - createdAt.getTime()) / 60000;
        const channel = random.weighted<s.OrderChannel>([['delivery', 55], ['pickup', 25], ['dine_in', 20]]);
        const customer = pickCustomer(branch.slug);
        const menuItems = orderableFor(window.menus, branch.slug);
        const lineCount = random.weighted([[1, 25], [2, 38], [3, 25], [4, 12]] as const);
        const chosen = random.shuffle(menuItems).slice(0, lineCount);
        if (random.chance(0.45)) chosen.push(random.pick(drinks));
        const lines = chosen.map((it, i) => {
          const groups = it.groups.map((g) => groupDefs.get(g)).filter((g): g is NonNullable<typeof g> => !!g);
          const optionIds: string[] = [];
          for (const g of groups) {
            if (g.minSelect > 0) optionIds.push((g.options.find((o) => o.default) ?? g.options[0])!.id);
            else if (random.chance(0.25)) optionIds.push(random.pick(g.options).id);
          }
          return { key: String(i), unitPrice: it.price, quantity: random.chance(0.2) ? 2 : 1, optionIds, groups, item: it };
        });
        const zone = channel === 'delivery' ? random.pick(zones.get(branch.slug) ?? []) : null;
        const usePromo = activePromo && random.chance(0.07);
        const subtotalGuess = lines.reduce((sum, l) => sum + l.unitPrice * l.quantity, 0);
        const promoDiscount = usePromo && activePromo.kind === 'percent' ? Math.min(Math.floor((subtotalGuess * activePromo.value) / 100), activePromo.maxDiscount ?? Infinity) : 0;
        const paymentMethod: 'card' | 'cash' | 'pay_at_venue' = channel === 'dine_in' ? (random.chance(0.6) ? 'pay_at_venue' : 'card') : random.chance(0.76) ? 'card' : 'cash';
        const tipPct = paymentMethod === 'card' && random.chance(0.3) ? random.pick([0.05, 0.1]) : 0;
        const priced = priceCart({
          lines,
          config: {
            taxRate: restaurantConfig.tax.rate,
            pricesIncludeTax: restaurantConfig.tax.pricesIncludeTax,
            serviceChargeRate: restaurantConfig.serviceCharge.rate,
            serviceChargeApplies: (restaurantConfig.serviceCharge.channels as string[]).includes(channel),
          },
          zone: zone ? { fee: zone.fee, minOrder: 0 } : null,
          promoDiscount,
          tip: tipPct ? { kind: 'percent', value: tipPct } : null,
        });
        const prep = Math.max(branch.ordering.basePrepMinutes, ...lines.map((l) => l.item.prep));
        // Status by age: older orders are settled; the last hour and a half is still moving.
        let status: s.OrderStatus = 'completed';
        if (ageMin > 150) status = random.weighted([['completed', 93], ['cancelled', 4], ['rejected', 3]]);
        else if (ageMin < 4) status = 'placed';
        else if (ageMin < 9) status = 'accepted';
        else if (ageMin < prep + 6) status = 'preparing';
        else if (ageMin < prep + 14) status = channel === 'delivery' ? 'out_for_delivery' : 'ready';
        else if (ageMin < prep + 40 && channel === 'delivery') status = 'out_for_delivery';
        const id = createId();
        const seq = (counters.get(branch.slug) ?? 1000) + 1;
        counters.set(branch.slug, seq);
        const number = `${branch.codePrefix}-${seq}`;
        const accountUser = accountIds.has(customer.id) ? customer.id : null;
        const promisedAt = new Date(createdAt.getTime() + (prep + (zone?.eta ?? 0)) * 60000);
        const paid = status === 'completed' || (paymentMethod === 'card' && status !== 'rejected' && status !== 'cancelled');
        orders.push({
          id,
          number,
          tokenHash,
          branchId: ids.branch(branch.slug),
          userId: accountUser,
          channel,
          tableId: channel === 'dine_in' ? ids.table(branch.slug, random.pick(branch.tables.filter((t) => t.area !== 'private')).label) : null,
          status,
          asap: true,
          promisedAt,
          prepMinutes: prep,
          name: customer.name,
          email: customer.email,
          phone: customer.phone,
          address: zone ? { zoneId: zone.id, area: zone.areas?.[0]?.[customer.locale] ?? zone.name[customer.locale], street: customer.locale === 'ar' ? 'شارع الخيالة، مبنى ٨' : 'Khayyala Street, Building 8', building: String(random.int(2, 40)), floor: String(random.int(0, 6)) } : null,
          zoneId: zone?.id ?? null,
          locale: customer.locale,
          subtotal: priced.subtotal,
          discount: priced.discount,
          promoCode: usePromo ? activePromo.code : null,
          deliveryFee: priced.deliveryFee,
          serviceCharge: priced.serviceCharge,
          tax: priced.tax,
          tip: priced.tip,
          total: priced.total,
          paymentMethod,
          paymentStatus: status === 'rejected' || status === 'cancelled' ? (paymentMethod === 'card' ? 'refunded' : 'unpaid') : paid ? 'paid' : 'unpaid',
          rejectReason: status === 'rejected' ? random.pick(['Kitchen at capacity', 'Out of the ordered dish', 'Delivery address outside our zones']) : null,
          acceptedAt: status === 'placed' || status === 'rejected' ? null : new Date(createdAt.getTime() + random.int(2, 6) * 60000),
          readyAt: ['ready', 'out_for_delivery', 'completed'].includes(status) ? new Date(createdAt.getTime() + prep * 60000) : null,
          completedAt: status === 'completed' ? new Date(promisedAt.getTime() + random.int(-6, 9) * 60000) : null,
          createdAt,
          updatedAt: createdAt,
        });
        for (const l of lines) {
          const selected = l.optionIds.map((oid) => {
            for (const g of l.groups) {
              const opt = g.options.find((o) => o.id === oid);
              if (opt) return { groupId: g.id, optionId: opt.id, name: opt.name, priceDelta: opt.priceDelta };
            }
            throw new Error('option not found');
          });
          orderItems.push({ id: createId(), orderId: id, itemId: l.item.id, name: l.item.name, unitPrice: l.unitPrice + selected.reduce((sum, m) => sum + m.priceDelta, 0), quantity: l.quantity, modifiers: selected, lineTotal: priced.lineTotals[l.key] ?? 0 });
        }
        const timeline: [s.OrderStatus, number][] = [['placed', 0]];
        if (status === 'rejected') timeline.push(['rejected', random.int(2, 5)]);
        else if (status === 'cancelled') timeline.push(['accepted', 3], ['cancelled', random.int(5, 12)]);
        else {
          const order: s.OrderStatus[] = channel === 'delivery' ? ['accepted', 'preparing', 'out_for_delivery', 'completed'] : ['accepted', 'preparing', 'ready', 'completed'];
          const offsets = channel === 'delivery' ? [4, 6, prep + 4, prep + (zone?.eta ?? 30)] : [4, 6, prep, prep + random.int(5, 25)];
          const reached = order.indexOf(status);
          for (let k = 0; k <= reached && k < order.length; k++) timeline.push([order[k] as s.OrderStatus, offsets[k] as number]);
        }
        for (const [st, off] of timeline) orderEvents.push({ id: createId(), orderId: id, status: st, at: new Date(createdAt.getTime() + off * 60000) });
        if (usePromo) promoUses.push({ id: createId(), promotionId: `pm_${promoSeeds.indexOf(activePromo)}`, orderId: id, email: customer.email, amount: priced.discount, at: createdAt });
        if (accountUser && status === 'completed') {
          const acc = pointsByUser.get(accountUser) ?? { balance: 0, lifetime: 0 };
          const earned = pointsEarned(priced.total - priced.tip, acc.lifetime, restaurantConfig.loyalty);
          acc.balance += earned;
          acc.lifetime += earned;
          pointsByUser.set(accountUser, acc);
          loyaltyTx.push({ id: createId(), userId: accountUser, kind: 'earn', points: earned, orderId: id, at: createdAt });
          orders[orders.length - 1]!.loyaltyPointsEarned = earned;
        }
      }
    }
  }
  await insertChunked(orders, (chunk) => ctx.db.insert(s.orders).values(chunk));
  await insertChunked(orderItems, (chunk) => ctx.db.insert(s.orderItems).values(chunk), 250);
  await insertChunked(orderEvents, (chunk) => ctx.db.insert(s.orderEvents).values(chunk), 300);
  await insertChunked(promoUses, (chunk) => ctx.db.insert(s.promotionRedemptions).values(chunk));
  if (promoUses.length && activePromo) {
    const { eq } = await import('drizzle-orm');
    await db.update(s.promotions).set({ usedCount: promoUses.length }).where(eq(s.promotions.id, `pm_${promoSeeds.indexOf(activePromo)}`));
  }
  log(`orders: ${orders.length} (${orderItems.length} lines)`);

  // Loyalty: signup bonus, a redemption for regulars, and balances.
  for (const userId of accountIds) {
    if (!userId.startsWith('us_c') && userId !== demo.id) continue;
    const acc = pointsByUser.get(userId) ?? { balance: 0, lifetime: 0 };
    acc.balance += restaurantConfig.loyalty.signupBonus;
    acc.lifetime += restaurantConfig.loyalty.signupBonus;
    loyaltyTx.push({ id: createId(), userId, kind: 'bonus', points: restaurantConfig.loyalty.signupBonus, note: 'Welcome to the Shade Circle', at: new Date(now.getTime() - 120 * 864e5) });
    if (acc.balance > 900 && random.chance(0.5)) {
      acc.balance -= 400;
      loyaltyTx.push({ id: createId(), userId, kind: 'redeem', points: -400, note: 'Redeemed at checkout', at: new Date(now.getTime() - random.int(3, 40) * 864e5) });
    }
    pointsByUser.set(userId, acc);
  }
  await insertChunked(loyaltyTx, (chunk) => ctx.db.insert(s.loyaltyTransactions).values(chunk), 300);
  const { eq } = await import('drizzle-orm');
  for (const [userId, acc] of pointsByUser) await db.update(s.users).set({ loyaltyPoints: acc.balance, lifetimePoints: acc.lifetime }).where(eq(s.users.id, userId));

  // ——— reservations ———
  const reservationRows: (typeof s.reservations.$inferInsert)[] = [];
  const periodWeights: Record<string, number> = { breakfast: 15, 'breakfast-friday': 15, lunch: 25, 'long-shade': 10, dinner: 50, 'dinner-late': 50 };
  const partyWeights = [[2, 45], [3, 12], [4, 22], [5, 7], [6, 8], [7, 3], [8, 3]] as const;
  const manageToken = await sha256('seeded-reservation-without-a-public-token');

  for (const branch of branchSeeds) {
    const branchId = ids.branch(branch.slug);
    const tables = branch.tables.map((t, i) => ({ id: ids.table(branch.slug, t.label), area: t.area, minSeats: t.min, maxSeats: t.max, combineGroup: t.group, reservable: t.reservable ?? true, isActive: true, sortOrder: i }));
    const periods = branch.periods.map((p) => ({ key: p.key, weekdays: p.weekdays, start: p.start, end: p.end, maxCoversPerSlot: p.maxCovers ?? null }));
    for (let d = -90; d <= 45; d++) {
      const date = addDays(today, d);
      const weekday = weekdayOf(date);
      const weekend = weekday === 4 || weekday === 5;
      const special = branch.specials.find((sp) => addDays(today, sp.inDays) === date);
      if (special?.closed) continue;
      let target = weekend ? random.int(22, 34) : random.int(12, 22);
      if (d > 0) target = Math.round(target * Math.max(0.12, 1 - d / 40)); // bookings thin out further ahead
      const bookings: BookingInfo[] = [];
      const ctxAvail = {
        timeZone: TZ,
        rules: branch.reservation,
        periods,
        tables,
        bookings,
        override: special ? { closed: false, ranges: special.ranges ? special.ranges.map((r) => ({ start: parseTime(r.opens), end: parseTime(r.closes) < parseTime(r.opens) ? parseTime(r.closes) + 1440 : parseTime(r.closes) })) : null, reservationsBlocked: special.reservationsBlocked ?? false } : null,
      };
      for (let n = 0; n < target; n++) {
        const partySize = random.weighted(partyWeights);
        const slots = computeAvailability(ctxAvail, { date, partySize, now: new Date(0) }).filter((sl) => sl.available);
        if (!slots.length) continue;
        const weighted = slots.map((sl) => [sl, periodWeights[sl.periodKey] ?? 10] as const);
        const slot = random.weighted(weighted);
        const customer = pickCustomer(branch.slug, d > 0 ? 0 : 0.0016);
        const past = slot.endsAt < now;
        const inProgress = slot.startsAt <= now && now < slot.endsAt;
        let status: s.ReservationStatus = 'confirmed';
        if (past) status = random.weighted([['completed', 84], ['no_show', 6], ['cancelled', 10]]);
        else if (inProgress) status = 'seated';
        else if (random.chance(0.04)) status = 'cancelled';
        const id = createId();
        if (status !== 'cancelled') bookings.push({ id, startsAt: slot.startsAt, endsAt: slot.endsAt, partySize, tableIds: slot.tableIds });
        const deposit = partySize >= restaurantConfig.reservations.deposit.minParty ? partySize * restaurantConfig.reservations.deposit.perGuest * 100 : 0;
        reservationRows.push({
          id,
          code: createCode(CODE_PREFIX.reservation),
          tokenHash: manageToken,
          branchId,
          userId: accountIds.has(customer.id) ? customer.id : null,
          date,
          time: formatTime(slot.minutes),
          startsAt: slot.startsAt,
          endsAt: slot.endsAt,
          partySize,
          area: random.chance(0.4) ? (tables.find((t) => t.id === slot.tableIds[0])?.area ?? 'any') : 'any',
          occasion: random.chance(0.18) ? random.pick(OCCASIONS) : null,
          tableIds: slot.tableIds,
          status,
          name: customer.name,
          email: customer.email,
          phone: customer.phone,
          notes: random.chance(0.12) ? random.pick(['Window side if possible', 'نحتفل بعيد ميلاد', 'A high chair, please', 'حساسية من المكسّرات']) : null,
          locale: customer.locale,
          source: random.weighted([['web', 70], ['phone', 18], ['walk_in', 7], ['admin', 5]]),
          depositAmount: deposit,
          depositStatus: deposit ? (status === 'cancelled' ? 'refunded' : 'paid') : 'none',
          reminderSentAt: past || d <= 1 ? new Date(slot.startsAt.getTime() - 24 * 3600000) : null,
          seatedAt: status === 'seated' || status === 'completed' ? slot.startsAt : null,
          completedAt: status === 'completed' ? slot.endsAt : null,
          cancelledAt: status === 'cancelled' ? new Date(slot.startsAt.getTime() - random.int(3, 72) * 3600000) : null,
          createdAt: new Date(slot.startsAt.getTime() - random.int(1, 21) * 864e5),
        });
      }
    }
  }
  // The demo guest has an upcoming booking and a past one.
  const demoUpcoming = reservationRows.find((r) => r.status === 'confirmed' && r.branchId === ids.branch('al-balad') && r.partySize === 2 && (r.startsAt as Date) > new Date(now.getTime() + 2 * 864e5));
  if (demoUpcoming) Object.assign(demoUpcoming, { userId: demo.id, name: demo.name, email: demo.email, phone: demo.phone, locale: 'ar', occasion: 'anniversary' });
  const demoPast = reservationRows.find((r) => r.status === 'completed' && r.branchId === ids.branch('al-balad'));
  if (demoPast) Object.assign(demoPast, { userId: demo.id, name: demo.name, email: demo.email, phone: demo.phone, locale: 'ar' });
  await insertChunked(reservationRows, (chunk) => ctx.db.insert(s.reservations).values(chunk));
  log(`reservations: ${reservationRows.length}`);

  // Waitlist for the next busy evening.
  const nextThursday = Array.from({ length: 7 }, (_, i) => addDays(today, i + 1)).find((dd) => weekdayOf(dd) === 4) ?? addDays(today, 3);
  await db.insert(s.waitlistEntries).values(
    Array.from({ length: 4 }, (_, i) => {
      const c = random.pick(customers);
      return { id: createId(), branchId: ids.branch('al-balad'), date: nextThursday, preferredTime: ['20:00', '20:30', '21:00', '21:15'][i] as string, partySize: [2, 4, 3, 6][i] as number, name: c.name, email: c.email, phone: c.phone, locale: c.locale, status: i === 3 ? ('offered' as const) : ('waiting' as const) };
    }),
  );

  // ——— gift cards ———
  const giftRows: (typeof s.giftCards.$inferInsert)[] = [];
  const giftTx: (typeof s.giftCardTransactions.$inferInsert)[] = [];
  for (let i = 0; i < 42; i++) {
    const buyer = random.pick(customers);
    const recipient = random.pick(customers);
    const amount = random.pick(restaurantConfig.giftCards.presets) * 100;
    const created = new Date(now.getTime() - random.int(1, 85) * 864e5);
    const id = createId();
    let balance = amount;
    let status: 'active' | 'redeemed' | 'void' | 'scheduled' = 'active';
    giftTx.push({ id: createId(), giftCardId: id, kind: 'issue', amount, note: 'Purchased online', at: created });
    if (random.chance(0.45)) {
      const spent = Math.min(balance, random.int(8, 40) * 1000);
      balance -= spent;
      giftTx.push({ id: createId(), giftCardId: id, kind: 'redeem', amount: -spent, note: 'Redeemed on an order', at: new Date(created.getTime() + random.int(2, 20) * 864e5) });
      if (balance === 0) status = 'redeemed';
    }
    if (i === 7) {
      status = 'void';
      giftTx.push({ id: createId(), giftCardId: id, kind: 'void', amount: -balance, note: 'Duplicate purchase, refunded', at: new Date(created.getTime() + 864e5) });
      balance = 0;
    }
    const scheduled = i < 2;
    if (scheduled) status = 'scheduled';
    giftRows.push({
      id,
      code: i === 0 ? 'ZILL-DEMO-GIFT-2026' : createGiftCardCode(),
      initialAmount: amount,
      balance: i === 0 ? 50000 : balance,
      currency: restaurantConfig.currency,
      design: random.pick(restaurantConfig.giftCards.designs),
      purchaserName: buyer.name,
      purchaserEmail: buyer.email,
      recipientName: recipient.name,
      recipientEmail: recipient.email,
      message: random.chance(0.6) ? random.pick(['لأجمل صباحاتنا في البلد.', 'Happy birthday — dinner is on me.', 'شكراً على كل شيء.', 'For the long shade, and the long talks.']) : null,
      locale: recipient.locale,
      deliverAt: scheduled ? new Date(now.getTime() + (i + 3) * 864e5) : created,
      deliveredAt: scheduled ? null : created,
      status: i === 0 ? 'active' : status,
      expiresAt: new Date(created.getTime() + restaurantConfig.giftCards.validityMonths * 30 * 864e5),
      createdAt: created,
    });
  }
  // The demo gift card (used by e2e tests and reviewers) has a fresh, known balance and was given to the demo guest.
  if (giftRows[0]) Object.assign(giftRows[0], { initialAmount: 50000, recipientName: demo.name, recipientEmail: demo.email, locale: demo.locale, deliverAt: giftRows[0].createdAt, deliveredAt: giftRows[0].createdAt });
  await insertChunked(giftRows, (chunk) => ctx.db.insert(s.giftCards).values(chunk));
  await insertChunked(giftTx, (chunk) => ctx.db.insert(s.giftCardTransactions).values(chunk));

  // ——— event tickets ———
  const bookings: (typeof s.eventBookings.$inferInsert)[] = [];
  for (const e of eventSeeds) {
    const sold = Math.floor(e.capacity * (0.3 + random.next() * 0.5));
    let remaining = sold;
    while (remaining > 0) {
      // The demo guest holds two seats at the first gathering in the programme.
      const demoSeat = e === eventSeeds[0] && remaining === sold;
      const qty = Math.min(remaining, demoSeat ? 2 : random.weighted([[1, 30], [2, 55], [4, 15]] as const));
      remaining -= qty;
      const c = demoSeat ? demo : random.pick(customers);
      const ticketIndex = e.tickets.length > 1 && random.chance(0.35) ? 1 : 0;
      const ticket = e.tickets[ticketIndex];
      if (!ticket) break;
      bookings.push({ id: createId(), code: createCode(CODE_PREFIX.ticket), eventId: ids.event(e.slug), ticketTypeId: `et_${e.slug}_${ticketIndex}`, quantity: qty, total: ticket.price * qty, name: c.name, email: c.email, phone: c.phone, locale: c.locale, userId: demoSeat ? demo.id : null, status: 'confirmed', createdAt: new Date(now.getTime() - random.int(1, 20) * 864e5) });
    }
  }
  await insertChunked(bookings, (chunk) => ctx.db.insert(s.eventBookings).values(chunk));

  // ——— newsletter ———
  const subscriberRows = customers.slice(0, 150).map((c, i) => ({
    id: createId(),
    email: c.email,
    locale: c.locale,
    status: (i % 13 === 0 ? 'unsubscribed' : i % 7 === 0 ? 'pending' : 'confirmed') as 'pending' | 'confirmed' | 'unsubscribed',
    tokenHash: `seed-${i}`,
    source: random.pick(['footer', 'links', 'checkout', 'journal']),
    confirmedAt: i % 7 === 0 ? null : new Date(now.getTime() - random.int(1, 200) * 864e5),
    createdAt: new Date(now.getTime() - random.int(1, 210) * 864e5),
  }));
  await insertChunked(subscriberRows, (chunk) => ctx.db.insert(s.newsletterSubscribers).values(chunk));

  // ——— inquiries ———
  const inquiryTemplates: { kind: 'private_dining' | 'catering' | 'contact' | 'press' | 'large_party'; locale: 'ar' | 'en'; message: string; guests?: number }[] = [
    { kind: 'private_dining', locale: 'ar', message: 'نرغب في حجز الغرفة الخاصة لعشاء عائلي لاثني عشر شخصاً، مع قائمة المزولة إن أمكن.', guests: 12 },
    { kind: 'catering', locale: 'en', message: 'Breakfast for forty at our office in Al-Olaya — foul, mutabbaq and coffee. Can you deliver by 8 am?', guests: 40 },
    { kind: 'large_party', locale: 'ar', message: 'عيد ميلاد والدتي، نحن أحد عشر شخصاً يوم الجمعة مساءً.', guests: 11 },
    { kind: 'press', locale: 'en', message: 'We are preparing a feature on courtyard architecture and would love to photograph the Al-Balad house at noon.' },
    { kind: 'contact', locale: 'ar', message: 'هل يمكن تقديم الكنافة بلا فستق لطفلي المصاب بحساسية المكسّرات؟' },
    { kind: 'private_dining', locale: 'en', message: 'A company dinner for eighteen at Wadi Hanifah, ideally in the majlis, early November.', guests: 18 },
  ];
  await db.insert(s.inquiries).values(
    Array.from({ length: 14 }, (_, i) => {
      const t = inquiryTemplates[i % inquiryTemplates.length] as (typeof inquiryTemplates)[number];
      const c = random.pick(customers);
      return {
        id: createId(),
        kind: t.kind,
        branchId: ids.branch(i % 2 ? 'wadi-hanifah' : 'al-balad'),
        name: c.name,
        email: c.email,
        phone: c.phone,
        date: t.guests ? addDays(today, random.int(5, 40)) : null,
        guests: t.guests ?? null,
        message: t.message,
        locale: t.locale,
        status: (['new', 'new', 'in_progress', 'won', 'lost', 'closed'] as const)[i % 6] as 'new',
        createdAt: new Date(now.getTime() - random.int(0, 30) * 864e5),
      };
    }),
  );

  // ——— job applications (with small PDF CVs in local storage) ———
  const postings = await db.select({ id: s.jobPostings.id, title: s.jobPostings.title }).from(s.jobPostings);
  const storageDir = process.env.LOCAL_STORAGE_DIR ?? './storage';
  mkdirSync(join(storageDir, 'private', 'cv'), { recursive: true });
  const applications: (typeof s.jobApplications.$inferInsert)[] = [];
  for (let i = 0; i < 9 && postings.length; i++) {
    const posting = postings[i % postings.length] as (typeof postings)[number];
    const c = random.pick(customers);
    const key = `private/cv/seed-${i}.pdf`;
    writeFileSync(join(storageDir, key), minimalPdf(`Curriculum vitae — ${c.email} (demo)`));
    writeFileSync(join(storageDir, `${key}.type`), 'application/pdf');
    applications.push({ id: createId(), postingId: posting.id, name: c.name, email: c.email, phone: c.phone, message: c.locale === 'ar' ? 'أحبّ العمل في المطابخ المفتوحة وأستيقظ مبكراً.' : 'I love open kitchens and early mornings.', cvKey: key, cvName: `cv-${i + 1}.pdf`, status: (['new', 'reviewing', 'interview', 'new', 'declined'] as const)[i % 5] as 'new', locale: c.locale, createdAt: new Date(now.getTime() - random.int(0, 25) * 864e5) });
  }
  if (applications.length) await db.insert(s.jobApplications).values(applications);

  // ——— live table requests ———
  await db.insert(s.tableRequests).values([
    { id: createId(), branchId: ids.branch('al-balad'), tableId: ids.table('al-balad', 'C3'), kind: 'waiter', status: 'open', createdAt: new Date(now.getTime() - 2 * 60000) },
    { id: createId(), branchId: ids.branch('al-balad'), tableId: ids.table('al-balad', 'L2'), kind: 'bill', status: 'open', createdAt: new Date(now.getTime() - 60000) },
    { id: createId(), branchId: ids.branch('al-balad'), tableId: ids.table('al-balad', 'C5'), kind: 'waiter', status: 'done', createdAt: new Date(now.getTime() - 40 * 60000), doneAt: new Date(now.getTime() - 37 * 60000) },
  ]);

  // ——— first-party analytics (cookieless) ———
  const pages = [
    ['/ar', 26], ['/ar/menu', 18], ['/en', 9], ['/en/menu', 7], ['/ar/reserve', 9], ['/ar/order', 8], ['/en/reserve', 3], ['/en/order', 3],
    ['/ar/locations/al-balad', 4], ['/ar/locations/wadi-hanifah', 3], ['/ar/experiences', 3], ['/ar/gift-cards', 2], ['/ar/journal', 2], ['/en/story', 2],
  ] as const;
  const analytics: (typeof s.analyticsEvents.$inferInsert)[] = [];
  for (let d = -90; d <= 0; d++) {
    const views = Math.round((weekdayOf(addDays(today, d)) >= 4 ? 520 : 360) * (0.85 + random.next() * 0.3) * (d > -30 ? 1.12 : 1) / 4);
    for (let n = 0; n < views; n++) {
      const at = new Date(now.getTime() + d * 864e5 - random.int(0, 86399) * 1000);
      if (at > now) continue;
      const path = random.weighted(pages);
      analytics.push({ id: createId(), visitor: `v${random.int(1, 9000)}`, name: 'page_view', path, locale: path.startsWith('/en') ? 'en' : 'ar', referrer: random.weighted([[null, 45], ['instagram.com', 22], ['google.com', 25], ['tiktok.com', 8]] as const), device: random.weighted([['mobile', 74], ['desktop', 22], ['tablet', 4]] as const), at });
    }
  }
  await insertChunked(analytics, (chunk) => ctx.db.insert(s.analyticsEvents).values(chunk), 400);
  log(`analytics events: ${analytics.length} (sampled at 1:4)`);

  // ——— audit & notifications ———
  await db.insert(s.auditLogs).values([
    { id: createId(), actorId: 'us_manager', actorEmail: 'manager@zill.test', action: 'menu.item.update', entity: 'menu_item', entityId: ids.item('kunafa-nabulsi'), summary: 'Price updated 46 → 48 SAR', at: new Date(now.getTime() - 6 * 864e5) },
    { id: createId(), actorId: 'us_kitchen', actorEmail: 'kitchen@zill.test', action: 'menu.item.sold_out', entity: 'menu_item', entityId: ids.item('eggs-east-wall'), summary: 'Sold out at Al-Balad', at: new Date(now.getTime() - 50 * 60000) },
    { id: createId(), actorId: 'us_owner', actorEmail: 'owner@zill.test', action: 'settings.update', entity: 'settings', entityId: 'features', summary: 'Enabled events and gift cards', at: new Date(now.getTime() - 30 * 864e5) },
    { id: createId(), actorId: 'us_editor', actorEmail: 'editor@zill.test', action: 'journal.publish', entity: 'journal_post', entityId: null, summary: 'Published a journal article', at: new Date(now.getTime() - 3 * 864e5) },
  ]);
  await db.insert(s.notifications).values([
    { id: createId(), role: 'host', branchId: ids.branch('al-balad'), kind: 'waitlist', title: { ar: 'قائمة انتظار الخميس تمتلئ', en: 'Thursday waitlist is filling' }, body: { ar: 'أربعة طلبات انتظار لمساء الخميس.', en: 'Four waitlist requests for Thursday evening.' }, href: '/admin/reservations?view=waitlist', readBy: [] },
    { id: createId(), role: 'manager', branchId: null, kind: 'review', title: { ar: 'مراجعات بانتظار الاعتماد', en: 'Reviews awaiting moderation' }, href: '/admin/reviews', readBy: [] },
    { id: createId(), role: 'manager', branchId: null, kind: 'inquiry', title: { ar: 'طلب عشاءٍ خاص جديد', en: 'New private dining inquiry' }, href: '/admin/inquiries', readBy: [] },
  ]);
}

/** A tiny, valid one-page PDF (for seeded CV uploads). */
function minimalPdf(text: string): Buffer {
  const safe = text.replace(/[()\\]/g, '');
  const stream = `BT /F1 14 Tf 72 720 Td (${safe}) Tj ET`;
  const objects = [
    '1 0 obj << /Type /Catalog /Pages 2 0 R >> endobj',
    '2 0 obj << /Type /Pages /Kids [3 0 R] /Count 1 >> endobj',
    '3 0 obj << /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >> endobj',
    `4 0 obj << /Length ${stream.length} >> stream\n${stream}\nendstream endobj`,
    '5 0 obj << /Type /Font /Subtype /Type1 /BaseFont /Helvetica >> endobj',
  ];
  let body = '%PDF-1.4\n';
  const offsets: number[] = [];
  for (const o of objects) {
    offsets.push(body.length);
    body += `${o}\n`;
  }
  const xref = body.length;
  body += `xref\n0 ${objects.length + 1}\n0000000000 65535 f \n${offsets.map((o) => `${String(o).padStart(10, '0')} 00000 n `).join('\n')}\n`;
  body += `trailer << /Size ${objects.length + 1} /Root 1 0 R >>\nstartxref\n${xref}\n%%EOF\n`;
  return Buffer.from(body, 'latin1');
}

