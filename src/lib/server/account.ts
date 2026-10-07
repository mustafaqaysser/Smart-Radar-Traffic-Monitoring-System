import 'server-only';
import { and, asc, desc, eq, gte, inArray, lt, or, type SQL } from 'drizzle-orm';
import type { AnySQLiteColumn } from 'drizzle-orm/sqlite-core';
import restaurantConfig from '@config';
import type { CurrentUser } from '@/lib/auth/session';
import { db } from '@/lib/db/client';
import * as s from '@/lib/db/schema';
import { nextTier, tierFor } from '@/lib/domain/loyalty';
import { audit } from '@/lib/server/audit';
import { linkToken } from '@/lib/server/tokens';
import { cancelByGuest, changeable, type Reservation } from './reservations';

/** Orders still with the kitchen or on the road. */
export const ACTIVE_ORDER_STATUSES: s.OrderStatus[] = ['placed', 'accepted', 'preparing', 'ready', 'out_for_delivery'];

/**
 * Records that belong to the guest: linked to the account, or — once the email is confirmed — made with the
 * same email before the account existed. An unconfirmed email never unlocks anyone else's history.
 */
function ownedBy(user: Pick<CurrentUser, 'id' | 'email' | 'emailVerified'>, userId: AnySQLiteColumn, email: AnySQLiteColumn): SQL {
  const byId = eq(userId, user.id);
  return user.emailVerified ? (or(byId, eq(email, user.email)) ?? byId) : byId;
}

export function orderToken(o: { id: string }): string {
  return linkToken('order', o.id);
}

export async function accountOrders(user: CurrentUser, limit = 40) {
  return db.query.orders.findMany({ where: ownedBy(user, s.orders.userId, s.orders.email), orderBy: [desc(s.orders.createdAt)], limit });
}

export async function orderItemCounts(orderIds: string[]): Promise<Map<string, number>> {
  if (!orderIds.length) return new Map();
  const rows = await db.select({ orderId: s.orderItems.orderId, quantity: s.orderItems.quantity }).from(s.orderItems).where(inArray(s.orderItems.orderId, orderIds));
  const counts = new Map<string, number>();
  for (const r of rows) counts.set(r.orderId, (counts.get(r.orderId) ?? 0) + r.quantity);
  return counts;
}

export async function accountReservations(user: CurrentUser, now: Date): Promise<{ upcoming: Reservation[]; past: Reservation[] }> {
  const mine = ownedBy(user, s.reservations.userId, s.reservations.email);
  const [upcoming, past] = await Promise.all([
    db.query.reservations.findMany({ where: and(mine, gte(s.reservations.endsAt, now), inArray(s.reservations.status, ['pending', 'confirmed', 'seated'])), orderBy: [asc(s.reservations.startsAt)], limit: 20 }),
    db.query.reservations.findMany({ where: and(mine, lt(s.reservations.startsAt, now)), orderBy: [desc(s.reservations.startsAt)], limit: 20 }),
  ]);
  return { upcoming, past };
}

export async function accountTickets(user: CurrentUser) {
  return db
    .select({ booking: s.eventBookings, event: s.events, ticket: s.eventTicketTypes })
    .from(s.eventBookings)
    .innerJoin(s.events, eq(s.events.id, s.eventBookings.eventId))
    .innerJoin(s.eventTicketTypes, eq(s.eventTicketTypes.id, s.eventBookings.ticketTypeId))
    .where(and(ownedBy(user, s.eventBookings.userId, s.eventBookings.email), inArray(s.eventBookings.status, ['confirmed', 'checked_in'])))
    .orderBy(desc(s.events.startsAt))
    .limit(30);
}

export async function accountGiftCards(user: CurrentUser) {
  const visible = inArray(s.giftCards.status, ['scheduled', 'active', 'redeemed', 'expired']);
  const [given, received] = await Promise.all([
    db.query.giftCards.findMany({ where: and(ownedBy(user, s.giftCards.userId, s.giftCards.purchaserEmail), visible), orderBy: [desc(s.giftCards.createdAt)], limit: 30 }),
    user.emailVerified
      ? db.query.giftCards.findMany({ where: and(eq(s.giftCards.recipientEmail, user.email), inArray(s.giftCards.status, ['active', 'redeemed', 'expired'])), orderBy: [desc(s.giftCards.createdAt)], limit: 30 })
      : Promise.resolve([]),
  ]);
  return { given, received };
}

export function loyaltyStanding(user: Pick<CurrentUser, 'loyaltyPoints' | 'lifetimePoints'>) {
  const tiers = restaurantConfig.loyalty.tiers;
  const tier = tierFor(user.lifetimePoints, tiers);
  const next = nextTier(user.lifetimePoints, tiers);
  const config = tiers.find((t) => t.id === tier.id) ?? tiers[0];
  const nextConfig = next ? tiers.find((t) => t.id === next.tier.id) : null;
  const from = tier.minPoints;
  const to = next?.tier.minPoints ?? null;
  return {
    points: user.loyaltyPoints,
    lifetime: user.lifetimePoints,
    tier: config,
    next: next && nextConfig ? { tier: nextConfig, pointsToGo: next.pointsToGo } : null,
    /** Progress from this tier's threshold to the next, 0–1 (1 at the top tier). */
    progress: to === null ? 1 : Math.min(1, Math.max(0, (user.lifetimePoints - from) / (to - from))),
  };
}

export async function loyaltyHistory(userId: string, limit = 40) {
  return db.query.loyaltyTransactions.findMany({ where: eq(s.loyaltyTransactions.userId, userId), orderBy: [desc(s.loyaltyTransactions.at)], limit });
}

export async function accountAddresses(userId: string) {
  return db.select().from(s.addresses).where(eq(s.addresses.userId, userId)).orderBy(desc(s.addresses.isDefault), desc(s.addresses.createdAt));
}

export async function favouriteItemIds(userId: string): Promise<string[]> {
  const rows = await db.select({ itemId: s.favorites.itemId }).from(s.favorites).where(eq(s.favorites.userId, userId)).orderBy(desc(s.favorites.createdAt));
  return rows.map((r) => r.itemId);
}

// ————————————————————————————————————————— data export —————————————————————————————————————————

/** Everything the platform keeps about the guest, as one JSON document (the "download your data" file). */
export async function exportAccountData(user: CurrentUser) {
  const [profile, addresses, favourites, orders, reservations, tickets, cards, loyalty, reviews, newsletter] = await Promise.all([
    db.query.users.findFirst({ where: eq(s.users.id, user.id) }),
    accountAddresses(user.id),
    db
      .select({ slug: s.menuItems.slug, name: s.menuItems.name, savedAt: s.favorites.createdAt })
      .from(s.favorites)
      .innerJoin(s.menuItems, eq(s.menuItems.id, s.favorites.itemId))
      .where(eq(s.favorites.userId, user.id)),
    accountOrders(user, 500),
    db.query.reservations.findMany({ where: ownedBy(user, s.reservations.userId, s.reservations.email), orderBy: [desc(s.reservations.startsAt)] }),
    accountTickets(user),
    accountGiftCards(user),
    loyaltyHistory(user.id, 1000),
    db.query.reviews.findMany({ where: ownedBy(user, s.reviews.userId, s.reviews.email) }),
    db.query.newsletterSubscribers.findFirst({ where: eq(s.newsletterSubscribers.email, user.email) }),
  ]);
  const items = orders.length ? await db.select().from(s.orderItems).where(inArray(s.orderItems.orderId, orders.map((o) => o.id))) : [];
  return {
    exportedAt: new Date().toISOString(),
    restaurant: restaurantConfig.name.en,
    profile: profile
      ? {
          name: profile.name,
          email: profile.email,
          emailVerified: profile.emailVerified,
          phone: profile.phone,
          language: profile.locale,
          dietary: profile.dietary,
          marketingEmail: profile.marketingEmail,
          marketingSms: profile.marketingSms,
          loyaltyPoints: profile.loyaltyPoints,
          lifetimePoints: profile.lifetimePoints,
          createdAt: profile.createdAt.toISOString(),
        }
      : null,
    addresses: addresses.map(({ label, area, street, building, floor, notes, isDefault, createdAt }) => ({ label, area, street, building, floor, notes, isDefault, createdAt: createdAt.toISOString() })),
    favourites: favourites.map((f) => ({ slug: f.slug, name: f.name, savedAt: f.savedAt.toISOString() })),
    orders: orders.map((o) => ({
      number: o.number,
      placedAt: o.createdAt.toISOString(),
      channel: o.channel,
      status: o.status,
      name: o.name,
      email: o.email,
      phone: o.phone,
      address: o.address,
      notes: o.notes,
      totals: { subtotal: o.subtotal, discount: o.discount, deliveryFee: o.deliveryFee, serviceCharge: o.serviceCharge, tax: o.tax, tip: o.tip, giftCard: o.giftCardAmount, loyaltyDiscount: o.loyaltyDiscount, total: o.total, currency: restaurantConfig.currency, minorUnits: 2 },
      items: items.filter((i) => i.orderId === o.id).map((i) => ({ name: i.name, quantity: i.quantity, unitPrice: i.unitPrice, modifiers: i.modifiers, notes: i.notes })),
    })),
    reservations: reservations.map((r) => ({ code: r.code, date: r.date, time: r.time, partySize: r.partySize, area: r.area, occasion: r.occasion, status: r.status, name: r.name, email: r.email, phone: r.phone, notes: r.notes, dietaryNotes: r.dietaryNotes, createdAt: r.createdAt.toISOString() })),
    tickets: tickets.map(({ booking, event }) => ({ code: booking.code, event: event.title, startsAt: event.startsAt.toISOString(), quantity: booking.quantity, total: booking.total, status: booking.status })),
    giftCards: {
      given: cards.given.map((c) => ({ initialAmount: c.initialAmount, balance: c.balance, recipientName: c.recipientName, recipientEmail: c.recipientEmail, message: c.message, status: c.status, createdAt: c.createdAt.toISOString() })),
      received: cards.received.map((c) => ({ code: c.code, initialAmount: c.initialAmount, balance: c.balance, from: c.purchaserName, message: c.message, status: c.status, expiresAt: c.expiresAt?.toISOString() ?? null })),
    },
    loyalty: loyalty.map((l) => ({ kind: l.kind, points: l.points, note: l.note, at: l.at.toISOString() })),
    reviews: reviews.map((r) => ({ rating: r.rating, title: r.title, body: r.body, status: r.status, createdAt: r.createdAt.toISOString() })),
    newsletter: newsletter ? { status: newsletter.status, since: (newsletter.confirmedAt ?? newsletter.createdAt).toISOString() } : null,
  };
}

// ————————————————————————————————————————— deletion —————————————————————————————————————————

export type DeletionBlocker = { kind: 'order'; number: string } | { kind: 'reservation'; code: string; startsAt: Date } | { kind: 'ticket'; code: string; startsAt: Date };

/** What deleting the account would do now: bookings that will be cancelled, and anything that must finish first. */
export async function deletionCheck(user: CurrentUser, now: Date): Promise<{ blockers: DeletionBlocker[]; toCancel: Reservation[] }> {
  const [orders, { upcoming }, tickets] = await Promise.all([
    db.query.orders.findMany({ where: and(eq(s.orders.userId, user.id), inArray(s.orders.status, ACTIVE_ORDER_STATUSES)) }),
    accountReservations(user, now),
    accountTickets(user),
  ]);
  const blockers: DeletionBlocker[] = orders.map((o) => ({ kind: 'order', number: o.number }));
  const toCancel: Reservation[] = [];
  for (const r of upcoming) {
    if (changeable(r, now)) toCancel.push(r);
    else blockers.push({ kind: 'reservation', code: r.code, startsAt: r.startsAt });
  }
  for (const { booking, event } of tickets) if (booking.status === 'confirmed' && event.endsAt > now) blockers.push({ kind: 'ticket', code: booking.code, startsAt: event.startsAt });
  return { blockers, toCancel };
}

/**
 * Deletes the account: upcoming bookings are cancelled (with the usual email and refund rules), the profile and
 * everything personal is removed, and past orders and bookings stay for the books with the name and contact
 * details replaced. Every session is ended.
 */
export async function deleteAccount(user: CurrentUser, now: Date): Promise<{ ok: true } | { ok: false; blockers: DeletionBlocker[] }> {
  const { blockers, toCancel } = await deletionCheck(user, now);
  if (blockers.length) return { ok: false, blockers };
  for (const r of toCancel) await cancelByGuest(r, now);

  const ghostEmail = `deleted-${user.id}@deleted.invalid`;
  const ghost = { name: '—', email: ghostEmail, phone: '' };
  const email = user.email;
  await db.transaction(async (tx) => {
    await tx.update(s.orders).set({ ...ghost, address: null, notes: null, userId: null }).where(eq(s.orders.userId, user.id));
    await tx.update(s.reservations).set({ ...ghost, notes: null, dietaryNotes: null, userId: null }).where(eq(s.reservations.userId, user.id));
    await tx.update(s.eventBookings).set({ ...ghost, userId: null }).where(eq(s.eventBookings.userId, user.id));
    await tx.update(s.giftCards).set({ purchaserName: '—', purchaserEmail: ghostEmail, userId: null }).where(eq(s.giftCards.userId, user.id));
    await tx.update(s.reviews).set({ name: user.locale === 'en' ? 'A guest' : 'ضيف', email: ghostEmail, userId: null }).where(eq(s.reviews.userId, user.id));
    if (user.emailVerified) {
      // Bookings made with the same, confirmed, address before the account existed are the guest's too.
      await tx.update(s.orders).set({ ...ghost, address: null, notes: null }).where(and(eq(s.orders.email, email), inArray(s.orders.status, ['completed', 'cancelled', 'rejected'])));
      await tx.update(s.reservations).set({ ...ghost, notes: null, dietaryNotes: null }).where(and(eq(s.reservations.email, email), lt(s.reservations.startsAt, now)));
    }
    await tx.delete(s.newsletterSubscribers).where(eq(s.newsletterSubscribers.email, email));
    await tx.delete(s.addresses).where(eq(s.addresses.userId, user.id));
    await tx.delete(s.favorites).where(eq(s.favorites.userId, user.id));
    await tx.delete(s.carts).where(eq(s.carts.userId, user.id));
    await tx.delete(s.loyaltyTransactions).where(eq(s.loyaltyTransactions.userId, user.id));
    await tx.delete(s.sessions).where(eq(s.sessions.userId, user.id));
    await tx.delete(s.accounts).where(eq(s.accounts.userId, user.id));
    await tx
      .update(s.users)
      .set({ name: '—', email: ghostEmail, emailVerified: false, image: null, phone: null, dietary: null, marketingEmail: false, marketingSms: false, loyaltyPoints: 0, lifetimePoints: 0, tags: null, staffNotes: null, disabled: true, deletedAt: now, updatedAt: now })
      .where(eq(s.users.id, user.id));
  });
  await audit({ actor: { id: user.id, email: ghostEmail }, action: 'account.delete', entity: 'user', entityId: user.id, summary: `Cancelled ${toCancel.length} upcoming booking(s)` });
  return { ok: true };
}
