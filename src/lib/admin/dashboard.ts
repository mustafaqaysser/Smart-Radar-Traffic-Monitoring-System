import 'server-only';
import { and, desc, eq, gte, inArray, lt, ne, notInArray } from 'drizzle-orm';
import { db } from '@/lib/db/client';
import * as s from '@/lib/db/schema';
import { addDays, localToUtc, toDateString, toLocalMinutes } from '@/lib/time/zoned';
import type { AdminScope } from './context';

const NOT_SALES: s.OrderStatus[] = ['pending_payment', 'rejected', 'cancelled'];
const COVER_STATUSES: s.ReservationStatus[] = ['pending', 'confirmed', 'seated', 'completed'];

/** Sales on an order, excluding the tip (which belongs to the team) — the figure every report uses. */
export const orderSales = (o: Pick<typeof s.orders.$inferSelect, 'total' | 'tip'>) => Math.max(0, o.total - o.tip);

export interface DayFigures {
  covers: number;
  reservations: number;
  noShows: number;
  orders: number;
  revenue: number;
  averageOrder: number;
}

export interface DashboardData {
  today: string;
  figures: DayFigures;
  /** The same weekday last week, for comparison. */
  lastWeek: DayFigures;
  salesByDay: { date: string; revenue: number; orders: number }[];
  ordersByHour: { hour: number; today: number; lastWeek: number }[];
  channels: { channel: s.OrderChannel; orders: number; revenue: number }[];
  visitors: { date: string; views: number; visitors: number }[];
  upcoming: { id: string; code: string; time: string; name: string; partySize: number; status: s.ReservationStatus; tables: string[]; branchId: string; occasion: string | null; notes: boolean }[];
  kitchen: Partial<Record<s.OrderStatus, number>>;
  openRequests: number;
}

function dayBounds(date: string, timeZone: string): [Date, Date] {
  return [localToUtc(date, 0, timeZone, true) as Date, localToUtc(addDays(date, 1), 0, timeZone, true) as Date];
}

/**
 * Figures for one service day. Bookings count for the whole day (they are known in advance); orders, sales and
 * no-shows count up to `until`, so today's partial day is compared with the same hour a week earlier.
 */
async function figuresFor(scope: AdminScope, date: string, until?: Date): Promise<DayFigures> {
  const [from, dayEnd] = dayBounds(date, scope.timeZone);
  const to = until && until < dayEnd ? until : dayEnd;
  const [bookings, orders] = await Promise.all([
    db
      .select({ status: s.reservations.status, partySize: s.reservations.partySize, startsAt: s.reservations.startsAt })
      .from(s.reservations)
      .where(and(inArray(s.reservations.branchId, scope.branchIds), eq(s.reservations.date, date), ne(s.reservations.status, 'cancelled'))),
    db
      .select({ total: s.orders.total, tip: s.orders.tip })
      .from(s.orders)
      .where(and(inArray(s.orders.branchId, scope.branchIds), gte(s.orders.createdAt, from), lt(s.orders.createdAt, to), notInArray(s.orders.status, NOT_SALES))),
  ]);
  const revenue = orders.reduce((sum, o) => sum + orderSales(o), 0);
  return {
    covers: bookings.filter((b) => COVER_STATUSES.includes(b.status)).reduce((sum, b) => sum + b.partySize, 0),
    reservations: bookings.length,
    noShows: bookings.filter((b) => b.status === 'no_show' && b.startsAt < to).length,
    orders: orders.length,
    revenue,
    averageOrder: orders.length ? Math.round(revenue / orders.length) : 0,
  };
}

export async function dashboardData(scope: AdminScope, now = new Date()): Promise<DashboardData> {
  const tz = scope.timeZone;
  const today = toDateString(now, tz);
  const weekAgo = addDays(today, -7);
  const [chartFrom] = dayBounds(addDays(today, -13), tz);
  const [mixFrom] = dayBounds(addDays(today, -29), tz);
  const [, todayEnd] = dayBounds(today, tz);
  const [weekAgoFrom, weekAgoTo] = dayBounds(weekAgo, tz);

  const [todayFrom] = dayBounds(today, tz);
  const [figures, lastWeek, recentOrders, upcomingRows, kitchenRows, requests, views] = await Promise.all([
    figuresFor(scope, today, now),
    figuresFor(scope, weekAgo, new Date(weekAgoFrom.getTime() + (now.getTime() - todayFrom.getTime()))),
    db
      .select({ createdAt: s.orders.createdAt, total: s.orders.total, tip: s.orders.tip, channel: s.orders.channel })
      .from(s.orders)
      .where(and(inArray(s.orders.branchId, scope.branchIds), gte(s.orders.createdAt, mixFrom), lt(s.orders.createdAt, todayEnd), notInArray(s.orders.status, NOT_SALES))),
    db
      .select()
      .from(s.reservations)
      .where(and(inArray(s.reservations.branchId, scope.branchIds), eq(s.reservations.date, today), inArray(s.reservations.status, ['pending', 'confirmed', 'seated'])))
      .orderBy(s.reservations.startsAt)
      .limit(40),
    db
      .select({ status: s.orders.status })
      .from(s.orders)
      .where(and(inArray(s.orders.branchId, scope.branchIds), inArray(s.orders.status, ['placed', 'accepted', 'preparing', 'ready', 'out_for_delivery']))),
    db
      .select({ id: s.tableRequests.id })
      .from(s.tableRequests)
      .where(and(inArray(s.tableRequests.branchId, scope.branchIds), ne(s.tableRequests.status, 'done'))),
    db
      .select({ at: s.analyticsEvents.at, visitor: s.analyticsEvents.visitor })
      .from(s.analyticsEvents)
      .where(and(eq(s.analyticsEvents.name, 'page_view'), gte(s.analyticsEvents.at, chartFrom), lt(s.analyticsEvents.at, todayEnd))),
  ]);

  const days = Array.from({ length: 14 }, (_, i) => addDays(today, i - 13));
  const salesByDay = days.map((date) => ({ date, revenue: 0, orders: 0 }));
  const dayIndex = new Map(days.map((d, i) => [d, i]));
  const ordersByHour = Array.from({ length: 24 }, (_, hour) => ({ hour, today: 0, lastWeek: 0 }));
  const channelMap = new Map<s.OrderChannel, { orders: number; revenue: number }>();
  for (const o of recentOrders) {
    const sales = orderSales(o);
    const c = channelMap.get(o.channel) ?? { orders: 0, revenue: 0 };
    channelMap.set(o.channel, { orders: c.orders + 1, revenue: c.revenue + sales });
    if (o.createdAt >= chartFrom) {
      const i = dayIndex.get(toDateString(o.createdAt, tz));
      const bucket = i === undefined ? undefined : salesByDay[i];
      if (bucket) {
        bucket.revenue += sales;
        bucket.orders += 1;
      }
    }
    const hour = Math.floor(toLocalMinutes(o.createdAt, tz) / 60);
    const slot = ordersByHour[hour];
    if (!slot) continue;
    if (o.createdAt >= todayFrom) slot.today += 1;
    else if (o.createdAt >= weekAgoFrom && o.createdAt < weekAgoTo) slot.lastWeek += 1;
  }

  const visitorsByDay = days.map((date) => ({ date, views: 0, visitors: new Set<string>() }));
  for (const v of views) {
    const i = dayIndex.get(toDateString(v.at, tz));
    const bucket = i === undefined ? undefined : visitorsByDay[i];
    if (!bucket) continue;
    bucket.views += 1;
    bucket.visitors.add(v.visitor);
  }

  // Only the hours the houses actually trade (with an hour either side), not a flat line through the night.
  const busy = ordersByHour.filter((h) => h.today || h.lastWeek).map((h) => h.hour);
  const firstHour = busy.length ? Math.max(0, Math.min(...busy) - 1) : 8;
  const lastHour = busy.length ? Math.min(23, Math.max(...busy) + 1) : 23;

  const kitchen: Partial<Record<s.OrderStatus, number>> = {};
  for (const k of kitchenRows) kitchen[k.status] = (kitchen[k.status] ?? 0) + 1;

  const tableIds = [...new Set(upcomingRows.flatMap((r) => r.tableIds))];
  const tables = tableIds.length ? await db.select({ id: s.diningTables.id, label: s.diningTables.label }).from(s.diningTables).where(inArray(s.diningTables.id, tableIds)) : [];
  const label = new Map(tables.map((t) => [t.id, t.label]));

  return {
    today,
    figures,
    lastWeek,
    salesByDay,
    ordersByHour: ordersByHour.slice(firstHour, lastHour + 1),
    channels: (['delivery', 'pickup', 'dine_in'] as const).map((channel) => ({ channel, ...(channelMap.get(channel) ?? { orders: 0, revenue: 0 }) })),
    visitors: visitorsByDay.map((d) => ({ date: d.date, views: d.views, visitors: d.visitors.size })),
    upcoming: upcomingRows
      .filter((r) => r.status === 'seated' || r.endsAt > now)
      .slice(0, 12)
      .map((r) => ({ id: r.id, code: r.code, time: r.time, name: r.name, partySize: r.partySize, status: r.status, tables: r.tableIds.map((t) => label.get(t) ?? t), branchId: r.branchId, occasion: r.occasion, notes: Boolean(r.notes || r.dietaryNotes) })),
    kitchen,
    openRequests: requests.length,
  };
}

export interface ActivityItem {
  id: string;
  kind: 'order' | 'reservation' | 'request' | 'review' | 'giftCard' | 'ticket';
  at: string;
  /** Message key in admin.dashboard.activity plus its values. */
  key: string;
  values: Record<string, string | number>;
  href: string | null;
}

/** The latest things that happened across the houses in scope, newest first. */
export async function recentActivity(scope: AdminScope, limit = 14): Promise<ActivityItem[]> {
  const [events, bookings, requests, reviews] = await Promise.all([
    db
      .select({ id: s.orderEvents.id, status: s.orderEvents.status, at: s.orderEvents.at, number: s.orders.number, name: s.orders.name, channel: s.orders.channel })
      .from(s.orderEvents)
      .innerJoin(s.orders, eq(s.orders.id, s.orderEvents.orderId))
      .where(inArray(s.orders.branchId, scope.branchIds))
      .orderBy(desc(s.orderEvents.at))
      .limit(limit),
    db
      .select({ id: s.reservations.id, code: s.reservations.code, name: s.reservations.name, partySize: s.reservations.partySize, date: s.reservations.date, time: s.reservations.time, status: s.reservations.status, updatedAt: s.reservations.updatedAt, createdAt: s.reservations.createdAt, source: s.reservations.source })
      .from(s.reservations)
      .where(inArray(s.reservations.branchId, scope.branchIds))
      .orderBy(desc(s.reservations.updatedAt))
      .limit(limit),
    db
      .select({ id: s.tableRequests.id, kind: s.tableRequests.kind, status: s.tableRequests.status, createdAt: s.tableRequests.createdAt, label: s.diningTables.label })
      .from(s.tableRequests)
      .innerJoin(s.diningTables, eq(s.diningTables.id, s.tableRequests.tableId))
      .where(inArray(s.tableRequests.branchId, scope.branchIds))
      .orderBy(desc(s.tableRequests.createdAt))
      .limit(6),
    db.select({ id: s.reviews.id, name: s.reviews.name, rating: s.reviews.rating, createdAt: s.reviews.createdAt, status: s.reviews.status }).from(s.reviews).orderBy(desc(s.reviews.createdAt)).limit(4),
  ]);
  const items: ActivityItem[] = [
    ...events.map((e) => ({ id: `o-${e.id}`, kind: 'order' as const, at: e.at.toISOString(), key: `order.${e.status}`, values: { number: e.number, name: e.name }, href: `/admin/orders/${e.number}` })),
    ...bookings.map((b) => {
      const created = Math.abs(b.updatedAt.getTime() - b.createdAt.getTime()) < 2000;
      return { id: `r-${b.id}`, kind: 'reservation' as const, at: b.updatedAt.toISOString(), key: created ? 'reservation.created' : `reservation.${b.status}`, values: { code: b.code, name: b.name, party: b.partySize, date: b.date, time: b.time }, href: `/admin/reservations/${b.code}` };
    }),
    ...requests.map((r) => ({ id: `t-${r.id}`, kind: 'request' as const, at: r.createdAt.toISOString(), key: `request.${r.kind}`, values: { table: r.label }, href: '/admin/tables' })),
    ...reviews.map((r) => ({ id: `v-${r.id}`, kind: 'review' as const, at: r.createdAt.toISOString(), key: 'review', values: { name: r.name, rating: r.rating }, href: '/admin/reviews' })),
  ];
  return items.sort((a, b) => b.at.localeCompare(a.at)).slice(0, limit);
}
