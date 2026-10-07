import 'server-only';
import { and, asc, desc, eq, gte, inArray, lt } from 'drizzle-orm';
import { db } from '@/lib/db/client';
import * as s from '@/lib/db/schema';
import { formatDateTime, joinParts } from '@/lib/i18n/format';
import { tr } from '@/lib/i18n/localized';
import { allowedNext, type Order } from '@/lib/server/orders';
import { addDays, localToUtc, toDateString } from '@/lib/time/zoned';
import type { AdminScope } from './context';

export const ACTIVE_STATUSES: s.OrderStatus[] = ['placed', 'accepted', 'preparing', 'ready', 'out_for_delivery'];

/** An order as the board, the kitchen display and the ticket show it (names in the staff member's language). */
export interface OrderCard {
  id: string;
  number: string;
  status: s.OrderStatus;
  channel: s.OrderChannel;
  branchId: string;
  branchName: string;
  table: string | null;
  name: string;
  phone: string;
  email: string;
  locale: string;
  createdAt: string;
  acceptedAt: string | null;
  readyAt: string | null;
  promisedAt: string | null;
  asap: boolean;
  prepMinutes: number | null;
  notes: string | null;
  address: string | null;
  addressNotes: string | null;
  items: { id: string; quantity: number; name: string; modifiers: string[]; notes: string | null; lineTotal: number }[];
  itemCount: number;
  total: number;
  tip: number;
  paymentMethod: Order['paymentMethod'];
  paymentStatus: Order['paymentStatus'];
  rejectReason: string | null;
  allowed: s.OrderStatus[];
}

async function toCards(orders: Order[], scope: AdminScope, locale: string): Promise<OrderCard[]> {
  if (!orders.length) return [];
  const ids = orders.map((o) => o.id);
  const tableIds = [...new Set(orders.map((o) => o.tableId).filter((t): t is string => Boolean(t)))];
  const [items, tables] = await Promise.all([
    db.select().from(s.orderItems).where(inArray(s.orderItems.orderId, ids)).orderBy(asc(s.orderItems.id)),
    tableIds.length ? db.select({ id: s.diningTables.id, label: s.diningTables.label }).from(s.diningTables).where(inArray(s.diningTables.id, tableIds)) : [],
  ]);
  const tableLabel = new Map(tables.map((t) => [t.id, t.label]));
  const branchName = new Map(scope.branches.map((b) => [b.id, tr(b.shortName, locale)]));
  return orders.map((o) => {
    const lines = items.filter((i) => i.orderId === o.id);
    return {
      id: o.id,
      number: o.number,
      status: o.status,
      channel: o.channel,
      branchId: o.branchId,
      branchName: branchName.get(o.branchId) ?? '',
      table: o.tableId ? (tableLabel.get(o.tableId) ?? null) : null,
      name: o.name,
      phone: o.phone,
      email: o.email,
      locale: o.locale,
      createdAt: o.createdAt.toISOString(),
      acceptedAt: o.acceptedAt?.toISOString() ?? null,
      readyAt: o.readyAt?.toISOString() ?? null,
      promisedAt: o.promisedAt?.toISOString() ?? null,
      asap: o.asap,
      prepMinutes: o.prepMinutes,
      notes: o.notes,
      address: o.address ? joinParts([o.address.area, o.address.street, o.address.building, o.address.floor], locale) : null,
      addressNotes: o.address?.notes ?? null,
      items: lines.map((i) => ({ id: i.id, quantity: i.quantity, name: tr(i.name, locale), modifiers: i.modifiers.map((m) => tr(m.name, locale)), notes: i.notes, lineTotal: i.lineTotal })),
      itemCount: lines.reduce((n, i) => n + i.quantity, 0),
      total: o.total,
      tip: o.tip,
      paymentMethod: o.paymentMethod,
      paymentStatus: o.paymentStatus,
      rejectReason: o.rejectReason,
      allowed: allowedNext(o),
    };
  });
}

export interface BoardData {
  active: OrderCard[];
  awaitingPayment: number;
  doneToday: { completed: number; rejected: number; cancelled: number };
  branches: { id: string; name: string; orderingPaused: boolean; busyMode: boolean; busyExtraMinutes: number }[];
}

/** Everything on the live board: open orders oldest first, plus today's tallies and each house's switches. */
export async function boardData(scope: AdminScope, locale: string, now = new Date()): Promise<BoardData> {
  const today = toDateString(now, scope.timeZone);
  const from = localToUtc(today, 0, scope.timeZone, true) as Date;
  const [active, pending, done, branchRows] = await Promise.all([
    db.query.orders.findMany({ where: and(inArray(s.orders.branchId, scope.branchIds), inArray(s.orders.status, ACTIVE_STATUSES)), orderBy: [asc(s.orders.createdAt)] }),
    db.select({ id: s.orders.id }).from(s.orders).where(and(inArray(s.orders.branchId, scope.branchIds), eq(s.orders.status, 'pending_payment'))),
    db
      .select({ status: s.orders.status })
      .from(s.orders)
      .where(and(inArray(s.orders.branchId, scope.branchIds), gte(s.orders.updatedAt, from), inArray(s.orders.status, ['completed', 'rejected', 'cancelled']))),
    db.select().from(s.branches).where(inArray(s.branches.id, scope.branchIds)).orderBy(asc(s.branches.sortOrder)),
  ]);
  return {
    active: await toCards(active, scope, locale),
    awaitingPayment: pending.length,
    doneToday: {
      completed: done.filter((d) => d.status === 'completed').length,
      rejected: done.filter((d) => d.status === 'rejected').length,
      cancelled: done.filter((d) => d.status === 'cancelled').length,
    },
    branches: branchRows.map((b) => ({ id: b.id, name: tr(b.shortName, locale), orderingPaused: b.orderingPaused, busyMode: b.busyMode, busyExtraMinutes: b.orderingSettings.busyExtraMinutes })),
  };
}

/** Orders for the kitchen display: accepted and cooking, plus what is ready to go. */
export async function kitchenOrders(branchId: string, scope: AdminScope, locale: string): Promise<{ cards: OrderCard[]; incoming: number }> {
  const [rows, incoming] = await Promise.all([
    db.query.orders.findMany({ where: and(eq(s.orders.branchId, branchId), inArray(s.orders.status, ['accepted', 'preparing', 'ready'])), orderBy: [asc(s.orders.acceptedAt), asc(s.orders.createdAt)] }),
    db.select({ id: s.orders.id }).from(s.orders).where(and(eq(s.orders.branchId, branchId), eq(s.orders.status, 'placed'))),
  ]);
  return { cards: await toCards(rows, scope, locale), incoming: incoming.length };
}

export interface OrderRow {
  id: string;
  number: string;
  status: s.OrderStatus;
  channel: s.OrderChannel;
  branchName: string;
  name: string;
  email: string;
  phone: string;
  createdAt: string;
  /** Formatted on the server: Node and browsers format dates with slightly different punctuation. */
  placedLabel: string;
  total: number;
  paymentMethod: Order['paymentMethod'];
  paymentStatus: Order['paymentStatus'];
  promoCode: string | null;
}

/** Order history for the table view: the chosen date range in the scope's time zone (newest first). */
export async function orderHistory(scope: AdminScope, locale: string, range: { from: string; to: string }): Promise<OrderRow[]> {
  const start = localToUtc(range.from, 0, scope.timeZone, true) as Date;
  const end = localToUtc(addDays(range.to, 1), 0, scope.timeZone, true) as Date;
  const rows = await db
    .select()
    .from(s.orders)
    .where(and(inArray(s.orders.branchId, scope.branchIds), gte(s.orders.createdAt, start), lt(s.orders.createdAt, end)))
    .orderBy(desc(s.orders.createdAt))
    .limit(3000);
  const branchName = new Map(scope.branches.map((b) => [b.id, tr(b.shortName, locale)]));
  return rows.map((o) => ({
    id: o.id,
    number: o.number,
    status: o.status,
    channel: o.channel,
    branchName: branchName.get(o.branchId) ?? '',
    name: o.name,
    email: o.email,
    phone: o.phone,
    createdAt: o.createdAt.toISOString(),
    placedLabel: formatDateTime(o.createdAt, locale, scope.timeZone),
    total: o.total,
    paymentMethod: o.paymentMethod,
    paymentStatus: o.paymentStatus,
    promoCode: o.promoCode,
  }));
}

export interface OrderDetail {
  order: Order;
  card: OrderCard;
  events: { status: s.OrderStatus; note: string | null; at: string; actor: string | null }[];
  customerId: string | null;
}

/** One order with its kitchen history; null when it is outside the staff member's houses. */
export async function orderDetail(number: string, scope: AdminScope, locale: string): Promise<OrderDetail | null> {
  if (!/^[A-Z]{1,4}-\d{3,8}$/.test(number)) return null;
  const order = await db.query.orders.findFirst({ where: eq(s.orders.number, number) });
  if (!order || !scope.branchIds.includes(order.branchId)) return null;
  const [cards, events] = await Promise.all([
    toCards([order], scope, locale),
    db
      .select({ status: s.orderEvents.status, note: s.orderEvents.note, at: s.orderEvents.at, actor: s.users.name })
      .from(s.orderEvents)
      .leftJoin(s.users, eq(s.users.id, s.orderEvents.actorId))
      .where(eq(s.orderEvents.orderId, order.id))
      .orderBy(asc(s.orderEvents.at)),
  ]);
  return {
    order,
    card: cards[0] as OrderCard,
    events: events.map((e) => ({ status: e.status, note: e.note, at: e.at.toISOString(), actor: e.actor })),
    customerId: order.userId,
  };
}
