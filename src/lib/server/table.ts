import 'server-only';
import { and, desc, eq, gte, inArray } from 'drizzle-orm';
import { db } from '@/lib/db/client';
import * as s from '@/lib/db/schema';
import { getBranch } from '@/lib/queries/branches';
import type { BranchDTO } from '@/lib/queries/types';
import { notifyStaff } from '@/lib/server/audit';
import { linkToken } from '@/lib/server/tokens';
import { createId } from '@/lib/utils/id';

export type DiningTable = typeof s.diningTables.$inferSelect;
export type TableRequestKind = 'waiter' | 'bill';

const MINUTE = 60_000;
/** A call or bill request still open after this long is treated as a new one if pressed again. */
const REQUEST_REPEAT_MINUTES = 15;
/** How far back the table's own activity is shown (a long lunch, a long evening). */
const VISIT_HOURS = 6;

/** The table behind a QR code (codes look like BL-C4-WZQ5), if it is active and takes QR orders. */
export async function tableByCode(raw: string): Promise<{ table: DiningTable; branch: BranchDTO } | null> {
  const code = raw.trim().toUpperCase();
  if (!/^[A-Z0-9]{1,6}(-[A-Z0-9]{1,8}){1,3}$/.test(code)) return null;
  const table = await db.query.diningTables.findFirst({ where: and(eq(s.diningTables.code, code), eq(s.diningTables.isActive, true), eq(s.diningTables.qrEnabled, true)) });
  if (!table) return null;
  const branch = await getBranch(table.branchId);
  return branch ? { table, branch } : null;
}

export interface TableActivity {
  requests: { id: string; kind: TableRequestKind; status: 'open' | 'acknowledged' | 'done'; at: string }[];
  orders: { number: string; status: s.OrderStatus; total: number; at: string; trackPath: string }[];
}

/** What the guests at this table have asked for during this visit: open calls and their orders. */
export async function tableActivity(table: DiningTable, now = new Date()): Promise<TableActivity> {
  const since = new Date(now.getTime() - VISIT_HOURS * 60 * MINUTE);
  const [requests, orders] = await Promise.all([
    db.query.tableRequests.findMany({ where: and(eq(s.tableRequests.tableId, table.id), gte(s.tableRequests.createdAt, since)), orderBy: [desc(s.tableRequests.createdAt)], limit: 10 }),
    db.query.orders.findMany({ where: and(eq(s.orders.tableId, table.id), eq(s.orders.channel, 'dine_in'), gte(s.orders.createdAt, since)), orderBy: [desc(s.orders.createdAt)], limit: 10 }),
  ]);
  return {
    requests: requests.map((r) => ({ id: r.id, kind: r.kind, status: r.status, at: r.createdAt.toISOString() })),
    orders: orders.map((o) => ({ number: o.number, status: o.status, total: o.total, at: o.createdAt.toISOString(), trackPath: `/order/track/${o.number}?token=${linkToken('order', o.id)}` })),
  };
}

/**
 * Calls the waiter or asks for the bill. Pressing again while a request is still open does not create a second
 * one; the waiters at the house are notified live.
 */
export async function requestFromTable(table: DiningTable, kind: TableRequestKind, note: string | null, now = new Date()): Promise<{ id: string; repeated: boolean }> {
  const recent = await db.query.tableRequests.findFirst({
    where: and(eq(s.tableRequests.tableId, table.id), eq(s.tableRequests.kind, kind), inArray(s.tableRequests.status, ['open', 'acknowledged']), gte(s.tableRequests.createdAt, new Date(now.getTime() - REQUEST_REPEAT_MINUTES * MINUTE))),
  });
  if (recent) return { id: recent.id, repeated: true };
  const id = createId();
  await db.insert(s.tableRequests).values({ id, branchId: table.branchId, tableId: table.id, kind, note, status: 'open', createdAt: now });
  await notifyStaff({
    role: 'waiter',
    branchId: table.branchId,
    kind: 'table',
    title: kind === 'bill' ? { ar: `الطاولة ${table.label} تطلب الحساب`, en: `Table ${table.label} asks for the bill` } : { ar: `الطاولة ${table.label} تنادي`, en: `Table ${table.label} is calling` },
    body: note ? { ar: note, en: note } : undefined,
    href: '/admin/tables',
  });
  return { id, repeated: false };
}

/** Staff side: acknowledge ("on my way") or close a table request. */
export async function updateTableRequest(id: string, status: 'acknowledged' | 'done', actor: { id: string }): Promise<boolean> {
  const now = new Date();
  const rows = await db
    .update(s.tableRequests)
    .set(status === 'acknowledged' ? { status, acknowledgedBy: actor.id, acknowledgedAt: now } : { status, doneAt: now, acknowledgedBy: actor.id })
    .where(and(eq(s.tableRequests.id, id), inArray(s.tableRequests.status, status === 'acknowledged' ? ['open'] : ['open', 'acknowledged'])))
    .returning({ id: s.tableRequests.id });
  return rows.length > 0;
}
