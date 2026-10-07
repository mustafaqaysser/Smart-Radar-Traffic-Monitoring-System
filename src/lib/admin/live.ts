import 'server-only';
import { and, eq, gt, inArray, max, sql } from 'drizzle-orm';
import type { CurrentUser } from '@/lib/auth/session';
import { can } from '@/lib/auth/permissions';
import { db } from '@/lib/db/client';
import * as s from '@/lib/db/schema';
import type { AdminScope } from './context';
import { unreadCount } from './notifications';

/**
 * A small summary of what changed, sent to open admin screens every few seconds. Screens compare versions and
 * re-render from the server when theirs moves; the orders board also chimes for newly placed orders.
 */
export interface LivePulse {
  orders: { version: string; placed: string[] } | null;
  requests: { version: string; open: number } | null;
  reservations: { version: string } | null;
  notifications: { unread: number; latest: string | null };
}

const DAY = 86_400_000;

export async function livePulse(user: CurrentUser, scope: AdminScope, now = new Date()): Promise<LivePulse> {
  const branchIds = scope.branchIds;
  const since = new Date(now.getTime() - 2 * DAY);
  const [orders, requests, reservations, notifications] = await Promise.all([
    can(user.role, 'orders:view')
      ? Promise.all([
          db
            .select({ v: max(s.orders.updatedAt), n: sql<number>`count(*)` })
            .from(s.orders)
            .where(and(inArray(s.orders.branchId, branchIds), gt(s.orders.updatedAt, since))),
          db
            .select({ id: s.orders.id })
            .from(s.orders)
            .where(and(inArray(s.orders.branchId, branchIds), eq(s.orders.status, 'placed'))),
        ]).then(([[agg], placed]) => ({ version: `${agg?.v?.getTime() ?? 0}:${agg?.n ?? 0}`, placed: placed.map((p) => p.id) }))
      : null,
    can(user.role, 'tables:respond')
      ? db
          .select({
            created: max(s.tableRequests.createdAt),
            acked: max(s.tableRequests.acknowledgedAt),
            done: max(s.tableRequests.doneAt),
            open: sql<number>`sum(case when ${s.tableRequests.status} = 'open' then 1 else 0 end)`,
          })
          .from(s.tableRequests)
          .where(and(inArray(s.tableRequests.branchId, branchIds), gt(s.tableRequests.createdAt, since)))
          .then(([r]) => ({ version: `${r?.created?.getTime() ?? 0}:${r?.acked?.getTime() ?? 0}:${r?.done?.getTime() ?? 0}`, open: Number(r?.open ?? 0) }))
      : null,
    can(user.role, 'reservations:view')
      ? Promise.all([
          db.select({ v: max(s.reservations.updatedAt), n: sql<number>`count(*)` }).from(s.reservations).where(inArray(s.reservations.branchId, branchIds)),
          db.select({ v: max(s.waitlistEntries.updatedAt), n: sql<number>`count(*)` }).from(s.waitlistEntries).where(inArray(s.waitlistEntries.branchId, branchIds)),
        ]).then(([[r], [w]]) => ({ version: `${r?.v?.getTime() ?? 0}:${r?.n ?? 0}:${w?.v?.getTime() ?? 0}:${w?.n ?? 0}` }))
      : null,
    unreadCount(user, scope),
  ]);
  return { orders, requests, reservations, notifications };
}
