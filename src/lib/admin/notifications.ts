import 'server-only';
import { and, desc, eq, inArray, isNull, or } from 'drizzle-orm';
import type { CurrentUser } from '@/lib/auth/session';
import { db } from '@/lib/db/client';
import * as s from '@/lib/db/schema';
import type { AdminScope } from './context';

export type StaffNotification = typeof s.notifications.$inferSelect;

/**
 * Notices a staff member sees: owners and managers see every role's, everyone else their own role's and the
 * all-staff ones; always limited to their houses and to notices addressed to them or to nobody in particular.
 */
export async function visibleNotifications(user: CurrentUser, scope: Pick<AdminScope, 'branchIds'>, limit = 50): Promise<StaffNotification[]> {
  const everyRole = user.role === 'owner' || user.role === 'manager';
  return db
    .select()
    .from(s.notifications)
    .where(
      and(
        everyRole ? undefined : or(isNull(s.notifications.role), eq(s.notifications.role, user.role)),
        or(isNull(s.notifications.branchId), inArray(s.notifications.branchId, scope.branchIds)),
        or(isNull(s.notifications.userId), eq(s.notifications.userId, user.id)),
      ),
    )
    .orderBy(desc(s.notifications.createdAt))
    .limit(limit);
}

export function isUnread(n: Pick<StaffNotification, 'readBy'>, userId: string): boolean {
  return !n.readBy.includes(userId);
}

export async function unreadCount(user: CurrentUser, scope: Pick<AdminScope, 'branchIds'>): Promise<{ unread: number; latest: string | null }> {
  const list = await visibleNotifications(user, scope, 100);
  return { unread: list.filter((n) => isUnread(n, user.id)).length, latest: list[0]?.createdAt.toISOString() ?? null };
}

/** Marks notices as read for this staff member only (colleagues still see them as new). */
export async function markRead(user: CurrentUser, scope: Pick<AdminScope, 'branchIds'>, ids: string[] | 'all'): Promise<number> {
  const list = await visibleNotifications(user, scope, 200);
  const targets = list.filter((n) => isUnread(n, user.id) && (ids === 'all' || ids.includes(n.id)));
  for (const n of targets) await db.update(s.notifications).set({ readBy: [...n.readBy, user.id] }).where(eq(s.notifications.id, n.id));
  return targets.length;
}
