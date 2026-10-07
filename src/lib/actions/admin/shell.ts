'use server';

import { and, desc, eq, inArray, like, or } from 'drizzle-orm';
import { cookies } from 'next/headers';
import { z } from 'zod';
import { routing } from '@/i18n/routing';
import { ADMIN_LOCALE_COOKIE } from '@/i18n/request';
import { ADMIN_BRANCH_COOKIE, ADMIN_SOUND_COOKIE, ADMIN_THEME_COOKIE, ADMIN_THEMES, getAdminScope } from '@/lib/admin/context';
import { markRead } from '@/lib/admin/notifications';
import { can, isStaff, homeFor } from '@/lib/auth/permissions';
import { getCurrentUser } from '@/lib/auth/session';
import { db } from '@/lib/db/client';
import * as s from '@/lib/db/schema';
import { normalizeDigits } from '@/lib/i18n/digits';
import { fail, ok, type ActionResult } from '../result';
import { staffAction } from './guard';

const YEAR = 60 * 60 * 24 * 365;
const cookieOptions = { path: '/', maxAge: YEAR, sameSite: 'lax' as const, httpOnly: true, secure: process.env.NODE_ENV === 'production' };

/** After a successful sign-in on /admin/sign-in: staff only, language from their profile, and where to land. */
export async function completeStaffSignIn(): Promise<ActionResult<{ href: string }>> {
  const user = await getCurrentUser();
  if (!user) return fail('unauthorized');
  if (!isStaff(user.role)) return fail('notStaff');
  const store = await cookies();
  if ((routing.locales as readonly string[]).includes(user.locale)) store.set(ADMIN_LOCALE_COOKIE, user.locale, cookieOptions);
  await db.update(s.users).set({ lastSeenAt: new Date() }).where(eq(s.users.id, user.id));
  return ok({ href: homeFor(user.role) });
}

export async function setAdminLocale(locale: string): Promise<ActionResult<null>> {
  return staffAction('staff', z.enum(routing.locales), locale, async (value, user) => {
    (await cookies()).set(ADMIN_LOCALE_COOKIE, value, cookieOptions);
    await db.update(s.users).set({ locale: value, updatedAt: new Date() }).where(eq(s.users.id, user.id));
    return ok(null);
  });
}

export async function setAdminTheme(theme: string): Promise<ActionResult<null>> {
  return staffAction('staff', z.enum(ADMIN_THEMES), theme, async (value) => {
    (await cookies()).set(ADMIN_THEME_COOKIE, value, cookieOptions);
    return ok(null);
  });
}

export async function setAdminBranch(branchId: string): Promise<ActionResult<null>> {
  return staffAction('staff', z.string().min(1).max(64), branchId, async (value, user) => {
    if (user.branchId) return fail('forbidden');
    const scope = await getAdminScope(user);
    if (value !== 'all' && !scope.branches.some((b) => b.id === value)) return fail('notFound');
    (await cookies()).set(ADMIN_BRANCH_COOKIE, value, cookieOptions);
    return ok(null);
  });
}

/** Remembers that this browser may play the new-order chime (browsers block sound until a person allows it). */
export async function setAdminSound(enabled: boolean): Promise<ActionResult<null>> {
  return staffAction('staff', z.boolean(), enabled, async (value) => {
    (await cookies()).set(ADMIN_SOUND_COOKIE, value ? '1' : '0', cookieOptions);
    return ok(null);
  }, { refresh: false });
}

export async function markNotificationsRead(ids: string[] | 'all'): Promise<ActionResult<{ count: number }>> {
  return staffAction('staff', z.union([z.literal('all'), z.array(z.string().max(64)).max(200)]), ids, async (value, user) => {
    const scope = await getAdminScope(user);
    return ok({ count: await markRead(user, scope, value) });
  });
}

export interface SearchHit {
  kind: 'order' | 'reservation' | 'customer' | 'giftCard' | 'ticket';
  id: string;
  title: string;
  subtitle: string;
  href: string;
}

/** The command palette's record search: order numbers, booking codes, guests and gift cards the role may open. */
export async function adminSearch(query: string): Promise<ActionResult<SearchHit[]>> {
  return staffAction(
    'staff',
    z.string().trim().min(2).max(80),
    query,
    async (raw, user) => {
      const q = normalizeDigits(raw);
      const scope = await getAdminScope(user);
      const term = `%${q.replace(/[%_]/g, '')}%`;
      const upper = q.toUpperCase();
      const hits: SearchHit[] = [];
      if (can(user.role, 'orders:view')) {
        const rows = await db
          .select({ id: s.orders.id, number: s.orders.number, name: s.orders.name, status: s.orders.status })
          .from(s.orders)
          .where(and(inArray(s.orders.branchId, scope.branchIds), or(like(s.orders.number, `%${upper}%`), like(s.orders.email, term), like(s.orders.name, term), like(s.orders.phone, term))))
          .orderBy(desc(s.orders.createdAt))
          .limit(6);
        for (const r of rows) hits.push({ kind: 'order', id: r.id, title: r.number, subtitle: r.name, href: `/admin/orders/${r.number}` });
      }
      if (can(user.role, 'reservations:view')) {
        const rows = await db
          .select({ id: s.reservations.id, code: s.reservations.code, name: s.reservations.name, date: s.reservations.date, time: s.reservations.time })
          .from(s.reservations)
          .where(and(inArray(s.reservations.branchId, scope.branchIds), or(like(s.reservations.code, `%${upper}%`), like(s.reservations.email, term), like(s.reservations.name, term), like(s.reservations.phone, term))))
          .orderBy(desc(s.reservations.startsAt))
          .limit(6);
        for (const r of rows) hits.push({ kind: 'reservation', id: r.id, title: r.code, subtitle: `${r.name} · ${r.date} ${r.time}`, href: `/admin/reservations/${r.code}` });
      }
      if (can(user.role, 'customers:view')) {
        const rows = await db
          .select({ id: s.users.id, name: s.users.name, email: s.users.email })
          .from(s.users)
          .where(and(eq(s.users.role, 'customer'), or(like(s.users.email, term), like(s.users.name, term), like(s.users.phone, term))))
          .limit(6);
        for (const r of rows) hits.push({ kind: 'customer', id: r.id, title: r.name, subtitle: r.email, href: `/admin/customers/${r.id}` });
      }
      if (can(user.role, 'giftcards:manage') && upper.length >= 4) {
        const rows = await db.select({ id: s.giftCards.id, code: s.giftCards.code, name: s.giftCards.recipientName }).from(s.giftCards).where(like(s.giftCards.code, `%${upper}%`)).limit(4);
        for (const r of rows) hits.push({ kind: 'giftCard', id: r.id, title: r.code, subtitle: r.name, href: `/admin/gift-cards/${r.id}` });
      }
      if (can(user.role, 'tickets:checkin') && upper.startsWith('ZT')) {
        const rows = await db.select({ id: s.eventBookings.id, code: s.eventBookings.code, name: s.eventBookings.name }).from(s.eventBookings).where(like(s.eventBookings.code, `%${upper}%`)).limit(4);
        for (const r of rows) hits.push({ kind: 'ticket', id: r.id, title: r.code, subtitle: r.name, href: `/admin/events/check-in?code=${encodeURIComponent(r.code)}` });
      }
      return ok(hits);
    },
    { refresh: false },
  );
}
