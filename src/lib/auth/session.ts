import 'server-only';
import { cache } from 'react';
import { headers } from 'next/headers';
import { redirect } from 'next/navigation';
import { eq } from 'drizzle-orm';
import { db } from '@/lib/db/client';
import { users, type Role } from '@/lib/db/schema';
import { auth } from './auth';
import { can, isStaff, type Permission } from './permissions';

export interface CurrentUser {
  id: string;
  name: string;
  email: string;
  role: Role;
  phone: string | null;
  locale: string;
  branchId: string | null;
}

/** The signed-in user (fresh from the database so role changes apply immediately), or null. */
export const getCurrentUser = cache(async (): Promise<CurrentUser | null> => {
  const session = await auth.api.getSession({ headers: await headers() });
  if (!session) return null;
  const row = await db.query.users.findFirst({ where: eq(users.id, session.user.id) });
  if (!row || row.disabled || row.deletedAt) return null;
  return { id: row.id, name: row.name, email: row.email, role: row.role, phone: row.phone, locale: row.locale, branchId: row.branchId };
});

export class AuthError extends Error {
  constructor(public readonly status: 401 | 403) {
    super(status === 401 ? 'Not signed in' : 'Not allowed');
  }
}

/** For server actions and route handlers: throws AuthError instead of redirecting. */
export async function requireUser(): Promise<CurrentUser> {
  const user = await getCurrentUser();
  if (!user) throw new AuthError(401);
  return user;
}

export async function requirePermission(permission: Permission): Promise<CurrentUser> {
  const user = await requireUser();
  if (!can(user.role, permission)) throw new AuthError(403);
  return user;
}

export async function requireStaff(): Promise<CurrentUser> {
  const user = await requireUser();
  if (!isStaff(user.role)) throw new AuthError(403);
  return user;
}

/** For admin pages: redirects to sign-in, or to the forbidden page. */
export async function requirePagePermission(permission: Permission): Promise<CurrentUser> {
  const user = await getCurrentUser();
  if (!user) redirect('/admin/sign-in');
  if (!can(user.role, permission)) redirect('/admin/forbidden');
  return user;
}

export async function requireStaffPage(): Promise<CurrentUser> {
  const user = await getCurrentUser();
  if (!user) redirect('/admin/sign-in');
  if (!isStaff(user.role)) redirect('/admin/forbidden');
  return user;
}
