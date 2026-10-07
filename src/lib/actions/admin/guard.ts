import 'server-only';
import { refresh, updateTag } from 'next/cache';
import type { z } from 'zod';
import { AuthError, getCurrentUser, type CurrentUser } from '@/lib/auth/session';
import { can, isStaff, type Permission } from '@/lib/auth/permissions';
import type { CacheTag } from '@/lib/queries/cache';
import { fail, zodFieldErrors, type ActionResult } from '../result';

export interface StaffActor {
  id: string;
  email: string;
}

export const actorOf = (user: CurrentUser): StaffActor => ({ id: user.id, email: user.email });

/**
 * Every back-office mutation runs through here: the signed-in staff member must hold one of the permissions
 * ('staff' = any staff role, for personal preferences),
 * the input is validated with Zod, public caches named in `tags` are expired (read-your-own-writes), and the
 * admin screen that called the action is refreshed.
 */
export async function staffAction<S extends z.ZodType, T>(
  anyOf: Permission | Permission[] | 'staff',
  schema: S,
  input: unknown,
  fn: (data: z.infer<S>, user: CurrentUser) => Promise<ActionResult<T>>,
  options: { tags?: CacheTag[]; refresh?: boolean } = {},
): Promise<ActionResult<T>> {
  const user = await getCurrentUser();
  if (!user) return fail('unauthorized');
  const allowed = anyOf === 'staff' ? isStaff(user.role) : (Array.isArray(anyOf) ? anyOf : [anyOf]).some((p) => can(user.role, p));
  if (!allowed) return fail('forbidden');
  const parsed = schema.safeParse(input);
  if (!parsed.success) return fail('validation', zodFieldErrors(parsed.error));
  try {
    const result = await fn(parsed.data, user);
    if (result.ok) {
      for (const tag of options.tags ?? []) updateTag(tag);
      if (options.refresh !== false) refresh();
    }
    return result;
  } catch (err) {
    if (err instanceof AuthError) return fail(err.status === 401 ? 'unauthorized' : 'forbidden');
    throw err;
  }
}
