'use server';

import { and, eq } from 'drizzle-orm';
import { cookies } from 'next/headers';
import { z } from 'zod';
import { routing } from '@/i18n/routing';
import { db } from '@/lib/db/client';
import { favorites, menuItems } from '@/lib/db/schema';
import { getCurrentUser } from '@/lib/auth/session';
import { getBranch } from '@/lib/queries/branches';
import { requestSubscription } from '@/lib/server/newsletter';
import { featureEnabled } from '@/lib/server/settings';
import { BRANCH_COOKIE } from '@/lib/site/selection';
import { guardPublic } from './guard';
import { fail, ok, zodFieldErrors, type ActionResult } from './result';

const localeSchema = z.enum(routing.locales);

/** Remembers the visitor's house for a year (drives the sun, the menu of the hour and ordering). */
export async function selectBranch(slug: string): Promise<ActionResult<{ slug: string }>> {
  const parsed = z.string().min(1).max(64).safeParse(slug);
  if (!parsed.success) return fail('invalid');
  const branch = await getBranch(parsed.data);
  if (!branch) return fail('notFound');
  (await cookies()).set(BRANCH_COOKIE, branch.slug, { path: '/', maxAge: 60 * 60 * 24 * 365, sameSite: 'lax', httpOnly: false, secure: process.env.NODE_ENV === 'production' });
  return ok({ slug: branch.slug });
}

const newsletterSchema = z.object({
  email: z.email('email').max(254),
  locale: localeSchema,
  source: z.string().max(40).optional(),
});

/** Double opt-in newsletter signup (see requestSubscription). */
export async function subscribeNewsletter(form: FormData): Promise<ActionResult<{ email: string }>> {
  if (!(await featureEnabled('newsletter'))) return fail('disabled');
  const blocked = await guardPublic('newsletter', form, 5, 3600);
  if (blocked) return blocked;
  const parsed = newsletterSchema.safeParse({ email: String(form.get('email') ?? '').trim().toLowerCase(), locale: form.get('locale'), source: form.get('source') ?? undefined });
  if (!parsed.success) return fail('validation', zodFieldErrors(parsed.error));
  const { email, locale, source } = parsed.data;

  await requestSubscription(email, locale, source ?? 'footer');
  return ok({ email });
}

/** Adds or removes a dish from the signed-in guest's favourites. */
export async function toggleFavourite(slug: string): Promise<ActionResult<{ favourite: boolean }>> {
  const parsed = z.string().min(1).max(80).regex(/^[a-z0-9-]+$/).safeParse(slug);
  if (!parsed.success) return fail('invalid');
  const user = await getCurrentUser();
  if (!user) return fail('unauthorized');
  const item = await db.query.menuItems.findFirst({ where: eq(menuItems.slug, parsed.data) });
  if (!item) return fail('notFound');
  const existing = await db.query.favorites.findFirst({ where: and(eq(favorites.userId, user.id), eq(favorites.itemId, item.id)) });
  if (existing) {
    await db.delete(favorites).where(and(eq(favorites.userId, user.id), eq(favorites.itemId, item.id)));
    return ok({ favourite: false });
  }
  await db.insert(favorites).values({ userId: user.id, itemId: item.id });
  return ok({ favourite: true });
}
