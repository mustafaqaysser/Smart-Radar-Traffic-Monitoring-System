'use server';

import { z } from 'zod';
import { db } from '@/lib/db/client';
import { reviews } from '@/lib/db/schema';
import { routing } from '@/i18n/routing';
import { getCurrentUser } from '@/lib/auth/session';
import { getBranch } from '@/lib/queries/branches';
import { notifyStaff } from '@/lib/server/audit';
import { featureEnabled } from '@/lib/server/settings';
import { createId } from '@/lib/utils/id';
import { guardPublic } from './guard';
import { fail, ok, zodFieldErrors, type ActionResult } from './result';

const schema = z.object({
  branch: z.string().min(1).max(64),
  name: z.string().trim().min(2, 'tooShort').max(80, 'tooLong'),
  email: z.email('email').max(254),
  rating: z.coerce.number().int().min(1, 'required').max(5),
  title: z.string().trim().max(90, 'tooLong').optional(),
  body: z.string().trim().min(30, 'tooShort').max(2000, 'tooLong'),
  visitDate: z.string().regex(/^\d{4}-\d{2}-\d{2}$/, 'invalid').optional().or(z.literal('')),
  locale: z.enum(routing.locales),
});

/** A guest review: stored as pending, shown only after a manager approves it. */
export async function submitReview(form: FormData): Promise<ActionResult<{ id: string }>> {
  if (!(await featureEnabled('reviews'))) return fail('disabled');
  const blocked = await guardPublic('review', form, 3, 3600);
  if (blocked) return blocked;
  const parsed = schema.safeParse(Object.fromEntries(form.entries()));
  if (!parsed.success) return fail('validation', zodFieldErrors(parsed.error));
  const d = parsed.data;
  const branch = await getBranch(d.branch);
  if (!branch) return fail('validation', { branch: 'required' });
  const user = await getCurrentUser();
  const id = createId();
  await db.insert(reviews).values({
    id,
    branchId: branch.id,
    userId: user?.id ?? null,
    name: d.name,
    email: d.email.toLowerCase(),
    rating: d.rating,
    title: d.title || null,
    body: d.body,
    locale: d.locale,
    visitDate: d.visitDate || null,
    status: 'pending',
  });
  await notifyStaff({
    role: 'manager',
    branchId: branch.id,
    kind: 'review',
    title: { ar: `تقييمٌ جديد (${d.rating}/٥) بانتظار المراجعة`, en: `New ${d.rating}/5 review waiting for moderation` },
    body: { ar: d.title || d.body.slice(0, 80), en: d.title || d.body.slice(0, 80) },
    href: '/admin/reviews',
  });
  return ok({ id });
}
