'use server';

import { z } from 'zod';
import { routing } from '@/i18n/routing';
import { db } from '@/lib/db/client';
import { inquiries, jobApplications, jobPostings } from '@/lib/db/schema';
import { eq } from 'drizzle-orm';
import { getBranch } from '@/lib/queries/branches';
import { notifyStaff } from '@/lib/server/audit';
import { mail } from '@/lib/server/mail';
import { featureEnabled, getSettings } from '@/lib/server/settings';
import { getStorage } from '@/lib/services/storage';
import { checkUpload, safeFileName } from '@/lib/services/uploads';
import { normalizePhone } from '@/lib/services/phone';
import { tr } from '@/lib/i18n/localized';
import { createId } from '@/lib/utils/id';
import { guardPublic } from './guard';
import { fail, ok, zodFieldErrors, type ActionResult } from './result';

const SUBJECTS = ['general', 'reservations', 'events', 'privateDining', 'press', 'feedback', 'other'] as const;
const SUBJECT_LABEL: Record<(typeof SUBJECTS)[number], { ar: string; en: string }> = {
  general: { ar: 'سؤال عام', en: 'General question' },
  reservations: { ar: 'حجز', en: 'Booking' },
  events: { ar: 'لقاء أو تذكرة', en: 'Gathering or ticket' },
  privateDining: { ar: 'جلسة خاصة', en: 'Private dining' },
  press: { ar: 'إعلام', en: 'Press' },
  feedback: { ar: 'ملاحظة عن زيارة', en: 'Visit feedback' },
  other: { ar: 'أمر آخر', en: 'Other' },
};

const contactSchema = z.object({
  name: z.string().trim().min(2, 'tooShort').max(80, 'tooLong'),
  email: z.email('email').max(254),
  phone: z.string().trim().max(32).optional().or(z.literal('')),
  branch: z.string().max(64).optional().or(z.literal('')),
  subject: z.enum(SUBJECTS),
  message: z.string().trim().min(10, 'tooShort').max(4000, 'tooLong'),
  consent: z.literal('on', { message: 'consent' }),
  locale: z.enum(routing.locales),
});

/** The contact form: stored as an inquiry for the team, acknowledged by email. */
export async function sendContactMessage(form: FormData): Promise<ActionResult<{ email: string }>> {
  const blocked = await guardPublic('contact', form, 5, 3600);
  if (blocked) return blocked;
  const parsed = contactSchema.safeParse(Object.fromEntries(form.entries()));
  if (!parsed.success) return fail('validation', zodFieldErrors(parsed.error));
  const d = parsed.data;
  let phone: string | null = null;
  if (d.phone) {
    phone = normalizePhone(d.phone);
    if (!phone) return fail('validation', { phone: 'phone' });
  }
  const branch = d.branch ? await getBranch(d.branch) : null;
  const kind = d.subject === 'press' ? 'press' : d.subject === 'privateDining' ? 'private_dining' : 'contact';
  const id = createId();
  await db.insert(inquiries).values({
    id,
    kind,
    branchId: branch?.id ?? null,
    name: d.name,
    email: d.email.toLowerCase(),
    phone,
    message: `[${SUBJECT_LABEL[d.subject].en}] ${d.message}`,
    locale: d.locale,
  });
  await notifyStaff({ role: 'manager', branchId: branch?.id ?? null, kind: 'inquiry', title: { ar: `رسالة جديدة: ${SUBJECT_LABEL[d.subject].ar}`, en: `New message: ${SUBJECT_LABEL[d.subject].en}` }, body: { ar: d.name, en: d.name }, href: '/admin/inquiries' });
  await mail(d.email, { name: 'inquiry-received', props: { locale: d.locale, name: d.name, kindLabel: tr(SUBJECT_LABEL[d.subject], d.locale), summary: d.message.slice(0, 280) } });
  return ok({ email: d.email });
}

const applicationSchema = z.object({
  posting: z.string().min(1).max(120),
  name: z.string().trim().min(2, 'tooShort').max(80, 'tooLong'),
  email: z.email('email').max(254),
  phone: z.string().trim().min(6, 'phone').max(32),
  message: z.string().trim().max(3000, 'tooLong').optional().or(z.literal('')),
  consent: z.literal('on', { message: 'consent' }),
  locale: z.enum(routing.locales),
});

/** A job application with a CV (checked by content, stored privately; only staff can download it). */
export async function applyForJob(form: FormData): Promise<ActionResult<{ email: string }>> {
  if (!(await featureEnabled('careers'))) return fail('disabled');
  const blocked = await guardPublic('apply', form, 3, 3600);
  if (blocked) return blocked;
  const parsed = applicationSchema.safeParse(Object.fromEntries([...form.entries()].filter(([, v]) => typeof v === 'string')));
  if (!parsed.success) return fail('validation', zodFieldErrors(parsed.error));
  const d = parsed.data;
  const phone = normalizePhone(d.phone);
  if (!phone) return fail('validation', { phone: 'phone' });
  const posting = await db.query.jobPostings.findFirst({ where: eq(jobPostings.slug, d.posting) });
  if (!posting || !posting.isOpen) return fail('notFound');

  const file = form.get('cv');
  if (!(file instanceof File)) return fail('validation', { cv: 'required' });
  const bytes = new Uint8Array(await file.arrayBuffer());
  const check = checkUpload(bytes, 'cv');
  if (!check.ok) return fail('validation', { cv: check.reason === 'too_large' ? 'fileTooLarge' : check.reason === 'empty' ? 'fileEmpty' : 'fileType' });
  const id = createId();
  const key = `private/cv/${id}.${check.ext}`;
  await getStorage().put(key, bytes, check.mime);
  await db.insert(jobApplications).values({
    id,
    postingId: posting.id,
    name: d.name,
    email: d.email.toLowerCase(),
    phone,
    message: d.message || null,
    cvKey: key,
    cvName: safeFileName(file.name),
    locale: d.locale,
  });
  const settings = await getSettings();
  await notifyStaff({ role: 'manager', branchId: posting.branchId, kind: 'application', title: { ar: `طلب توظيف: ${tr(posting.title, 'ar')}`, en: `Application: ${tr(posting.title, 'en')}` }, body: { ar: d.name, en: d.name }, href: '/admin/careers' });
  await mail(d.email, { name: 'application-received', props: { locale: d.locale, name: d.name, role: tr(posting.title, d.locale) } }, { meta: { careers: settings.contact.careers } });
  return ok({ email: d.email });
}
