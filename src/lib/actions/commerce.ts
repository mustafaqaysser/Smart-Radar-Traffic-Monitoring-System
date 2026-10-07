'use server';

import { z } from 'zod';
import restaurantConfig from '@config';
import { routing } from '@/i18n/routing';
import { getCurrentUser } from '@/lib/auth/session';
import { isGiftCardDesign } from '@/lib/brand/gift-card-art';
import { db } from '@/lib/db/client';
import { inquiries } from '@/lib/db/schema';
import { normalizeDigits } from '@/lib/i18n/digits';
import { formatNumber } from '@/lib/i18n/format';
import { tr } from '@/lib/i18n/localized';
import { getBranch } from '@/lib/queries/branches';
import { getPrivateDining } from '@/lib/queries/content';
import { bookEvent, ticketPath } from '@/lib/server/events';
import { giftCardBalance, giftCardPath, purchaseGiftCard } from '@/lib/server/gift-cards';
import { notifyStaff } from '@/lib/server/audit';
import { mail } from '@/lib/server/mail';
import type { StartedPayment } from '@/lib/server/payments';
import { featureEnabled } from '@/lib/server/settings';
import { normalizePhone } from '@/lib/services/phone';
import { limitByIp } from '@/lib/services/rate-limit';
import { addDays, toDateString } from '@/lib/time/zoned';
import { createId } from '@/lib/utils/id';
import { guardPublic } from './guard';
import { fail, ok, zodFieldErrors, type ActionResult } from './result';

const locale = z.enum(routing.locales);
const name = z.string().trim().min(2, 'tooShort').max(80, 'tooLong');
const email = z.email('email').max(254);

// ————————————————————————————————————————— gift cards —————————————————————————————————————————

const giftSchema = z.object({
  amount: z.number().int().min(restaurantConfig.giftCards.min * 100).max(restaurantConfig.giftCards.max * 100),
  design: z.string().refine(isGiftCardDesign, 'design'),
  recipientName: name,
  recipientEmail: email,
  purchaserName: name,
  purchaserEmail: email,
  message: z.string().trim().max(300, 'tooLong').optional().or(z.literal('')),
  deliverAt: z.iso.datetime().nullable(),
  locale,
  company_website: z.string().optional(),
  rendered_at: z.union([z.string(), z.number()]).optional(),
});

/** Buys a gift card: the card waits for payment, then is emailed now or at the chosen time. */
export async function buyGiftCard(input: z.input<typeof giftSchema>): Promise<ActionResult<{ href: string; payment: StartedPayment }>> {
  if (!(await featureEnabled('giftCards'))) return fail('disabled');
  const blocked = await guardPublic('gift-card', input as Record<string, unknown>, 6, 3600);
  if (blocked) return blocked;
  const parsed = giftSchema.safeParse(input);
  if (!parsed.success) return fail('validation', zodFieldErrors(parsed.error));
  const d = parsed.data;
  const deliverAt = d.deliverAt ? new Date(d.deliverAt) : null;
  const now = Date.now();
  if (deliverAt && (deliverAt.getTime() < now - 60_000 || deliverAt.getTime() > now + 366 * 864e5)) return fail('validation', { deliverAt: 'invalid' });
  const user = await getCurrentUser();
  const { card, payment } = await purchaseGiftCard({
    amount: d.amount,
    design: d.design,
    purchaserName: d.purchaserName,
    purchaserEmail: d.purchaserEmail.toLowerCase(),
    recipientName: d.recipientName,
    recipientEmail: d.recipientEmail.toLowerCase(),
    message: d.message || null,
    deliverAt,
    locale: d.locale,
    userId: user?.id ?? null,
  });
  return ok({ href: `/${d.locale}${giftCardPath(card)}`, payment });
}

export interface BalanceView {
  balance: number;
  initial: number;
  expiresAt: string | null;
  usable: boolean;
  reason: string | null;
}

/** Balance check by code (rate limited so codes cannot be guessed by trying). */
export async function checkGiftCardBalance(code: string): Promise<ActionResult<BalanceView>> {
  const rl = await limitByIp('gift-balance', 12, 600);
  if (!rl.ok) return fail('rateLimited', undefined, rl.retryAfterSeconds);
  const parsed = z.string().trim().min(8).max(40).safeParse(code);
  if (!parsed.success) return fail('not_found');
  const result = await giftCardBalance(parsed.data);
  if (!result.ok) return fail(result.reason);
  return ok({ balance: result.balance, initial: result.initial, expiresAt: result.expiresAt, usable: result.usable, reason: result.reason });
}

// ————————————————————————————————————————— events —————————————————————————————————————————

const ticketSchema = z.object({
  event: z.string().min(1).max(120),
  ticketTypeId: z.string().min(1).max(40),
  quantity: z.number().int().min(1).max(12),
  name,
  email,
  phone: z.string().trim().min(6, 'phone').max(32, 'phone'),
  locale,
  company_website: z.string().optional(),
  rendered_at: z.union([z.string(), z.number()]).optional(),
});

/** Books seats at a gathering; paid tickets continue to payment, free ones are emailed at once. */
export async function bookEventSeats(input: z.input<typeof ticketSchema>): Promise<ActionResult<{ href: string; payment: StartedPayment | null }>> {
  if (!(await featureEnabled('events'))) return fail('disabled');
  const blocked = await guardPublic('event-ticket', input as Record<string, unknown>, 8, 3600);
  if (blocked) return blocked;
  const parsed = ticketSchema.safeParse(input);
  if (!parsed.success) return fail('validation', zodFieldErrors(parsed.error));
  const d = parsed.data;
  const phone = normalizePhone(d.phone, restaurantConfig.country);
  if (!phone) return fail('validation', { phone: 'phone' });
  const user = await getCurrentUser();
  const res = await bookEvent({ eventSlug: d.event, ticketTypeId: d.ticketTypeId, quantity: d.quantity, name: d.name, email: d.email.toLowerCase(), phone, locale: d.locale, userId: user?.id ?? null, now: new Date() });
  if (!res.ok) return fail(res.error);
  return ok({ href: `/${d.locale}${ticketPath(res.booking)}`, payment: res.payment });
}

// ————————————————————————————————————————— private dining —————————————————————————————————————————

const inquirySchema = z.object({
  kind: z.enum(['private_dining', 'catering']),
  branch: z.string().max(64).optional().or(z.literal('')),
  room: z.string().max(64).optional().or(z.literal('')),
  date: z.string().regex(/^\d{4}-\d{2}-\d{2}$/, 'invalid').optional().or(z.literal('')),
  guests: z.coerce.number<string>().int('invalid').min(2, 'invalid').max(500, 'invalid'),
  package: z.string().max(40).optional().or(z.literal('')),
  name,
  email,
  phone: z.string().trim().min(6, 'phone').max(32, 'phone'),
  message: z.string().trim().min(10, 'tooShort').max(3000, 'tooLong'),
  consent: z.literal('on', { message: 'consent' }),
  locale,
});

/** Private dining and catering requests go to the events host (inquiries) with an acknowledgement email. */
export async function sendPrivateDiningInquiry(form: FormData): Promise<ActionResult<{ email: string }>> {
  if (!(await featureEnabled('privateDining'))) return fail('disabled');
  const blocked = await guardPublic('private-dining', form, 4, 3600);
  if (blocked) return blocked;
  const raw = Object.fromEntries([...form.entries()].filter((e): e is [string, string] => typeof e[1] === 'string'));
  const parsed = inquirySchema.safeParse({ ...raw, guests: normalizeDigits(raw.guests ?? '') });
  if (!parsed.success) return fail('validation', zodFieldErrors(parsed.error));
  const d = parsed.data;
  const phone = normalizePhone(d.phone, restaurantConfig.country);
  if (!phone) return fail('validation', { phone: 'phone' });
  if (d.date) {
    const today = toDateString(new Date(), restaurantConfig.defaultTimeZone);
    if (d.date < today) return fail('validation', { date: 'dateInPast' });
    if (d.date > addDays(today, 730)) return fail('validation', { date: 'invalid' });
  }
  const { rooms, packages } = await getPrivateDining();
  // A room only applies to dining at a house; it also settles which house.
  const room = d.kind === 'private_dining' && d.room ? rooms.find((r) => r.slug === d.room) : undefined;
  if (d.kind === 'private_dining' && d.room && !room) return fail('validation', { room: 'invalid' });
  if (room && d.guests > Math.max(room.seated, room.standing ?? 0)) return fail('validation', { guests: 'roomSize' });
  const pkg = d.package ? packages.find((p) => p.id === d.package) : undefined;
  if (d.package && (!pkg || pkg.kind !== d.kind)) return fail('validation', { package: 'invalid' });
  if (pkg && d.guests < pkg.minGuests) return fail('validation', { guests: 'minGuests' });
  const branch = room ? await getBranch(room.branchId) : d.branch ? await getBranch(d.branch) : null;
  await db.insert(inquiries).values({
    id: createId(),
    kind: d.kind,
    branchId: branch?.id ?? null,
    roomId: room?.id ?? null,
    name: d.name,
    email: d.email.toLowerCase(),
    phone,
    date: d.date || null,
    guests: d.guests,
    packageId: pkg?.id ?? null,
    message: d.message,
    locale: d.locale,
  });
  const where = (l: 'ar' | 'en') => [room ? tr(room.name, l) : null, branch ? tr(branch.shortName, l) : null, d.date || null].filter(Boolean).join(' · ');
  await notifyStaff({
    role: 'manager',
    branchId: branch?.id ?? null,
    kind: 'inquiry',
    title: {
      ar: `${d.kind === 'catering' ? 'طلب ضيافة' : 'طلب جلسة خاصة'} (عدد الضيوف: ${formatNumber(d.guests, 'ar')})`,
      en: `${d.kind === 'catering' ? 'Catering request' : 'Private dining request'} (${d.guests} guests)`,
    },
    body: { ar: [d.name, where('ar')].filter(Boolean).join(' · '), en: [d.name, where('en')].filter(Boolean).join(' · ') },
    href: '/admin/inquiries',
  });
  await mail(d.email, { name: 'inquiry-received', props: { locale: d.locale, name: d.name, kindLabel: d.kind === 'catering' ? tr({ ar: 'الضيافة في مكانك', en: 'Catering' }, d.locale) : tr({ ar: 'جلسة خاصة', en: 'Private dining' }, d.locale), summary: d.message.slice(0, 280) } });
  return ok({ email: d.email });
}
