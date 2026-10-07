'use server';

import { z } from 'zod';
import restaurantConfig from '@config';
import { routing } from '@/i18n/routing';
import { getCurrentUser } from '@/lib/auth/session';
import { normalizeDigits } from '@/lib/i18n/digits';
import { getBranch } from '@/lib/queries/branches';
import type { BranchDTO } from '@/lib/queries/types';
import { ensureBookingSession, readBookingSession } from '@/lib/server/booking-session';
import { requestSubscription } from '@/lib/server/newsletter';
import { startPayment, type StartedPayment } from '@/lib/server/payments';
import {
  AREA_CHOICES,
  OCCASIONS,
  bookFromHold,
  bookingWindow,
  cancelByGuest,
  changeable,
  confirmationPath,
  joinWaitlist,
  markWaitlistBooked,
  moveByGuest,
  partyRangeForChange,
  releaseHolds,
  reservationByCode,
  holdSlot,
  waitlistOffer,
  waitlistSessionKey,
  type BookResult,
  type BookingDetails,
} from '@/lib/server/reservations';
import { featureEnabled } from '@/lib/server/settings';
import { normalizePhone } from '@/lib/services/phone';
import { limitByIp } from '@/lib/services/rate-limit';
import { guardPublic } from './guard';
import { fail, ok, zodFieldErrors, type ActionResult } from './result';

const rules = restaurantConfig.reservations;

const dateSchema = z.string().regex(/^\d{4}-\d{2}-\d{2}$/);
const timeSchema = z.string().regex(/^([01]\d|2[0-3]):[0-5]\d$/);
const partySchema = z.coerce.number().int().min(1).max(rules.maxPartyOnline);

async function openBranch(slug: string): Promise<BranchDTO | null> {
  if (!(await featureEnabled('reservations'))) return null;
  const branch = await getBranch(slug);
  return branch && branch.reservationsEnabled ? branch : null;
}

function inWindow(branch: BranchDTO, date: string, now: Date): boolean {
  const { first, last } = bookingWindow(branch, now);
  return date >= first && date <= last;
}

const holdSchema = z.object({
  branch: z.string().min(1).max(64),
  date: dateSchema,
  time: timeSchema,
  party: partySchema,
  area: z.enum(AREA_CHOICES),
});

export interface HoldView {
  holdId: string;
  expiresAt: string;
  time: string;
  areas: string[];
}

/** Holds the chosen table while the guest finishes (re-holding replaces the previous hold). */
export async function holdTable(input: z.input<typeof holdSchema>): Promise<ActionResult<HoldView>> {
  const rl = await limitByIp('reserve-hold', 60, 600);
  if (!rl.ok) return fail('rateLimited', undefined, rl.retryAfterSeconds);
  const parsed = holdSchema.safeParse(input);
  if (!parsed.success) return fail('validation');
  const d = parsed.data;
  const branch = await openBranch(d.branch);
  if (!branch) return fail('disabled');
  const now = new Date();
  if (!inWindow(branch, d.date, now)) return fail('window');
  const hold = await holdSlot({ branch, date: d.date, time: d.time, partySize: d.party, area: d.area, sessionKey: await ensureBookingSession(), now });
  if (!hold) return fail('taken');
  return ok({ holdId: hold.id, expiresAt: hold.expiresAt.toISOString(), time: hold.time, areas: hold.areas });
}

/** Lets the table go (the guest changed their mind or left the flow). */
export async function releaseTable(): Promise<ActionResult> {
  const key = await readBookingSession();
  if (key) await releaseHolds(key);
  return ok(undefined);
}

const detailsSchema = z.object({
  name: z.string().trim().min(2, 'tooShort').max(80, 'tooLong'),
  email: z.email('email').max(254),
  phone: z.string().trim().min(6, 'phone').max(32, 'phone'),
  occasion: z.enum([...OCCASIONS, 'none']).default('none'),
  notes: z.string().trim().max(500, 'tooLong').optional().or(z.literal('')),
  dietary: z.string().trim().max(300, 'tooLong').optional().or(z.literal('')),
  marketing: z.literal('on').optional(),
  locale: z.enum(routing.locales),
});

const confirmSchema = detailsSchema.extend({ holdId: z.string().min(10).max(40) });

export type BookingOutcome =
  | { kind: 'confirmed'; href: string }
  | { kind: 'payment'; href: string; code: string; payment: StartedPayment };

function stringFields(form: FormData): Record<string, string> {
  return Object.fromEntries([...form.entries()].filter((e): e is [string, string] => typeof e[1] === 'string'));
}

async function finishBooking(result: Extract<BookResult, { ok: true }>, locale: string, marketing: boolean): Promise<BookingOutcome> {
  const { reservation, deposit, branch } = result;
  if (marketing && (await featureEnabled('newsletter'))) await requestSubscription(reservation.email, locale, 'reservation');
  const href = confirmationPath(reservation, locale);
  if (deposit === 0) return { kind: 'confirmed', href };
  const payment = await startPayment({
    purpose: 'deposit',
    referenceId: reservation.id,
    amount: deposit,
    description: `${branch.shortName.en} · ${reservation.code}`,
    email: reservation.email,
    locale,
    returnPath: href,
  });
  return { kind: 'payment', href, code: reservation.code, payment };
}

async function details(d: z.infer<typeof detailsSchema>, source: BookingDetails['source']): Promise<BookingDetails | { error: Record<string, string> }> {
  const phone = normalizePhone(d.phone, restaurantConfig.country);
  if (!phone) return { error: { phone: 'phone' } };
  const user = await getCurrentUser();
  return {
    name: d.name,
    email: d.email.toLowerCase(),
    phone,
    occasion: d.occasion === 'none' ? null : d.occasion,
    notes: d.notes || null,
    dietaryNotes: d.dietary || null,
    locale: d.locale,
    userId: user?.id ?? null,
    source,
  };
}

/** Confirms the held table with the guest's details; large parties continue to the deposit. */
export async function confirmReservation(form: FormData): Promise<ActionResult<BookingOutcome>> {
  if (!(await featureEnabled('reservations'))) return fail('disabled');
  const blocked = await guardPublic('reserve', form, 8, 600);
  if (blocked) return blocked;
  const parsed = confirmSchema.safeParse(stringFields(form));
  if (!parsed.success) return fail('validation', zodFieldErrors(parsed.error));
  const d = parsed.data;
  const booking = await details(d, 'web');
  if ('error' in booking) return fail('validation', booking.error);
  const key = await readBookingSession();
  if (!key) return fail('holdExpired');
  const result = await bookFromHold({ holdId: d.holdId, sessionKey: key, details: booking, now: new Date() });
  if (!result.ok) return fail(result.error);
  return ok(await finishBooking(result, d.locale, d.marketing === 'on'));
}

const offerSchema = detailsSchema.extend({ entry: z.string().min(10).max(40), token: z.string().min(16).max(64) });

/** Accepts a table offered from the waitlist (held for the guest in their name). */
export async function acceptWaitlistOffer(form: FormData): Promise<ActionResult<BookingOutcome>> {
  if (!(await featureEnabled('reservations'))) return fail('disabled');
  const blocked = await guardPublic('reserve-offer', form, 8, 600);
  if (blocked) return blocked;
  const parsed = offerSchema.safeParse(stringFields(form));
  if (!parsed.success) return fail('validation', zodFieldErrors(parsed.error));
  const d = parsed.data;
  const now = new Date();
  const offer = await waitlistOffer(d.entry, d.token, now);
  if (!offer) return fail('notFound');
  if (!offer.hold) return fail('holdExpired');
  const booking = await details(d, 'web');
  if ('error' in booking) return fail('validation', booking.error);
  const result = await bookFromHold({ holdId: offer.hold.id, sessionKey: waitlistSessionKey(offer.entry.id), details: booking, now });
  if (!result.ok) return fail(result.error);
  await markWaitlistBooked(offer.entry.id, result.reservation.id);
  return ok(await finishBooking(result, d.locale, d.marketing === 'on'));
}

const waitlistSchema = z.object({
  branch: z.string().min(1).max(64),
  date: dateSchema,
  party: partySchema,
  preferredTime: timeSchema,
  flexible: z.enum(['30', '60', '120', '1440']),
  name: z.string().trim().min(2, 'tooShort').max(80, 'tooLong'),
  email: z.email('email').max(254),
  phone: z.string().trim().min(6, 'phone').max(32, 'phone'),
  notes: z.string().trim().max(500, 'tooLong').optional().or(z.literal('')),
  locale: z.enum(routing.locales),
});

/** Adds the guest to the waitlist for a full day; freed tables are offered first come, first served. */
export async function joinReservationWaitlist(form: FormData): Promise<ActionResult<{ email: string }>> {
  const blocked = await guardPublic('waitlist', form, 4, 3600);
  if (blocked) return blocked;
  const parsed = waitlistSchema.safeParse(stringFields(form));
  if (!parsed.success) return fail('validation', zodFieldErrors(parsed.error));
  const d = parsed.data;
  const branch = await openBranch(d.branch);
  if (!branch) return fail('disabled');
  if (!inWindow(branch, d.date, new Date())) return fail('window');
  const phone = normalizePhone(d.phone, restaurantConfig.country);
  if (!phone) return fail('validation', { phone: 'phone' });
  await joinWaitlist({
    branch,
    date: d.date,
    preferredTime: d.preferredTime,
    flexibleMinutes: Number(d.flexible),
    partySize: d.party,
    name: d.name,
    email: d.email.toLowerCase(),
    phone,
    notes: d.notes || null,
    locale: d.locale,
  });
  return ok({ email: d.email });
}

const manageSchema = z.object({ code: z.string().min(4).max(16), token: z.string().min(16).max(64) });
const moveSchema = manageSchema.extend({ date: dateSchema, time: timeSchema, party: partySchema, area: z.enum(AREA_CHOICES) });

/** Moves a booking from the manage page (within the policy window). */
export async function moveReservation(input: z.input<typeof moveSchema>): Promise<ActionResult<{ date: string; time: string }>> {
  const rl = await limitByIp('reserve-manage', 20, 600);
  if (!rl.ok) return fail('rateLimited', undefined, rl.retryAfterSeconds);
  const parsed = moveSchema.safeParse({ ...input, party: normalizeDigits(String(input.party)) });
  if (!parsed.success) return fail('validation');
  const d = parsed.data;
  const r = await reservationByCode(d.code, d.token);
  if (!r) return fail('notFound');
  const now = new Date();
  if (!changeable(r, now)) return fail('window');
  const range = partyRangeForChange(r);
  if (d.party < range.min || d.party > range.max) return fail('validation');
  const branch = await getBranch(r.branchId);
  if (!branch || !inWindow(branch, d.date, now)) return fail('window');
  if (d.date === r.date && d.time === r.time && d.party === r.partySize && d.area === r.area) return fail('unchanged');
  const moved = await moveByGuest(r, { date: d.date, time: d.time, partySize: d.party, area: d.area, now });
  if (!moved) return fail('unavailable');
  return ok({ date: moved.date, time: moved.time });
}

/** Cancels a booking from the manage page; a deposit is refunded when outside the cut-off. */
export async function cancelReservation(input: z.input<typeof manageSchema>): Promise<ActionResult<{ refunded: boolean }>> {
  const rl = await limitByIp('reserve-manage', 20, 600);
  if (!rl.ok) return fail('rateLimited', undefined, rl.retryAfterSeconds);
  const parsed = manageSchema.safeParse(input);
  if (!parsed.success) return fail('validation');
  const r = await reservationByCode(parsed.data.code, parsed.data.token);
  if (!r) return fail('notFound');
  const now = new Date();
  if (!changeable(r, now)) return fail('window');
  return ok(await cancelByGuest(r, now));
}
