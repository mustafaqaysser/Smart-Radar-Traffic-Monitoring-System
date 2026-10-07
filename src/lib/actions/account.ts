'use server';

import { and, eq, ne } from 'drizzle-orm';
import { headers } from 'next/headers';
import { z } from 'zod';
import { routing } from '@/i18n/routing';
import { auth } from '@/lib/auth/auth';
import { getCurrentUser, type CurrentUser } from '@/lib/auth/session';
import { db } from '@/lib/db/client';
import { accounts, addresses, newsletterSubscribers, users } from '@/lib/db/schema';
import { ALLERGENS, DIETARY_TAGS } from '@/lib/menu/tags';
import { deleteAccount, deletionCheck } from '@/lib/server/account';
import { requestSubscription } from '@/lib/server/newsletter';
import { normalizePhone } from '@/lib/services/phone';
import { limitByIp } from '@/lib/services/rate-limit';
import { createId } from '@/lib/utils/id';
import { fail, ok, zodFieldErrors, type ActionResult } from './result';

type Guarded = { user: CurrentUser } | { failure: ReturnType<typeof fail> };

/** Every account action: a signed-in guest, and a generous per-IP limit against scripted abuse. */
async function member(action: string): Promise<Guarded> {
  const user = await getCurrentUser();
  if (!user) return { failure: fail('unauthorized') };
  const rl = await limitByIp(`account:${action}`, 40, 600);
  if (!rl.ok) return { failure: fail('rateLimited', undefined, rl.retryAfterSeconds) };
  return { user };
}

// ————————————————————————————————————————— profile —————————————————————————————————————————

const profileSchema = z.object({
  name: z.string().trim().min(2, 'tooShort').max(80, 'tooLong'),
  phone: z.string().trim().max(32, 'phone').optional().or(z.literal('')),
  locale: z.enum(routing.locales),
});

export async function updateProfile(input: z.input<typeof profileSchema>): Promise<ActionResult<{ name: string; phone: string | null; locale: string }>> {
  const g = await member('profile');
  if ('failure' in g) return g.failure;
  const parsed = profileSchema.safeParse(input);
  if (!parsed.success) return fail('validation', zodFieldErrors(parsed.error));
  const d = parsed.data;
  let phone: string | null = null;
  if (d.phone) {
    phone = normalizePhone(d.phone);
    if (!phone) return fail('validation', { phone: 'phone' });
  }
  await db.update(users).set({ name: d.name, phone, locale: d.locale, updatedAt: new Date() }).where(eq(users.id, g.user.id));
  return ok({ name: d.name, phone, locale: d.locale });
}

/** For accounts that sign in with an email code only: adds a password. */
export async function addPassword(input: { newPassword: string }): Promise<ActionResult> {
  const g = await member('password');
  if ('failure' in g) return g.failure;
  const parsed = z.object({ newPassword: z.string().min(8, 'passwordShort').max(128, 'tooLong') }).safeParse(input);
  if (!parsed.success) return fail('validation', zodFieldErrors(parsed.error));
  const existing = await db.query.accounts.findFirst({ where: and(eq(accounts.userId, g.user.id), eq(accounts.providerId, 'credential')) });
  if (existing) return fail('passwordAlreadySet');
  await auth.api.setPassword({ body: { newPassword: parsed.data.newPassword }, headers: await headers() });
  return ok(undefined);
}

// ————————————————————————————————————————— dietary profile —————————————————————————————————————————

const dietarySchema = z.object({
  diets: z.array(z.enum(DIETARY_TAGS)).max(DIETARY_TAGS.length),
  avoid: z.array(z.enum(ALLERGENS)).max(ALLERGENS.length),
  maxSpice: z.number().int().min(0).max(3).nullable(),
});

export async function saveDietaryProfile(input: z.input<typeof dietarySchema>): Promise<ActionResult<{ empty: boolean }>> {
  const g = await member('dietary');
  if ('failure' in g) return g.failure;
  const parsed = dietarySchema.safeParse(input);
  if (!parsed.success) return fail('validation', zodFieldErrors(parsed.error));
  const { diets, avoid, maxSpice } = parsed.data;
  const empty = !diets.length && !avoid.length && maxSpice === null;
  await db
    .update(users)
    .set({ dietary: empty ? null : { diets: [...new Set(diets)], avoidAllergens: [...new Set(avoid)], ...(maxSpice !== null ? { maxSpice } : {}) }, updatedAt: new Date() })
    .where(eq(users.id, g.user.id));
  return ok({ empty });
}

// ————————————————————————————————————————— addresses —————————————————————————————————————————

const addressSchema = z.object({
  id: z.string().max(40).optional(),
  label: z.string().trim().min(1, 'required').max(40, 'tooLong'),
  area: z.string().trim().min(2, 'required').max(80, 'tooLong'),
  street: z.string().trim().min(2, 'required').max(160, 'tooLong'),
  building: z.string().trim().max(40, 'tooLong').optional().or(z.literal('')),
  floor: z.string().trim().max(40, 'tooLong').optional().or(z.literal('')),
  notes: z.string().trim().max(240, 'tooLong').optional().or(z.literal('')),
  isDefault: z.boolean().optional(),
});

export async function saveAddress(input: z.input<typeof addressSchema>): Promise<ActionResult<{ id: string }>> {
  const g = await member('address');
  if ('failure' in g) return g.failure;
  const parsed = addressSchema.safeParse(input);
  if (!parsed.success) return fail('validation', zodFieldErrors(parsed.error));
  const { id, isDefault, ...d } = parsed.data;
  const values = { label: d.label, area: d.area, street: d.street, building: d.building || null, floor: d.floor || null, notes: d.notes || null };
  const owned = await db.select({ id: addresses.id }).from(addresses).where(eq(addresses.userId, g.user.id));
  if (id && !owned.some((a) => a.id === id)) return fail('notFound');
  if (!id && owned.length >= 12) return fail('addressLimit');
  const addressId = id ?? createId();
  await db.transaction(async (tx) => {
    if (id) await tx.update(addresses).set(values).where(and(eq(addresses.id, id), eq(addresses.userId, g.user.id)));
    else await tx.insert(addresses).values({ id: addressId, userId: g.user.id, ...values, isDefault: owned.length === 0 });
    if (isDefault) {
      await tx.update(addresses).set({ isDefault: false }).where(and(eq(addresses.userId, g.user.id), ne(addresses.id, addressId)));
      await tx.update(addresses).set({ isDefault: true }).where(and(eq(addresses.userId, g.user.id), eq(addresses.id, addressId)));
    }
  });
  return ok({ id: addressId });
}

export async function deleteAddress(id: string): Promise<ActionResult> {
  const g = await member('address');
  if ('failure' in g) return g.failure;
  const parsed = z.string().min(1).max(40).safeParse(id);
  if (!parsed.success) return fail('invalid');
  const removed = await db.delete(addresses).where(and(eq(addresses.id, parsed.data), eq(addresses.userId, g.user.id))).returning({ isDefault: addresses.isDefault });
  if (!removed[0]) return fail('notFound');
  if (removed[0].isDefault) {
    // Another address (the newest) becomes the default.
    const next = await db.query.addresses.findFirst({ where: eq(addresses.userId, g.user.id), orderBy: (a, { desc }) => [desc(a.createdAt)] });
    if (next) await db.update(addresses).set({ isDefault: true }).where(eq(addresses.id, next.id));
  }
  return ok(undefined);
}

// ————————————————————————————————————————— communication —————————————————————————————————————————

const preferencesSchema = z.object({ marketingEmail: z.boolean(), marketingSms: z.boolean() });

/** The seasonal letter follows the email preference (double opt-in unless the address is already confirmed). */
export async function savePreferences(input: z.input<typeof preferencesSchema>): Promise<ActionResult<{ pendingConfirmation: boolean }>> {
  const g = await member('preferences');
  if ('failure' in g) return g.failure;
  const parsed = preferencesSchema.safeParse(input);
  if (!parsed.success) return fail('validation');
  const { marketingEmail, marketingSms } = parsed.data;
  const user = g.user;
  await db.update(users).set({ marketingEmail, marketingSms, updatedAt: new Date() }).where(eq(users.id, user.id));
  const subscriber = await db.query.newsletterSubscribers.findFirst({ where: eq(newsletterSubscribers.email, user.email) });
  let pendingConfirmation = false;
  if (marketingEmail && subscriber?.status !== 'confirmed') {
    if (user.emailVerified) {
      const now = new Date();
      if (subscriber) await db.update(newsletterSubscribers).set({ status: 'confirmed', confirmedAt: now, unsubscribedAt: null, locale: user.locale }).where(eq(newsletterSubscribers.id, subscriber.id));
      else await db.insert(newsletterSubscribers).values({ id: createId(), email: user.email, locale: user.locale, status: 'confirmed', tokenHash: createId(), source: 'account', confirmedAt: now });
    } else {
      await requestSubscription(user.email, user.locale, 'account');
      pendingConfirmation = true;
    }
  } else if (!marketingEmail && subscriber && subscriber.status !== 'unsubscribed') {
    await db.update(newsletterSubscribers).set({ status: 'unsubscribed', unsubscribedAt: new Date() }).where(eq(newsletterSubscribers.id, subscriber.id));
  }
  return ok({ pendingConfirmation });
}

// ————————————————————————————————————————— deletion —————————————————————————————————————————

/** Deletes the account after the guest types their email; bookings that must finish first block it. */
export async function deleteMyAccount(input: { confirmEmail: string }): Promise<ActionResult> {
  const g = await member('delete');
  if ('failure' in g) return g.failure;
  const parsed = z.object({ confirmEmail: z.string().trim().max(254) }).safeParse(input);
  if (!parsed.success || parsed.data.confirmEmail.toLowerCase() !== g.user.email.toLowerCase()) return fail('validation', { confirmEmail: 'emailMismatch' });
  const now = new Date();
  const check = await deletionCheck(g.user, now);
  if (check.blockers.length) return fail('deletionBlocked');
  const requestHeaders = await headers();
  await auth.api.signOut({ headers: requestHeaders });
  const result = await deleteAccount(g.user, now);
  if (!result.ok) return fail('deletionBlocked');
  return ok(undefined);
}
