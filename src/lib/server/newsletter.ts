import 'server-only';
import { eq } from 'drizzle-orm';
import { db } from '@/lib/db/client';
import { newsletterSubscribers } from '@/lib/db/schema';
import { routing } from '@/i18n/routing';
import { sha256 } from '@/lib/utils/id';

export function localeFrom(value: string | null): string {
  return value && (routing.locales as readonly string[]).includes(value) ? value : routing.defaultLocale;
}

/** Finds a subscriber by the secret token from an email link (only its hash is stored). */
export async function subscriberByToken(token: string | null) {
  if (!token || token.length < 16 || token.length > 128) return null;
  const tokenHash = await sha256(token);
  return (await db.query.newsletterSubscribers.findFirst({ where: eq(newsletterSubscribers.tokenHash, tokenHash) })) ?? null;
}

export async function confirmSubscriber(id: string) {
  await db.update(newsletterSubscribers).set({ status: 'confirmed', confirmedAt: new Date(), unsubscribedAt: null }).where(eq(newsletterSubscribers.id, id));
}

export async function unsubscribe(id: string) {
  await db.update(newsletterSubscribers).set({ status: 'unsubscribed', unsubscribedAt: new Date() }).where(eq(newsletterSubscribers.id, id));
}
