import 'server-only';
import { eq } from 'drizzle-orm';
import { db } from '@/lib/db/client';
import { newsletterSubscribers } from '@/lib/db/schema';
import { routing } from '@/i18n/routing';
import { mail } from '@/lib/server/mail';
import { absoluteUrl } from '@/lib/site/url';
import { createId, createToken, sha256 } from '@/lib/utils/id';

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

/**
 * Double opt-in: stores a pending subscriber and emails a confirmation link. An address that is already
 * confirmed is left alone, and callers answer the same way either way so nobody learns who is subscribed.
 */
export async function requestSubscription(email: string, locale: string, source: string): Promise<void> {
  const address = email.trim().toLowerCase();
  const existing = await db.query.newsletterSubscribers.findFirst({ where: eq(newsletterSubscribers.email, address) });
  if (existing?.status === 'confirmed') return;
  const token = createToken();
  const tokenHash = await sha256(token);
  if (existing) {
    await db.update(newsletterSubscribers).set({ tokenHash, locale, status: 'pending', source: existing.source ?? source }).where(eq(newsletterSubscribers.id, existing.id));
  } else {
    await db.insert(newsletterSubscribers).values({ id: createId(), email: address, locale, status: 'pending', tokenHash, source });
  }
  await mail(address, { name: 'newsletter-confirm', props: { locale, confirmUrl: absoluteUrl(`/api/newsletter/confirm?token=${encodeURIComponent(token)}&locale=${locale}`) } });
}
