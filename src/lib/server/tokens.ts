import 'server-only';
import { createHmac, timingSafeEqual } from 'node:crypto';

export type LinkPurpose = 'reservation' | 'order' | 'ticket' | 'waitlist';

function secret(): string {
  const value = process.env.BETTER_AUTH_SECRET;
  if (value) return value;
  if (process.env.NODE_ENV === 'production') throw new Error('BETTER_AUTH_SECRET must be set in production.');
  return 'zill-local-development-secret';
}

/**
 * The secret in a guest's link (manage a booking, track an order, open a ticket, accept a waitlist offer).
 * Derived with HMAC from the server secret, so every email — confirmation, reminder, update — carries the
 * same working link without the token ever being stored.
 */
export function linkToken(purpose: LinkPurpose, id: string): string {
  return createHmac('sha256', secret()).update(`${purpose}:${id}`).digest('base64url').slice(0, 32);
}

export function verifyLinkToken(purpose: LinkPurpose, id: string, token: string | null | undefined): boolean {
  if (!token || token.length !== 32) return false;
  const expected = Buffer.from(linkToken(purpose, id));
  const given = Buffer.from(token);
  return given.length === expected.length && timingSafeEqual(given, expected);
}
