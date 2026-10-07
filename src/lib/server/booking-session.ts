import 'server-only';
import { cookies } from 'next/headers';
import { createToken } from '@/lib/utils/id';

/**
 * The guest's booking session: holds belong to it. httpOnly, so scripts can neither read nor forge it.
 * It is issued by the slots endpoint (a route handler) before the first hold — setting a cookie from a server
 * action would make the router re-render the page mid-flow.
 */
const SESSION_COOKIE = 'zill_booking';
const VALID = /^[A-Za-z0-9_-]{20,64}$/;

export async function readBookingSession(): Promise<string | null> {
  const value = (await cookies()).get(SESSION_COOKIE)?.value;
  return value && VALID.test(value) ? value : null;
}

/** Returns the session, issuing one when missing (route handlers; server actions only as a fallback). */
export async function ensureBookingSession(): Promise<string> {
  const existing = await readBookingSession();
  if (existing) return existing;
  const key = createToken(18);
  (await cookies()).set(SESSION_COOKIE, key, { path: '/', httpOnly: true, sameSite: 'lax', secure: process.env.NODE_ENV === 'production', maxAge: 60 * 60 * 3 });
  return key;
}
