import 'server-only';
import { eq, lt, sql } from 'drizzle-orm';
import { headers } from 'next/headers';
import { db } from '@/lib/db/client';
import { rateLimits } from '@/lib/db/schema';
import { sha256 } from '@/lib/utils/id';

export interface RateLimitResult {
  ok: boolean;
  remaining: number;
  retryAfterSeconds: number;
}

/**
 * Fixed-window rate limiter stored in the database, so it holds across server instances and restarts.
 * `key` should combine the action and the caller (e.g. `reserve:ip`).
 */
export async function rateLimit(key: string, limit: number, windowSeconds: number): Promise<RateLimitResult> {
  const now = Date.now();
  // Expired windows are useless; clear them now and then (the daily cron also purges them).
  if (Math.random() < 0.02) await purgeExpiredRateLimits(now);
  const resetAt = now + windowSeconds * 1000;
  const row = await db.query.rateLimits.findFirst({ where: eq(rateLimits.key, key) });
  if (!row || row.resetAt <= now) {
    await db
      .insert(rateLimits)
      .values({ key, count: 1, resetAt })
      .onConflictDoUpdate({ target: rateLimits.key, set: { count: 1, resetAt } });
    return { ok: true, remaining: limit - 1, retryAfterSeconds: 0 };
  }
  if (row.count >= limit) return { ok: false, remaining: 0, retryAfterSeconds: Math.ceil((row.resetAt - now) / 1000) };
  await db.update(rateLimits).set({ count: sql`${rateLimits.count} + 1` }).where(eq(rateLimits.key, key));
  return { ok: true, remaining: limit - row.count - 1, retryAfterSeconds: 0 };
}

/** Deletes rate-limit windows that ended more than a day ago. */
export async function purgeExpiredRateLimits(now = Date.now()): Promise<void> {
  await db.delete(rateLimits).where(lt(rateLimits.resetAt, now - 864e5));
}

/** Caller IP from proxy headers (first hop). Used raw only for staff audit entries and bot verification. */
export async function clientIp(): Promise<string> {
  const h = await headers();
  const forwarded = h.get('x-forwarded-for')?.split(',')[0]?.trim();
  return forwarded || h.get('x-real-ip') || 'local';
}

/** A salted hash of the caller's IP: rate-limit keys never contain the address itself. */
export async function ipFingerprint(): Promise<string> {
  const salt = process.env.ANALYTICS_SALT || process.env.BETTER_AUTH_SECRET || 'zill';
  return (await sha256(`${salt}:${await clientIp()}`)).slice(0, 32);
}

export async function limitByIp(action: string, limit: number, windowSeconds: number): Promise<RateLimitResult> {
  return rateLimit(`${action}:${await ipFingerprint()}`, limit, windowSeconds);
}
