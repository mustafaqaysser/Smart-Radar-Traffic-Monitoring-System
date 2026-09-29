import 'server-only';
import { eq, sql } from 'drizzle-orm';
import { headers } from 'next/headers';
import { db } from '@/lib/db/client';
import { rateLimits } from '@/lib/db/schema';

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

/** Caller IP from proxy headers (first hop), for rate-limit keys only — never stored. */
export async function clientIp(): Promise<string> {
  const h = await headers();
  const forwarded = h.get('x-forwarded-for')?.split(',')[0]?.trim();
  return forwarded || h.get('x-real-ip') || 'local';
}

export async function limitByIp(action: string, limit: number, windowSeconds: number): Promise<RateLimitResult> {
  return rateLimit(`${action}:${await clientIp()}`, limit, windowSeconds);
}
