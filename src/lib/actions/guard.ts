import 'server-only';
import { passesBotChecks, HONEYPOT_FIELD, RENDERED_AT_FIELD } from '@/lib/services/bot-protection';
import { limitByIp } from '@/lib/services/rate-limit';
import { fail } from './result';

/**
 * Shared protection for public mutations: rate limit by IP, honeypot, minimum fill time and (when configured)
 * Turnstile. Returns a failure result to return as-is, or null when the request may proceed.
 */
export async function guardPublic(action: string, form: FormData | Record<string, unknown>, limit = 8, windowSeconds = 600) {
  const rl = await limitByIp(action, limit, windowSeconds);
  if (!rl.ok) return fail('rateLimited', undefined, rl.retryAfterSeconds);
  const get = (k: string) => (form instanceof FormData ? form.get(k) : form[k]);
  const ok = await passesBotChecks({
    honeypot: (get(HONEYPOT_FIELD) as string | null | undefined) ?? null,
    renderedAt: (get(RENDERED_AT_FIELD) as string | number | null | undefined) ?? null,
    turnstileToken: (get('cf-turnstile-response') as string | null | undefined) ?? null,
  });
  if (!ok) return fail('botCheck');
  return null;
}
