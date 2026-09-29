import 'server-only';
import { clientIp } from './rate-limit';

/** Name of the honeypot field every public form renders (visually hidden, never filled by humans). */
export const HONEYPOT_FIELD = 'company_website';
/** Hidden field holding the time the form was rendered (ms since epoch). */
export const RENDERED_AT_FIELD = 'rendered_at';

export function turnstileEnabled(): boolean {
  return Boolean(process.env.TURNSTILE_SECRET_KEY && process.env.NEXT_PUBLIC_TURNSTILE_SITE_KEY);
}

/**
 * Honeypot + minimum fill time always; Cloudflare Turnstile when configured.
 * Returns false for likely bots. Humans never see a difference.
 */
export async function passesBotChecks(input: { honeypot?: string | null; renderedAt?: string | number | null; turnstileToken?: string | null }): Promise<boolean> {
  if (input.honeypot) return false;
  const rendered = Number(input.renderedAt);
  if (Number.isFinite(rendered) && rendered > 0 && Date.now() - rendered < 1500) return false;
  if (!turnstileEnabled()) return true;
  if (!input.turnstileToken) return false;
  const body = new URLSearchParams({ secret: process.env.TURNSTILE_SECRET_KEY as string, response: input.turnstileToken, remoteip: await clientIp() });
  try {
    const res = await fetch('https://challenges.cloudflare.com/turnstile/v0/siteverify', { method: 'POST', body });
    const json = (await res.json()) as { success?: boolean };
    return json.success === true;
  } catch {
    return false;
  }
}
