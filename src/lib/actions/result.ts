import type { z } from 'zod';

/**
 * Uniform result for server actions. `error` is a message key in the `forms.errors` namespace, so the client
 * shows it in the visitor's language; `fieldErrors` maps field names to message keys.
 */
export type ActionResult<T = undefined> =
  | { ok: true; data: T }
  | { ok: false; error: string; fieldErrors?: Record<string, string>; retryAfterSeconds?: number };

export function ok<T>(data: T): ActionResult<T> {
  return { ok: true, data };
}

export function fail(error: string, fieldErrors?: Record<string, string>, retryAfterSeconds?: number): { ok: false; error: string; fieldErrors?: Record<string, string>; retryAfterSeconds?: number } {
  return { ok: false, error, fieldErrors, retryAfterSeconds };
}

/** Maps a Zod error to field → message key (the first issue per field; messages are keys, not prose). */
export function zodFieldErrors(error: z.ZodError): Record<string, string> {
  const out: Record<string, string> = {};
  for (const issue of error.issues) {
    const key = issue.path.join('.') || '_';
    if (!out[key]) out[key] = issue.message && /^[a-zA-Z][\w.]*$/.test(issue.message) ? issue.message : 'invalid';
  }
  return out;
}
