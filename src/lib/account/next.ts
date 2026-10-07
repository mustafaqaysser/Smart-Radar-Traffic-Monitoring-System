import { routing } from '@/i18n/routing';

/**
 * Where to go after signing in: only paths on this site (never another origin, never the API), without the
 * locale prefix so the router adds the visitor's own.
 */
export function safeNext(value: string | string[] | null | undefined, fallback = '/account'): string {
  const raw = Array.isArray(value) ? value[0] : value;
  if (!raw || typeof raw !== 'string' || raw.length > 300) return fallback;
  if (!raw.startsWith('/') || raw.startsWith('//') || raw.includes('\\') || /[\u0000-\u001f]/.test(raw)) return fallback;
  let path = raw;
  for (const l of routing.locales) {
    if (path === `/${l}`) path = '/';
    else if (path.startsWith(`/${l}/`)) path = path.slice(l.length + 1);
  }
  if (path.startsWith('/api') || path.startsWith('/admin') || path.startsWith('/account/sign-') || path.startsWith('/account/reset')) return fallback;
  return path;
}

/** Maps a Better Auth error to a key in `account.auth.errors`. */
export function authErrorKey(error: { code?: string | null; status?: number | null } | null | undefined): string {
  const code = error?.code ?? '';
  if (error?.status === 429) return 'rateLimited';
  if (code === 'INVALID_EMAIL_OR_PASSWORD' || code === 'CREDENTIAL_ACCOUNT_NOT_FOUND' || code === 'USER_NOT_FOUND') return 'invalidCredentials';
  if (code.startsWith('USER_ALREADY_EXISTS')) return 'userExists';
  if (code === 'INVALID_OTP') return 'invalidOtp';
  if (code === 'OTP_EXPIRED') return 'otpExpired';
  if (code === 'TOO_MANY_ATTEMPTS') return 'tooManyAttempts';
  if (code === 'INVALID_TOKEN') return 'invalidToken';
  if (code === 'INVALID_PASSWORD') return 'invalidPassword';
  if (code === 'PASSWORD_TOO_SHORT') return 'passwordShort';
  return 'unknown';
}
