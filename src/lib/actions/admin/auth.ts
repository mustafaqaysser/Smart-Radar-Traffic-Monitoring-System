'use server';

import { cookies } from 'next/headers';
import { routing } from '@/i18n/routing';
import { ADMIN_LOCALE_COOKIE } from '@/i18n/request';

/** The sign-in screen's language (before anyone is signed in, so no profile to save it to). */
export async function setSignInLocale(locale: string): Promise<void> {
  if (!(routing.locales as readonly string[]).includes(locale)) return;
  (await cookies()).set(ADMIN_LOCALE_COOKIE, locale, { path: '/', maxAge: 60 * 60 * 24 * 365, sameSite: 'lax', httpOnly: true, secure: process.env.NODE_ENV === 'production' });
}
