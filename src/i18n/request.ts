import { hasLocale } from 'next-intl';
import { getRequestConfig } from 'next-intl/server';
import { cookies } from 'next/headers';
import restaurantConfig from '@config';
import { routing } from './routing';

/** Cookie that stores a staff member's admin language (the admin is not locale-prefixed). */
export const ADMIN_LOCALE_COOKIE = 'zill_admin_locale';

export default getRequestConfig(async ({ requestLocale }) => {
  let locale = await requestLocale;
  if (!locale) {
    const store = await cookies();
    locale = store.get(ADMIN_LOCALE_COOKIE)?.value;
  }
  if (!hasLocale(routing.locales, locale)) locale = routing.defaultLocale;

  return {
    locale,
    messages: (await import(`../messages/${locale}.json`)).default,
    timeZone: restaurantConfig.defaultTimeZone,
  };
});
