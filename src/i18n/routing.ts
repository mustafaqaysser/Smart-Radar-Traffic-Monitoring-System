import { defineRouting } from 'next-intl/routing';
import restaurantConfig from '@config';

export const routing = defineRouting({
  locales: restaurantConfig.locales,
  defaultLocale: restaurantConfig.defaultLocale,
  localePrefix: 'always',
  // Arabic is the default: "/" always resolves to /ar; the language switch keeps visitors on the same page.
  localeDetection: false,
  localeCookie: false,
  alternateLinks: true,
});

export type AppLocale = (typeof routing.locales)[number];
