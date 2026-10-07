import restaurantConfig from '@config';
import { tr } from '@/lib/i18n/localized';
import type { BranchDTO } from '@/lib/queries/types';
import { absoluteUrl } from '@/lib/site/url';

export type Json = string | number | boolean | null | Json[] | JsonObject;
export type JsonObject = { [key: string]: Json | undefined };

/** Structured data. `<` is escaped so content can never close the script element. */
export function JsonLd({ data }: { data: Json }) {
  return <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(data).replace(/</g, '\\u003c') }} />;
}

const DAYS = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];

export function branchJsonLd(b: BranchDTO, locale: string): JsonObject {
  return {
    '@type': 'Restaurant',
    '@id': absoluteUrl(`/${locale}/locations/${b.slug}#restaurant`),
    name: `${tr(restaurantConfig.name, locale)} — ${tr(b.shortName, locale)}`,
    url: absoluteUrl(`/${locale}/locations/${b.slug}`),
    telephone: b.phone,
    email: b.email,
    image: b.hero ? absoluteUrl(b.hero.src) : undefined,
    servesCuisine: tr(restaurantConfig.cuisine, locale),
    priceRange: restaurantConfig.priceRange,
    acceptsReservations: b.reservationsEnabled ? absoluteUrl(`/${locale}/reserve?branch=${b.slug}`) : false,
    hasMenu: absoluteUrl(`/${locale}/menu`),
    currenciesAccepted: restaurantConfig.currency,
    address: {
      '@type': 'PostalAddress',
      streetAddress: tr(b.address, locale),
      addressLocality: tr(b.city, locale),
      addressCountry: restaurantConfig.country,
    },
    geo: { '@type': 'GeoCoordinates', latitude: b.lat, longitude: b.lng },
    openingHoursSpecification: b.venueHours.map((h) => ({ '@type': 'OpeningHoursSpecification', dayOfWeek: DAYS[h.weekday], opens: h.opens, closes: h.closes })),
  };
}

export function restaurantJsonLd(branches: BranchDTO[], locale: string): Json {
  return {
    '@context': 'https://schema.org',
    '@graph': [
      {
        '@type': 'Organization',
        '@id': absoluteUrl('/#organization'),
        name: tr(restaurantConfig.name, locale),
        legalName: tr(restaurantConfig.legalName, locale),
        url: absoluteUrl(`/${locale}`),
        logo: absoluteUrl('/brand/zill-monogram.svg'),
        slogan: tr(restaurantConfig.tagline, locale),
      },
      ...branches.map((b) => branchJsonLd(b, locale)),
    ],
  };
}
