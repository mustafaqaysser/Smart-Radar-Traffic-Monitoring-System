import type { Metadata } from 'next';
import { routing } from '@/i18n/routing';
import { absoluteUrl } from './url';

interface PageMetaInput {
  locale: string;
  /** Path without the locale prefix, starting with '/' (or '' for home). */
  path: string;
  title: string | null;
  description?: string | null;
  /** Pre-rendered OG image key in /public/og (`<key>-<locale>.png`), or an absolute image URL. */
  image?: string;
  noindex?: boolean;
}

/** Canonical URL, hreflang alternates and Open Graph for a page in both languages. */
export function pageMetadata({ locale, path, title, description, image, noindex }: PageMetaInput): Metadata {
  const url = absoluteUrl(`/${locale}${path}`);
  const ogImage = image ? (image.startsWith('http') || image.startsWith('/') ? image : `/og/${image}-${locale}.png`) : `/og/home-${locale}.png`;
  return {
    ...(title ? { title } : {}),
    ...(description ? { description } : {}),
    alternates: {
      canonical: url,
      languages: { ...Object.fromEntries(routing.locales.map((l) => [l, absoluteUrl(`/${l}${path}`)])), 'x-default': absoluteUrl(`/${routing.defaultLocale}${path}`) },
    },
    openGraph: {
      url,
      ...(title ? { title } : {}),
      ...(description ? { description } : {}),
      images: [{ url: ogImage, width: 1200, height: 630 }],
    },
    ...(noindex ? { robots: { index: false, follow: false } } : {}),
  };
}
