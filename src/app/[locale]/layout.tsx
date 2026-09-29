import type { CSSProperties, ReactNode } from 'react';
import { notFound } from 'next/navigation';
import { headers } from 'next/headers';
import { hasLocale, NextIntlClientProvider } from 'next-intl';
import { setRequestLocale } from 'next-intl/server';
import { routing } from '@/i18n/routing';
import { siteFontVariables } from '@/lib/fonts';
import { phaseStylesheet } from '@/lib/brand/palette';
import { atmosphereCssVars, computeAtmosphere } from '@/lib/brand/atmosphere';
import '@/styles/site.css';

export function generateStaticParams() {
  return routing.locales.map((locale) => ({ locale }));
}

export default async function LocaleLayout({ children, params }: { children: ReactNode; params: Promise<{ locale: string }> }) {
  const { locale } = await params;
  if (!hasLocale(routing.locales, locale)) notFound();
  setRequestLocale(locale);
  const nonce = (await headers()).get('x-nonce') ?? undefined;
  const atmosphere = computeAtmosphere(new Date(), { lat: 21.4858, lng: 39.1925, timeZone: 'Asia/Riyadh' });

  return (
    <html
      lang={locale}
      dir={locale === 'ar' ? 'rtl' : 'ltr'}
      data-phase={atmosphere.phase}
      className={siteFontVariables}
      style={atmosphereCssVars(atmosphere) as CSSProperties}
      suppressHydrationWarning
    >
      <head>
        <style nonce={nonce} dangerouslySetInnerHTML={{ __html: phaseStylesheet() }} />
      </head>
      <body>
        <NextIntlClientProvider>{children}</NextIntlClientProvider>
      </body>
    </html>
  );
}
