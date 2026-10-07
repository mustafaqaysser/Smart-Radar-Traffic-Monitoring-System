import type { CSSProperties, ReactNode } from 'react';
import type { Metadata, Viewport } from 'next';
import { notFound } from 'next/navigation';
import { headers } from 'next/headers';
import { hasLocale, NextIntlClientProvider } from 'next-intl';
import { getMessages, getTranslations, setRequestLocale } from 'next-intl/server';
import restaurantConfig from '@config';
import { routing } from '@/i18n/routing';
import { siteFontVariables } from '@/lib/fonts';
import { phases, phaseStylesheet } from '@/lib/brand/palette';
import { atmosphereCssVars, computeAtmosphere } from '@/lib/brand/atmosphere';
import { getCurrentUser } from '@/lib/auth/session';
import { isStaff } from '@/lib/auth/permissions';
import { tr } from '@/lib/i18n/localized';
import { getBranches } from '@/lib/queries/branches';
import { getSettings } from '@/lib/server/settings';
import { getSelectedBranch } from '@/lib/site/selection';
import { getServingContext } from '@/lib/site/serving';
import { absoluteUrl, siteUrl } from '@/lib/site/url';
import { MotionProvider } from '@/components/motion/motion-provider';
import { AtmosphereProvider } from '@/components/site/atmosphere-provider';
import { GnomonPreloader } from '@/components/site/gnomon-preloader';
import { HeadScript } from '@/components/site/head-script';
import { MaintenanceView } from '@/components/site/maintenance-view';
import { Toaster } from '@/components/site/ui/toast';
import '@/styles/site.css';

const SERVER_ONLY_NAMESPACES = new Set(['legal', 'admin']);

export function generateStaticParams() {
  return routing.locales.map((locale) => ({ locale }));
}

export async function generateMetadata({ params }: { params: Promise<{ locale: string }> }): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: 'common.meta' });
  const settings = await getSettings();
  const title = tr(settings.seo.title, locale) || t('tagline');
  return {
    metadataBase: new URL(siteUrl()),
    title: { default: `${t('siteName')} · ${title}`, template: `%s · ${t('siteName')}` },
    description: tr(settings.seo.description, locale) || t('description'),
    applicationName: t('siteName'),
    manifest: '/manifest.webmanifest',
    alternates: {
      canonical: absoluteUrl(`/${locale}`),
      languages: Object.fromEntries(routing.locales.map((l) => [l, absoluteUrl(`/${l}`)])),
    },
    openGraph: {
      type: 'website',
      siteName: t('siteName'),
      locale: locale === 'ar' ? 'ar_SA' : 'en_GB',
      alternateLocale: routing.locales.filter((l) => l !== locale).map((l) => (l === 'ar' ? 'ar_SA' : 'en_GB')),
      images: [{ url: `/og/home-${locale}.png`, width: 1200, height: 630 }],
    },
    twitter: { card: 'summary_large_image' },
    formatDetection: { telephone: false, address: false, email: false },
  };
}

export async function generateViewport(): Promise<Viewport> {
  return {
    width: 'device-width',
    initialScale: 1,
    viewportFit: 'cover',
    themeColor: [
      { media: '(prefers-color-scheme: light)', color: phases.morning.bg },
      { media: '(prefers-color-scheme: dark)', color: phases.night.bg },
    ],
  };
}

export default async function LocaleLayout({ children, params }: { children: ReactNode; params: Promise<{ locale: string }> }) {
  const { locale } = await params;
  if (!hasLocale(routing.locales, locale)) notFound();
  setRequestLocale(locale);

  const [nonce, settings, branch, branches, user, t, messages] = await Promise.all([
    headers().then((h) => h.get('x-nonce') ?? undefined),
    getSettings(),
    getSelectedBranch(),
    getBranches(),
    getCurrentUser(),
    getTranslations('common'),
    getMessages(),
  ]);
  // Client components only receive the namespaces they use (long-form legal copy and the admin stay on the server).
  const clientMessages = Object.fromEntries(Object.entries(messages).filter(([ns]) => !SERVER_ONLY_NAMESPACES.has(ns)));
  // Children of the (site) group render the header, footer and seasonal notice; bare routes (print, table) do not.
  const serving = await getServingContext(branch);
  const location = branch ?? { lat: 21.4854, lng: 39.1869, timeZone: restaurantConfig.defaultTimeZone };
  const atmosphere = computeAtmosphere(serving.now, { lat: location.lat, lng: location.lng, timeZone: location.timeZone });
  const season = settings.features.seasonalModes ? serving.seasons.find((s) => s.theme && s.theme !== 'none') : undefined;
  const maintenance = settings.maintenance.enabled && !isStaff(user?.role);

  // Preloader: the shadow's final angle on the dial (screen degrees, 0 = east, clockwise) and its length.
  const daylight = atmosphere.sun.altitude > 0;
  const shadowAngle = daylight ? atmosphere.sun.azimuth + 90 : 90;
  const shadowLength = daylight ? Math.min(1.6, Math.max(0.3, 1 / Math.tan((Math.max(atmosphere.sun.altitude, 4) * Math.PI) / 180))) : 0.35;

  return (
    <html
      lang={locale}
      dir={locale === 'ar' ? 'rtl' : 'ltr'}
      data-phase={atmosphere.phase}
      data-shade-dir={atmosphere.shade.x < 0 ? 'rev' : 'fwd'}
      data-season={season?.theme}
      className={siteFontVariables}
      style={atmosphereCssVars(atmosphere) as CSSProperties}
      suppressHydrationWarning
    >
      <head>
        <style nonce={nonce} dangerouslySetInnerHTML={{ __html: phaseStylesheet() }} />
        <HeadScript nonce={nonce} preloader={settings.features.preloader && !maintenance} />
      </head>
      <body>
        <NextIntlClientProvider messages={clientMessages}>
          <MotionProvider>
            <AtmosphereProvider
              branch={branch ? { slug: branch.slug, name: tr(branch.name, locale), city: tr(branch.city, locale), lat: branch.lat, lng: branch.lng, timeZone: branch.timeZone } : null}
              initial={{ phase: atmosphere.phase, altitude: atmosphere.sun.altitude, azimuth: atmosphere.sun.azimuth, at: serving.now.toISOString(), minutesToSunset: atmosphere.minutesToSunset }}
            >
              <a href="#main" className="skip-link">
                {t('a11y.skip')}
              </a>
              {maintenance ? (
                <MaintenanceView message={tr(settings.maintenance.message, locale)} branches={branches} locale={locale} />
              ) : (
                <>
                  {settings.features.preloader ? <GnomonPreloader angle={Math.round(shadowAngle)} length={Math.round(shadowLength * 100) / 100} /> : null}
                  {children}
                  <Toaster closeLabel={t('a11y.close')} />
                </>
              )}
            </AtmosphereProvider>
          </MotionProvider>
        </NextIntlClientProvider>
      </body>
    </html>
  );
}
