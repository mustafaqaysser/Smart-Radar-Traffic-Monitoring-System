import type { CSSProperties, ReactNode } from 'react';
import type { Metadata, Viewport } from 'next';
import { notFound } from 'next/navigation';
import { headers } from 'next/headers';
import { hasLocale, NextIntlClientProvider } from 'next-intl';
import { getTranslations, setRequestLocale } from 'next-intl/server';
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
import { getServingContext, servingNames } from '@/lib/site/serving';
import { absoluteUrl, siteUrl } from '@/lib/site/url';
import { MotionProvider } from '@/components/motion/motion-provider';
import { AtmosphereProvider } from '@/components/site/atmosphere-provider';
import { SiteHeader } from '@/components/site/chrome/site-header';
import { SiteFooter } from '@/components/site/chrome/site-footer';
import { GnomonPreloader } from '@/components/site/gnomon-preloader';
import { HeadScript } from '@/components/site/head-script';
import { SeasonBanner } from '@/components/site/season-banner';
import { MaintenanceView } from '@/components/site/maintenance-view';
import '@/styles/site.css';

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

  const [nonce, settings, branch, branches, user, t] = await Promise.all([
    headers().then((h) => h.get('x-nonce') ?? undefined),
    getSettings(),
    getSelectedBranch(),
    getBranches(),
    getCurrentUser(),
    getTranslations('common'),
  ]);
  const serving = await getServingContext(branch);
  const location = branch ?? { lat: 21.4854, lng: 39.1869, timeZone: restaurantConfig.defaultTimeZone };
  const atmosphere = computeAtmosphere(serving.now, { lat: location.lat, lng: location.lng, timeZone: location.timeZone });
  const season = serving.seasons.find((s) => s.banner && settings.features.seasonalModes);
  const maintenance = settings.maintenance.enabled && !isStaff(user?.role);

  // Preloader: the shadow's final angle on the dial (screen degrees, 0 = east, clockwise) and its length.
  const daylight = atmosphere.sun.altitude > 0;
  const shadowAngle = daylight ? atmosphere.sun.azimuth + 90 : 90;
  const shadowLength = daylight ? Math.min(1.6, Math.max(0.3, 1 / Math.tan((Math.max(atmosphere.sun.altitude, 4) * Math.PI) / 180))) : 0.35;

  const branchOptions = branches.map((b) => ({ slug: b.slug, name: tr(b.shortName, locale), city: tr(b.city, locale) }));

  return (
    <html
      lang={locale}
      dir={locale === 'ar' ? 'rtl' : 'ltr'}
      data-phase={atmosphere.phase}
      data-shade-dir={atmosphere.shade.x < 0 ? 'rev' : 'fwd'}
      data-season={season?.theme && season.theme !== 'none' ? season.theme : undefined}
      className={siteFontVariables}
      style={atmosphereCssVars(atmosphere) as CSSProperties}
      suppressHydrationWarning
    >
      <head>
        <style nonce={nonce} dangerouslySetInnerHTML={{ __html: phaseStylesheet() }} />
        <HeadScript nonce={nonce} preloader={settings.features.preloader && !maintenance} />
      </head>
      <body>
        <NextIntlClientProvider>
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
                  <SiteHeader
                    features={settings.features}
                    branches={branchOptions}
                    selectedBranch={branch?.slug ?? null}
                    signedIn={Boolean(user)}
                    servingNow={servingNames(serving, locale)}
                  />
                  {season?.banner ? <SeasonBanner slug={season.slug} name={tr(season.name, locale)} text={tr(season.banner, locale)} /> : null}
                  <main id="main" tabIndex={-1} className="outline-none">
                    {children}
                  </main>
                  <SiteFooter />
                </>
              )}
            </AtmosphereProvider>
          </MotionProvider>
        </NextIntlClientProvider>
      </body>
    </html>
  );
}
