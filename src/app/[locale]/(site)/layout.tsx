import type { ReactNode } from 'react';
import { getLocale } from 'next-intl/server';
import { SiteHeader } from '@/components/site/chrome/site-header';
import { SiteFooter } from '@/components/site/chrome/site-footer';
import { CartSync } from '@/components/site/order/cart-sync';
import { SeasonBanner } from '@/components/site/season-banner';
import { getCurrentUser } from '@/lib/auth/session';
import { tr } from '@/lib/i18n/localized';
import { getBranches } from '@/lib/queries/branches';
import { effectiveFeatures, getSettings } from '@/lib/server/settings';
import { getSelectedBranch } from '@/lib/site/selection';
import { getServingContext, servingNames } from '@/lib/site/serving';

/** The public site's chrome: header with navigation, seasonal notice, main landmark and footer. */
export default async function SiteLayout({ children }: { children: ReactNode }) {
  const locale = await getLocale();
  const [settings, features, branch, branches, user] = await Promise.all([getSettings(), effectiveFeatures(), getSelectedBranch(), getBranches(), getCurrentUser()]);
  const serving = await getServingContext(branch);
  const season = settings.features.seasonalModes ? serving.seasons.find((s) => s.banner) : undefined;
  return (
    <>
      <SiteHeader
        features={features}
        branches={branches.map((b) => ({ slug: b.slug, name: tr(b.shortName, locale), city: tr(b.city, locale) }))}
        selectedBranch={branch?.slug ?? null}
        signedIn={Boolean(user)}
        servingNow={servingNames(serving, locale)}
      />
      {season?.banner ? <SeasonBanner slug={season.slug} name={tr(season.name, locale)} text={tr(season.banner, locale)} /> : null}
      <main id="main" tabIndex={-1} className="outline-none">
        {children}
      </main>
      <SiteFooter />
      {settings.features.ordering ? <CartSync signedIn={Boolean(user)} /> : null}
    </>
  );
}
