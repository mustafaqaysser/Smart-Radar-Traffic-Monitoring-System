import { getLocale, getTranslations } from 'next-intl/server';
import restaurantConfig from '@config';
import { Lockup } from '@/components/brand/logo';
import { VentsBand } from '@/components/brand/patterns';
import { Link } from '@/i18n/navigation';
import { formatDate } from '@/lib/i18n/format';
import { tr } from '@/lib/i18n/localized';
import { getBranches, getSeasonalModes, hoursFor } from '@/lib/queries/branches';
import { getSettings } from '@/lib/server/settings';
import { describeOpenStatus } from '@/lib/site/open-status';
import { isDemoMode } from '@/lib/site/url';
import { telUrl } from '@/lib/services/maps';
import { NAV_GROUPS, visible } from './nav-config';
import { NewsletterForm } from './newsletter-form';

export async function SiteFooter() {
  const locale = await getLocale();
  const t = await getTranslations('common');
  const tb = await getTranslations('common.branch');
  const [branches, modes, settings] = await Promise.all([getBranches(), getSeasonalModes(), getSettings()]);
  const now = new Date();
  const year = formatDate(now, locale, restaurantConfig.defaultTimeZone, { year: 'numeric' });

  return (
    <footer className="relative mt-[var(--spacing-section)] border-t border-line pb-16 lg:pb-0">
      <VentsBand rows={1} className="text-line" />
      <div className="site-grid gap-y-16 pt-[var(--spacing-block)]">
        <p className="t-display-md col-span-full max-w-[14ch] lg:col-span-7" aria-hidden="true">
          {t('footer.line')}
        </p>
        {settings.features.newsletter ? (
          <section className="col-span-full lg:col-span-5 lg:pt-4" aria-labelledby="footer-newsletter">
            <h2 id="footer-newsletter" className="t-heading-sm">
              {t('footer.newsletterTitle')}
            </h2>
            <p className="t-small mt-2 mb-6 text-muted">{t('footer.newsletterBody')}</p>
            <NewsletterForm />
          </section>
        ) : null}

        <section className="col-span-full" aria-labelledby="footer-houses">
          <h2 id="footer-houses" className="t-label mb-6 text-muted">
            {t('footer.houses')}
          </h2>
          <ul className="grid gap-10 md:grid-cols-2">
            {branches.map((b) => {
              const status = describeOpenStatus(hoursFor(b, modes), now, locale, (k, v) => tb(k, v));
              return (
                <li key={b.id} className="border-t border-line pt-5">
                  <Link href={`/locations/${b.slug}`} className="t-heading-md underline decoration-transparent underline-offset-[0.2em] hover-capable:hover:decoration-current">
                    {tr(b.name, locale)}
                  </Link>
                  <p className="t-small mt-2 text-muted">{tr(b.address, locale)}</p>
                  <p className="t-small mt-3 flex flex-wrap items-center gap-x-3">
                    <span className="inline-flex items-center gap-2">
                      <span aria-hidden="true" className={status.open ? 'size-2 rounded-full bg-success' : 'size-2 rounded-full bg-muted'} />
                      {status.open ? tb('openNow') : tb('closedNow')}
                    </span>
                    {status.detail ? <span className="text-muted">{status.detail}</span> : null}
                  </p>
                  <a href={telUrl(b.phone)} dir="ltr" className="t-small mt-2 inline-block tabular underline decoration-line underline-offset-4">
                    {b.phone}
                  </a>
                </li>
              );
            })}
          </ul>
        </section>

        <nav aria-label={t('a11y.footerNav')} className="col-span-full grid grid-cols-2 gap-x-6 gap-y-10 md:grid-cols-4">
          {NAV_GROUPS.map((group) => (
            <div key={group.key}>
              <h2 className="t-label mb-4 text-muted">{t(`nav.groups.${group.key}`)}</h2>
              <ul className="space-y-2">
                {visible(group.items, settings.features).map((item) => (
                  <li key={item.href}>
                    <Link href={item.href} className="t-small underline decoration-transparent underline-offset-4 hover-capable:hover:decoration-current">
                      {t(`nav.${item.key}`)}
                    </Link>
                  </li>
                ))}
              </ul>
            </div>
          ))}
        </nav>

        <div className="col-span-full flex flex-col gap-8 border-t border-line pt-8 pb-10 lg:flex-row lg:items-end lg:justify-between">
          <div className="space-y-4">
            <Lockup locale={locale === 'ar' ? 'ar' : 'en'} decorative sunTint className="h-10 w-auto" />
            <p className="t-small text-muted">{t('footer.rights', { year, legalName: tr(restaurantConfig.legalName, locale) })}</p>
            <p className="t-small text-muted">
              {t('footer.vat', { number: settings.tax.registrationNumber })} · {t('footer.pricesInclude')}
            </p>
            {isDemoMode() ? <p className="t-small max-w-prose text-muted">{t('footer.demo')}</p> : null}
          </div>
          <div className="flex flex-col gap-4 lg:items-end">
            {settings.social.length ? (
              <ul className="flex flex-wrap gap-x-5 gap-y-2" aria-label={t('footer.follow')}>
                {settings.social.map((s) => (
                  <li key={s.id}>
                    <a href={s.url} target="_blank" rel="noopener noreferrer" className="t-small underline decoration-line underline-offset-4">
                      {s.label}
                      <span className="sr-only"> ({t('a11y.newWindow')})</span>
                    </a>
                  </li>
                ))}
              </ul>
            ) : null}
            <ul className="t-small flex flex-wrap gap-x-5 gap-y-2 text-muted" aria-label={t('footer.legal')}>
              <li><Link href="/legal/privacy" className="underline decoration-line underline-offset-4">{t('footer.privacy')}</Link></li>
              <li><Link href="/legal/terms" className="underline decoration-line underline-offset-4">{t('footer.terms')}</Link></li>
              <li><Link href="/legal/cookies" className="underline decoration-line underline-offset-4">{t('footer.cookies')}</Link></li>
              <li><Link href="/legal/allergens" className="underline decoration-line underline-offset-4">{t('footer.allergenPolicy')}</Link></li>
              <li><Link href="/legal/accessibility" className="underline decoration-line underline-offset-4">{t('footer.accessibility')}</Link></li>
              <li><Link href="/credits" className="underline decoration-line underline-offset-4">{t('footer.credits')}</Link></li>
              <li><Link href="/links" className="underline decoration-line underline-offset-4">{t('footer.links')}</Link></li>
            </ul>
          </div>
        </div>
      </div>
    </footer>
  );
}
