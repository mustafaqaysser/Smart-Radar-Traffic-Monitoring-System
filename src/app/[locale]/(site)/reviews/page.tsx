import type { Metadata } from 'next';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import restaurantConfig from '@config';
import { Reveal } from '@/components/motion/reveal';
import { ReviewForm } from '@/components/site/review-form';
import { PageHeader } from '@/components/site/ui/page-header';
import { Stars } from '@/components/site/ui/stars';
import { Link } from '@/i18n/navigation';
import { formatDateString, formatNumber } from '@/lib/i18n/format';
import { tr } from '@/lib/i18n/localized';
import { plural } from '@/lib/i18n/plural';
import { getBranches } from '@/lib/queries/branches';
import { getPublishedReviews } from '@/lib/queries/content';
import { getSelectedBranch } from '@/lib/site/selection';
import { pageMetadata } from '@/lib/site/metadata';
import { toDateString } from '@/lib/time/zoned';
import { cn } from '@/lib/utils/cn';
import { JsonLd } from '@/components/site/json-ld';
import { absoluteUrl } from '@/lib/site/url';

export async function generateMetadata({ params }: { params: Promise<{ locale: string }> }): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: 'pages.reviews.meta' });
  return pageMetadata({ locale, path: '/reviews', title: t('title'), description: t('description'), image: 'reviews' });
}

export default async function ReviewsPage({ params, searchParams }: { params: Promise<{ locale: string }>; searchParams: Promise<{ house?: string }> }) {
  const { locale } = await params;
  const { house } = await searchParams;
  setRequestLocale(locale);
  const [t, all, branches, selected] = await Promise.all([getTranslations('pages.reviews'), getPublishedReviews(), getBranches(), getSelectedBranch()]);
  const branch = branches.find((b) => b.slug === house) ?? null;
  const list = branch ? all.filter((r) => r.branchId === branch.id) : all;
  const average = list.length ? list.reduce((s, r) => s + r.rating, 0) / list.length : 0;
  const ordered = [...list.filter((r) => r.locale === locale), ...list.filter((r) => r.locale !== locale)];
  const today = toDateString(new Date(), restaurantConfig.defaultTimeZone);

  return (
    <>
      {all.length ? (
        <JsonLd
          data={{
            '@context': 'https://schema.org',
            '@type': 'Restaurant',
            '@id': absoluteUrl('/#organization'),
            name: tr(restaurantConfig.name, locale),
            aggregateRating: { '@type': 'AggregateRating', ratingValue: (all.reduce((s, r) => s + r.rating, 0) / all.length).toFixed(1), reviewCount: all.length, bestRating: 5, worstRating: 1 },
          }}
        />
      ) : null}
      <PageHeader eyebrow={t('eyebrow')} title={t('title')} intro={<p>{t('intro')}</p>}>
        {list.length ? <p className="t-heading-sm">{t('summary', { average: formatNumber(average, locale, { maximumFractionDigits: 1 }), ...plural(list.length, locale) })}</p> : null}
        <nav aria-label={t('eyebrow')} className="flex flex-wrap gap-2">
          {[null, ...branches].map((b) => {
            const active = (b?.slug ?? null) === (branch?.slug ?? null);
            return (
              <Link
                key={b?.slug ?? 'all'}
                href={b ? { pathname: '/reviews', query: { house: b.slug } } : '/reviews'}
                aria-current={active ? 'page' : undefined}
                className={cn('inline-flex min-h-11 items-center rounded-pill border px-4 text-[0.9375rem]', active ? 'border-ink bg-ink text-bg' : 'border-line hover-capable:hover:border-ink')}
              >
                {b ? tr(b.shortName, locale) : t('filterAll')}
              </Link>
            );
          })}
        </nav>
      </PageHeader>
      <section className="site-grid" aria-label={t('eyebrow')}>
        {ordered.length === 0 ? <p className="t-body col-span-full text-muted">{t('none')}</p> : null}
        <ul className="col-span-full columns-1 gap-[var(--spacing-gutter)] md:columns-2 lg:columns-3">
          {ordered.map((r, i) => {
            const house = branches.find((b) => b.id === r.branchId);
            return (
              <Reveal as="li" key={r.id} delay={(i % 3) * 60} className="mb-[var(--spacing-gutter)] break-inside-avoid border-t border-line pt-6 pb-4">
                <figure className="flex flex-col gap-4">
                  <Stars rating={r.rating} label={t('write.ratingValue', { n: formatNumber(r.rating, locale) })} />
                  <blockquote lang={r.locale} dir={r.locale === 'ar' ? 'rtl' : 'ltr'} className="flex flex-col gap-2">
                    {r.title ? <p className="t-heading-sm">{r.title}</p> : null}
                    <p className="t-body">{r.body}</p>
                  </blockquote>
                  <figcaption className="t-small text-muted">
                    <bdi>{r.name}</bdi>
                    {house ? ` · ${tr(house.shortName, locale)}` : ''}
                    {r.visitDate ? ` · ${t('visited', { date: formatDateString(r.visitDate, locale, { month: 'long', year: 'numeric' }) })}` : ''}
                    {r.locale !== locale ? ` · ${t('original', { language: t(`languages.${r.locale}`) })}` : ''}
                  </figcaption>
                  {r.response ? (
                    <div className="border-s-2 border-accent ps-4">
                      <p className="t-label mb-1 text-muted">{t('reply')}</p>
                      <p className="t-small">{tr(r.response, locale)}</p>
                    </div>
                  ) : null}
                </figure>
              </Reveal>
            );
          })}
        </ul>
      </section>
      <section className="site-grid gap-y-8 pt-[var(--spacing-section)]" aria-labelledby="review-write">
        <div className="col-span-full flex flex-col gap-4 lg:col-span-4">
          <h2 id="review-write" className="t-display-md">
            {t('write.title')}
          </h2>
          <p className="t-body text-muted">{t('write.intro')}</p>
        </div>
        <div className="col-span-full lg:col-span-7 lg:col-start-6">
          <ReviewForm branches={branches.map((b) => ({ slug: b.slug, name: tr(b.shortName, locale) }))} selected={branch?.slug ?? selected?.slug ?? null} today={today} />
        </div>
      </section>
    </>
  );
}
