import type { Metadata } from 'next';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import { Reveal } from '@/components/motion/reveal';
import { JsonLd, restaurantJsonLd } from '@/components/site/json-ld';
import { ArrowLink, ButtonAnchor } from '@/components/site/ui/button';
import { MediaImage } from '@/components/site/ui/media-image';
import { PageHeader } from '@/components/site/ui/page-header';
import { Link } from '@/i18n/navigation';
import { scheduleForDate } from '@/lib/domain/hours';
import { formatWallTime } from '@/lib/i18n/format';
import { tr } from '@/lib/i18n/localized';
import { getBranches, getSeasonalModes, hoursFor } from '@/lib/queries/branches';
import { getMediaIndex } from '@/lib/queries/catalog';
import { googleDirectionsUrl, telUrl } from '@/lib/services/maps';
import { pageMetadata } from '@/lib/site/metadata';
import { describeOpenStatus } from '@/lib/site/open-status';
import { formatTime, toDateString } from '@/lib/time/zoned';

export async function generateMetadata({ params }: { params: Promise<{ locale: string }> }): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: 'visit.locations.meta' });
  return pageMetadata({ locale, path: '/locations', title: t('title'), description: t('description'), image: 'locations' });
}

const FALLBACK_PHOTO: Record<number, string> = { 0: 'place-coral-house', 1: 'place-mudbrick' };

export default async function LocationsPage({ params }: { params: Promise<{ locale: string }> }) {
  const { locale } = await params;
  setRequestLocale(locale);
  const [t, tb, tc, branches, modes, media] = await Promise.all([getTranslations('visit.locations'), getTranslations('common.branch'), getTranslations('common'), getBranches(), getSeasonalModes(), getMediaIndex()]);
  const now = new Date();
  return (
    <>
      <JsonLd data={restaurantJsonLd(branches, locale)} />
      <PageHeader eyebrow={t('eyebrow')} title={t('title')} intro={<p>{t('intro')}</p>} />
      <ul className="site-grid gap-y-[var(--spacing-section)]">
        {branches.map((b, i) => {
          const hours = hoursFor(b, modes);
          const status = describeOpenStatus(hours, now, locale, (k, v) => tb(k, v));
          const today = scheduleForDate(hours, toDateString(now, b.timeZone));
          const photo = b.hero ?? media[FALLBACK_PHOTO[i] ?? 'place-courtyard-sun'] ?? null;
          return (
            <li key={b.id} className="col-span-full grid items-end gap-[var(--spacing-gutter)] gap-y-8 md:grid-cols-8 lg:grid-cols-12">
              {photo ? (
                <Reveal variant="shade" className={i % 2 ? 'md:col-span-4 md:col-start-5 lg:col-span-6 lg:col-start-7' : 'md:col-span-4 lg:col-span-6'}>
                  <MediaImage media={photo} locale={locale} sizes="(min-width: 1024px) 48vw, (min-width: 768px) 50vw, 100vw" ratio="4/5" shape="arch-4x5" preload={i === 0} />
                </Reveal>
              ) : null}
              <div className={i % 2 ? 'flex flex-col gap-5 md:col-span-4 md:col-start-1 md:row-start-1 lg:col-span-5' : 'flex flex-col gap-5 md:col-span-4 lg:col-span-5 lg:col-start-8'}>
                <p className="t-label text-muted">{tr(b.city, locale)}</p>
                <h2 className="t-display-md">
                  <Link href={`/locations/${b.slug}`} className="underline decoration-transparent underline-offset-[0.15em] hover-capable:hover:decoration-current">
                    {tr(b.name, locale)}
                  </Link>
                </h2>
                {b.story ? <p className="t-body-lg">{tr(b.story, locale)}</p> : null}
                <p className="t-body text-muted">{tr(b.address, locale)}</p>
                <p className="t-small flex flex-wrap items-center gap-2">
                  <span aria-hidden="true" className={status.open ? 'size-2.5 rounded-full bg-success' : 'size-2.5 rounded-full bg-muted'} />
                  <span className="font-semibold">{status.open ? tb('openNow') : tb('closedNow')}</span>
                  {status.detail ? <span className="text-muted">· {status.detail}</span> : null}
                </p>
                <p className="t-small">
                  <span className="text-muted">{t('today')}: </span>
                  {today.closed
                    ? t('closed')
                    : today.ranges.map((r) => (
                        <bdi key={r.start} className="tabular">
                          {formatWallTime(formatTime(r.start % 1440), locale)} – {formatWallTime(formatTime(r.end % 1440), locale)}{' '}
                        </bdi>
                      ))}
                </p>
                <div className="flex flex-wrap items-center gap-3">
                  <ButtonAnchor href={googleDirectionsUrl({ lat: b.lat, lng: b.lng })} target="_blank" size="sm" newWindowLabel={tc('a11y.newWindow')}>
                    {tc('actions.directions')}
                  </ButtonAnchor>
                  <ButtonAnchor href={telUrl(b.phone)} size="sm" variant="quiet" icon={null} leadingIcon="phone">
                    <bdi dir="ltr">{b.phone}</bdi>
                  </ButtonAnchor>
                </div>
                <ArrowLink href={`/locations/${b.slug}`}>{t('details')}</ArrowLink>
              </div>
            </li>
          );
        })}
      </ul>
    </>
  );
}
