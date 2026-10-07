import type { Metadata } from 'next';
import { notFound } from 'next/navigation';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import { Icon } from '@/components/brand/icon';
import { SplitWords } from '@/components/motion/split-words';
import { Reveal } from '@/components/motion/reveal';
import { HouseHours } from '@/components/site/house-hours';
import { HouseMap } from '@/components/site/house-map';
import { JsonLd, branchJsonLd } from '@/components/site/json-ld';
import { BackLink, ButtonAnchor, ButtonLink } from '@/components/site/ui/button';
import { MediaImage } from '@/components/site/ui/media-image';
import { Link } from '@/i18n/navigation';
import { formatClock, formatDate, formatPercent } from '@/lib/i18n/format';
import { tr } from '@/lib/i18n/localized';
import { plural } from '@/lib/i18n/plural';
import { getBranch, getSeasonalModes, hoursFor } from '@/lib/queries/branches';
import { getMediaIndex } from '@/lib/queries/catalog';
import { getPrivateDining, getUpcomingEvents } from '@/lib/queries/content';
import { getSettings } from '@/lib/server/settings';
import { appleMapsUrl, googleDirectionsUrl, telUrl, wazeUrl, whatsappUrl } from '@/lib/services/maps';
import { pageMetadata } from '@/lib/site/metadata';
import { describeOpenStatus } from '@/lib/site/open-status';
import { sunFacts } from '@/lib/site/sun-facts';
import { toDateString } from '@/lib/time/zoned';

type Params = Promise<{ locale: string; slug: string }>;

export async function generateMetadata({ params }: { params: Params }): Promise<Metadata> {
  const { locale, slug } = await params;
  const branch = await getBranch(slug);
  if (!branch) return {};
  return pageMetadata({ locale, path: `/locations/${slug}`, title: tr(branch.name, locale), description: tr(branch.address, locale), image: branch.hero?.src ?? 'locations' });
}

export default async function LocationPage({ params }: { params: Params }) {
  const { locale, slug } = await params;
  setRequestLocale(locale);
  const branch = await getBranch(slug);
  if (!branch) notFound();
  const [t, tb, tc, modes, media, rooms, events, settings] = await Promise.all([
    getTranslations('visit.locations'),
    getTranslations('common.branch'),
    getTranslations('common'),
    getSeasonalModes(),
    getMediaIndex(),
    getPrivateDining(),
    getUpcomingEvents(),
    getSettings(),
  ]);
  const now = new Date();
  const today = toDateString(now, branch.timeZone);
  const hours = hoursFor(branch, modes);
  const status = describeOpenStatus(hours, now, locale, (k, v) => tb(k, v));
  const sun = sunFacts(today, branch.lat, branch.lng);
  const name = tr(branch.shortName, locale);
  const point = { lat: branch.lat, lng: branch.lng };
  const photo = branch.hero ?? media['place-courtyard-sun'] ?? null;
  const houseRooms = rooms.rooms.filter((r) => r.branchId === branch.id);
  const houseEvents = events.filter((e) => e.branchId === branch.id && new Date(e.startsAt) > now).slice(0, 3);
  const shadowLine =
    sun.noonShadowRatio === null
      ? t('sun.noShadow')
      : sun.shadowFallsSouth
        ? t('sun.southShadow', { percent: formatPercent(sun.noonShadowRatio, locale) })
        : t('sun.shadow', { percent: formatPercent(sun.noonShadowRatio, locale) });

  return (
    <article aria-labelledby="house-title">
      <JsonLd data={{ '@context': 'https://schema.org', ...branchJsonLd(branch, locale) }} />
      <div className="site-grid gap-y-10 pt-8 lg:pt-12">
        <nav className="col-span-full">
          <BackLink href="/locations">{t('eyebrow')}</BackLink>
        </nav>
        <header className="col-span-full flex flex-col gap-6 md:col-span-4 lg:col-span-6 lg:self-end">
          <p className="t-label text-muted">{tr(branch.city, locale)}</p>
          <h1 id="house-title" className="t-display-lg">
            <SplitWords text={tr(branch.name, locale)} />
          </h1>
          {branch.story ? <p className="t-body-lg measure">{tr(branch.story, locale)}</p> : null}
          <p className="t-body flex flex-wrap items-center gap-2">
            <span aria-hidden="true" className={status.open ? 'size-2.5 rounded-full bg-success' : 'size-2.5 rounded-full bg-muted'} />
            <span className="font-semibold">{status.open ? tb('openNow') : tb('closedNow')}</span>
            {status.detail ? <span className="text-muted">· {status.detail}</span> : null}
          </p>
          <div className="flex flex-wrap gap-3">
            {settings.features.reservations && branch.reservationsEnabled ? (
              <ButtonLink href={{ pathname: '/reserve', query: { branch: branch.slug } }}>{t('reserveHere', { house: name })}</ButtonLink>
            ) : null}
            {settings.features.ordering && !branch.orderingPaused ? (
              <ButtonLink href={{ pathname: '/order', query: { branch: branch.slug } }} variant="secondary">
                {t('orderHere', { house: name })}
              </ButtonLink>
            ) : null}
          </div>
        </header>
        {photo ? (
          <Reveal variant="shade" className="col-span-full md:col-span-4 lg:col-span-5 lg:col-start-8">
            <MediaImage media={photo} locale={locale} sizes="(min-width: 1024px) 40vw, (min-width: 768px) 50vw, 100vw" ratio="4/5" shape="arch-4x5" preload className="cast-shade" />
          </Reveal>
        ) : null}
      </div>

      <div className="site-grid gap-y-14 pt-[var(--spacing-section)]">
        <section className="col-span-full md:col-span-4 lg:col-span-5" aria-labelledby="house-hours">
          <h2 id="house-hours" className="t-heading-lg mb-6">
            {t('weekTitle')}
          </h2>
          <HouseHours branch={branch} hours={hours} today={today} locale={locale} />
        </section>

        <section className="col-span-full flex flex-col gap-6 md:col-span-4 lg:col-span-6 lg:col-start-7" aria-labelledby="house-sun">
          <h2 id="house-sun" className="t-heading-lg">
            {t('sun.title', { house: name })}
          </h2>
          <dl className="grid grid-cols-3 gap-4 border-t border-ink pt-6">
            {[
              { label: t('sun.sunrise'), at: sun.sunrise },
              { label: t('sun.noon'), at: sun.solarNoon },
              { label: t('sun.maghrib'), at: sun.sunset },
            ].map((x) => (
              <div key={x.label} className="flex flex-col gap-1">
                <dt className="t-label text-muted">{x.label}</dt>
                <dd className="t-heading-md tabular">{x.at ? <bdi>{formatClock(x.at, locale, branch.timeZone)}</bdi> : '—'}</dd>
              </div>
            ))}
          </dl>
          <p className="t-body-lg">{shadowLine}</p>
          <p className="t-small text-muted">{formatDate(now, locale, branch.timeZone, { weekday: 'long', day: 'numeric', month: 'long' })}</p>
        </section>

        <section className="col-span-full flex flex-col gap-6" aria-labelledby="house-map">
          <h2 id="house-map" className="t-heading-lg">
            {t('mapTitle')}
          </h2>
          <p className="t-body-lg">{tr(branch.address, locale)}</p>
          <HouseMap lat={branch.lat} lng={branch.lng} label={tr(branch.name, locale)} />
          <div className="flex flex-col gap-3">
            <p className="t-label text-muted">{t('directions')}</p>
            <div className="flex flex-wrap gap-3">
              <ButtonAnchor href={googleDirectionsUrl(point)} target="_blank" size="sm" newWindowLabel={tc('a11y.newWindow')}>
                {t('google')}
              </ButtonAnchor>
              <ButtonAnchor href={appleMapsUrl(point, tr(branch.name, locale))} target="_blank" size="sm" newWindowLabel={tc('a11y.newWindow')}>
                {t('apple')}
              </ButtonAnchor>
              <ButtonAnchor href={wazeUrl(point)} target="_blank" size="sm" newWindowLabel={tc('a11y.newWindow')}>
                {t('waze')}
              </ButtonAnchor>
            </div>
          </div>
        </section>

        <section className="col-span-full flex flex-col gap-4 md:col-span-4 lg:col-span-4" aria-labelledby="house-contact">
          <h2 id="house-contact" className="t-heading-md">
            {t('contact')}
          </h2>
          <a href={telUrl(branch.phone)} dir="ltr" className="t-body inline-flex items-center gap-3 self-start underline decoration-line underline-offset-4">
            <Icon name="phone" size={18} />
            <bdi className="tabular">{branch.phone}</bdi>
          </a>
          {branch.whatsapp ? (
            <a href={whatsappUrl(branch.whatsapp)} target="_blank" rel="noopener noreferrer" className="t-body inline-flex items-center gap-3 self-start underline decoration-line underline-offset-4">
              <Icon name="chat" size={18} />
              {tc('actions.whatsapp')}
              <span className="sr-only"> ({tc('a11y.newWindow')})</span>
            </a>
          ) : null}
          <a href={`mailto:${branch.email}`} dir="ltr" className="t-body inline-flex items-center gap-3 self-start underline decoration-line underline-offset-4">
            <Icon name="mail" size={18} />
            {branch.email}
          </a>
        </section>
        {branch.parking ? (
          <section className="col-span-full flex flex-col gap-4 md:col-span-4 lg:col-span-4" aria-labelledby="house-parking">
            <h2 id="house-parking" className="t-heading-md flex items-center gap-3">
              <Icon name="parking" size={22} />
              {t('parking')}
            </h2>
            <p className="t-body">{tr(branch.parking, locale)}</p>
          </section>
        ) : null}
        {branch.accessibility ? (
          <section className="col-span-full flex flex-col gap-4 md:col-span-4 lg:col-span-4" aria-labelledby="house-access">
            <h2 id="house-access" className="t-heading-md flex items-center gap-3">
              <Icon name="accessible" size={22} />
              {t('access')}
            </h2>
            <p className="t-body">{tr(branch.accessibility, locale)}</p>
          </section>
        ) : null}

        {houseRooms.length && settings.features.privateDining ? (
          <section className="col-span-full" aria-labelledby="house-rooms">
            <h2 id="house-rooms" className="t-heading-lg mb-6">
              {t('rooms')}
            </h2>
            <ul className="grid gap-[var(--spacing-gutter)] md:grid-cols-3">
              {houseRooms.map((r) => (
                <li key={r.id} data-card className="relative flex flex-col gap-3">
                  {r.image ? <MediaImage media={r.image} locale={locale} sizes="(min-width: 768px) 30vw, 100vw" ratio="4/3" /> : null}
                  <h3 className="t-heading-sm">
                    <Link href="/private-dining" className="card-link">
                      {tr(r.name, locale)}
                    </Link>
                  </h3>
                  <p className="t-small text-muted">{t('seated', plural(r.seated, locale))}</p>
                </li>
              ))}
            </ul>
          </section>
        ) : null}

        {houseEvents.length && settings.features.events ? (
          <section className="col-span-full" aria-labelledby="house-events">
            <h2 id="house-events" className="t-heading-lg mb-6">
              {t('events')}
            </h2>
            <ul className="border-t border-ink">
              {houseEvents.map((e) => (
                <li key={e.id} className="flex flex-wrap items-baseline justify-between gap-4 border-b border-line py-5">
                  <Link href={`/experiences/${e.slug}`} className="t-heading-sm underline decoration-transparent underline-offset-4 hover-capable:hover:decoration-current">
                    {tr(e.title, locale)}
                  </Link>
                  <span className="t-small text-muted">
                    {formatDate(new Date(e.startsAt), locale, branch.timeZone, { weekday: 'long', day: 'numeric', month: 'long' })} · {formatClock(new Date(e.startsAt), locale, branch.timeZone)}
                  </span>
                </li>
              ))}
            </ul>
          </section>
        ) : null}
      </div>
    </article>
  );
}
