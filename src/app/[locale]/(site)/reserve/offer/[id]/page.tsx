import type { Metadata } from 'next';
import { notFound } from 'next/navigation';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import restaurantConfig from '@config';
import { SplitWords } from '@/components/motion/split-words';
import { BookingRows, HourCard } from '@/components/site/reserve/booking-pieces';
import { OfferBooking } from '@/components/site/reserve/offer-booking';
import { ButtonLink } from '@/components/site/ui/button';
import { PageHeader } from '@/components/site/ui/page-header';
import { db } from '@/lib/db/client';
import { formatClock, formatDate } from '@/lib/i18n/format';
import { tr } from '@/lib/i18n/localized';
import { plural } from '@/lib/i18n/plural';
import { getBranch } from '@/lib/queries/branches';
import { bookingPath, depositFor, waitlistOffer } from '@/lib/server/reservations';
import { featureEnabled } from '@/lib/server/settings';
import { pageMetadata } from '@/lib/site/metadata';

type Params = Promise<{ locale: string; id: string }>;
type Search = Promise<Record<string, string | string[] | undefined>>;

const one = (v: string | string[] | undefined): string | null => (Array.isArray(v) ? (v[0] ?? null) : (v ?? null));

export async function generateMetadata({ params }: { params: Params }): Promise<Metadata> {
  const { locale, id } = await params;
  const t = await getTranslations({ locale, namespace: 'reserve.offer' });
  return pageMetadata({ locale, path: `/reserve/offer/${id}`, title: t('meta'), noindex: true });
}

export default async function WaitlistOfferPage({ params, searchParams }: { params: Params; searchParams: Search }) {
  const { locale, id } = await params;
  setRequestLocale(locale);
  const token = one((await searchParams).token);
  const [t, tc, tu, tsun, newsletter] = await Promise.all([getTranslations('reserve'), getTranslations('common'), getTranslations('common.units'), getTranslations('visit.locations.sun'), featureEnabled('newsletter')]);
  const offer = await waitlistOffer(id, token, new Date());
  if (!offer || !token) {
    return (
      <PageHeader eyebrow={t('offer.eyebrow')} title={t('offer.title')} intro={<p>{t('manage.invalid')}</p>}>
        <ButtonLink href="/reserve" className="self-start">
          {tc('actions.reserve')}
        </ButtonLink>
      </PageHeader>
    );
  }
  const { entry, hold } = offer;
  const branch = await getBranch(entry.branchId);
  if (!branch) notFound();
  const search = { pathname: '/reserve' as const, query: { branch: branch.slug, date: entry.date, party: String(entry.partySize) } };

  if (entry.status === 'booked' && entry.reservationId) {
    const r = await db.query.reservations.findFirst({ where: (rs, { eq }) => eq(rs.id, entry.reservationId as string) });
    return (
      <PageHeader eyebrow={t('offer.eyebrow')} title={t('offer.title')} intro={<p>{t('offer.booked')}</p>}>
        {r ? (
          <ButtonLink href={bookingPath(r)} className="self-start">
            {t('summary.title')}
          </ButtonLink>
        ) : null}
      </PageHeader>
    );
  }

  if (!hold) {
    return (
      <PageHeader eyebrow={t('offer.eyebrow')} title={t('offer.title')} intro={<p>{t('offer.expired')}</p>}>
        <ButtonLink href={search} className="self-start">
          {t('offer.search')}
        </ButtonLink>
      </PageHeader>
    );
  }

  const dateLabel = formatDate(hold.startsAt, locale, branch.timeZone, { weekday: 'long', day: 'numeric', month: 'long' });
  const timeLabel = formatClock(hold.startsAt, locale, branch.timeZone);
  return (
    <article className="site-grid gap-y-12 pt-10 pb-[var(--spacing-section)] lg:pt-16">
      <header className="col-span-full flex flex-col gap-6 lg:col-span-7">
        <p className="t-label text-muted">{t('offer.eyebrow')}</p>
        <h1 className="t-display-lg">
          <SplitWords text={t('offer.title')} />
        </h1>
        <p className="t-body-lg measure">{t('offer.body', { time: formatClock(hold.expiresAt, locale, branch.timeZone) })}</p>
      </header>
      <div className="col-span-full flex flex-col gap-8 lg:col-span-5 lg:col-start-8 lg:row-span-2">
        <HourCard at={hold.startsAt} branch={branch} locale={locale} dateLabel={dateLabel} timeLabel={timeLabel} lightLabel={(phase) => t(`light.${phase}`)} sunLabels={{ sunrise: tsun('sunrise'), sunset: tsun('maghrib') }} />
        <BookingRows
          rows={[
            { label: t('fields.branch'), value: tr(branch.name, locale) },
            { label: t('fields.date'), value: dateLabel },
            { label: t('fields.time'), value: <bdi>{timeLabel}</bdi> },
            { label: t('fields.party'), value: tu('guests', plural(entry.partySize, locale)) },
          ]}
        />
      </div>
      <div className="col-span-full lg:col-span-7">
        <OfferBooking
          entry={entry.id}
          token={token}
          expiresAt={hold.expiresAt.toISOString()}
          guest={{ name: entry.name, email: entry.email, phone: entry.phone }}
          deposit={depositFor(entry.partySize)}
          rules={{ minParty: restaurantConfig.reservations.deposit.minParty, perGuest: restaurantConfig.reservations.deposit.perGuest * 100, cutoffHours: restaurantConfig.reservations.modifyCutoffHours }}
          newsletter={newsletter}
        />
      </div>
    </article>
  );
}
