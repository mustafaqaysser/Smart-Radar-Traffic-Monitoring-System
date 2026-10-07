import type { Metadata } from 'next';
import { notFound } from 'next/navigation';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import restaurantConfig from '@config';
import { BookingRows } from '@/components/site/reserve/booking-pieces';
import { ManageBooking } from '@/components/site/reserve/manage-booking';
import { ButtonAnchor, ButtonLink } from '@/components/site/ui/button';
import { PageHeader } from '@/components/site/ui/page-header';
import { formatMoney } from '@/lib/i18n/format';
import { getBranch } from '@/lib/queries/branches';
import { isAreaChoice } from '@/lib/reserve/types';
import { changeable, describeReservation, icsPath, partyRangeForChange, reservationByCode, reservationCalendarUrl } from '@/lib/server/reservations';
import { reserveSetup } from '@/lib/server/reserve-setup';
import { telUrl } from '@/lib/services/maps';
import { pageMetadata } from '@/lib/site/metadata';
import { cn } from '@/lib/utils/cn';

type Params = Promise<{ locale: string; code: string }>;
type Search = Promise<Record<string, string | string[] | undefined>>;

const one = (v: string | string[] | undefined): string | null => (Array.isArray(v) ? (v[0] ?? null) : (v ?? null));

export async function generateMetadata({ params }: { params: Params }): Promise<Metadata> {
  const { locale, code } = await params;
  const t = await getTranslations({ locale, namespace: 'reserve.manage' });
  return pageMetadata({ locale, path: `/reserve/manage/${code}`, title: t('meta'), noindex: true });
}

export default async function ManageReservationPage({ params, searchParams }: { params: Params; searchParams: Search }) {
  const { locale, code } = await params;
  setRequestLocale(locale);
  const token = one((await searchParams).token);
  const [t, tc] = await Promise.all([getTranslations('reserve'), getTranslations('common')]);
  const r = await reservationByCode(code, token);
  if (!r || !token) {
    return (
      <PageHeader eyebrow={t('eyebrow')} title={t('manage.title')} intro={<p>{t('manage.invalid')}</p>}>
        <div className="flex flex-wrap gap-3">
          <ButtonLink href="/reserve">{tc('actions.reserve')}</ButtonLink>
          <ButtonLink href="/contact" variant="secondary">
            {tc('nav.contact')}
          </ButtonLink>
        </div>
      </PageHeader>
    );
  }
  const branch = await getBranch(r.branchId);
  if (!branch) notFound();
  const labels = await describeReservation(r, branch, locale);
  const active = r.status === 'confirmed' || r.status === 'pending';
  const canChange = changeable(r, new Date()) && branch.reservationsEnabled;
  const setup = canChange ? await reserveSetup(locale) : null;
  const flowBranch = setup?.branches.find((b) => b.slug === branch.slug) ?? null;
  const rows = [
    { label: t('fields.branch'), value: labels.branchName },
    { label: t('fields.date'), value: labels.dateLabel },
    { label: t('fields.time'), value: <bdi>{labels.timeLabel}</bdi> },
    { label: t('fields.party'), value: labels.partyLabel },
    { label: t('summary.seating'), value: labels.areaLabel },
    ...(labels.occasionLabel ? [{ label: t('summary.occasion'), value: labels.occasionLabel }] : []),
    ...(labels.depositLabel ? [{ label: t('summary.deposit'), value: labels.depositLabel }] : []),
    { label: t('summary.reference'), value: <bdi>{r.code}</bdi>, strong: true },
  ];
  const refundable = r.depositStatus === 'paid';
  const depositNote = refundable ? t('manage.refundWill', { amount: formatMoney(r.depositAmount, locale) }) : null;

  return (
    <article className="site-grid gap-y-12 pt-10 pb-[var(--spacing-section)] lg:pt-16">
      <header className="col-span-full flex flex-col gap-5 lg:col-span-7">
        <p className="t-label text-muted">{t('confirmed.eyebrow', { code: r.code })}</p>
        <h1 className="t-display-lg">{t('manage.title')}</h1>
        <p id="booking-status" tabIndex={-1} aria-live="polite" className="flex items-center gap-3 focus:outline-none">
          <span aria-hidden="true" className={cn('size-2.5 rounded-full', r.status === 'confirmed' ? 'bg-success' : r.status === 'pending' ? 'bg-warning' : 'bg-muted')} />
          <span className="t-heading-sm">{t(`manage.status.${r.status}`)}</span>
        </p>
      </header>

      <div className="col-span-full flex flex-col gap-8 lg:col-span-5 lg:col-start-8 lg:row-span-2">
        <BookingRows rows={rows} />
        {active ? (
          <div className="flex flex-wrap gap-3">
            <ButtonAnchor href={reservationCalendarUrl(r, branch, labels)} target="_blank" size="sm" newWindowLabel={tc('a11y.newWindow')}>
              {t('confirmed.google')}
            </ButtonAnchor>
            <ButtonAnchor href={icsPath(r)} download={`zill-${r.code}.ics`} icon="download" size="sm">
              {t('confirmed.ics')}
            </ButtonAnchor>
          </div>
        ) : null}
      </div>

      <div className="col-span-full lg:col-span-7">
        {r.status === 'cancelled' ? (
          <div className="flex flex-col gap-5">
            <p className="t-body-lg measure">{t('manage.cancelled')}</p>
            {r.depositStatus === 'refunded' ? <p className="t-body">{t('manage.refund', { amount: formatMoney(r.depositAmount, locale) })}</p> : null}
            <ButtonLink href={{ pathname: '/reserve', query: { branch: branch.slug, party: String(r.partySize) } }} className="self-start">
              {t('offer.search')}
            </ButtonLink>
          </div>
        ) : !active ? (
          <p className="t-body-lg measure">{t('manage.closedStatus')}</p>
        ) : canChange && flowBranch ? (
          <ManageBooking
            code={r.code}
            token={token}
            branch={flowBranch}
            current={{ date: r.date, time: r.time, party: r.partySize, area: isAreaChoice(r.area) ? r.area : 'any' }}
            partyRange={partyRangeForChange(r)}
            maxPartyOnline={restaurantConfig.reservations.maxPartyOnline}
            depositNote={depositNote}
          />
        ) : (
          <div className="flex flex-col gap-5">
            <p className="t-body-lg measure">{t('manage.locked', { hours: labels.cutoffLabel })}</p>
            <ButtonAnchor href={telUrl(branch.phone)} icon="phone" className="self-start">
              {t('manage.call', { house: labels.branchName })}
            </ButtonAnchor>
          </div>
        )}
      </div>
    </article>
  );
}
