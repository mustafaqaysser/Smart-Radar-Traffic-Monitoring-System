import type { Metadata } from 'next';
import { notFound } from 'next/navigation';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import { Icon } from '@/components/brand/icon';
import { SplitWords } from '@/components/motion/split-words';
import { BookingRows, HourCard } from '@/components/site/reserve/booking-pieces';
import { DepositResume } from '@/components/site/reserve/deposit-resume';
import { ButtonAnchor, ButtonLink } from '@/components/site/ui/button';
import { PageHeader } from '@/components/site/ui/page-header';
import { formatMoney, formatNumber } from '@/lib/i18n/format';
import { getBranch } from '@/lib/queries/branches';
import { paymentFor, resumablePayment, syncPayment } from '@/lib/server/payments';
import {
  DEPOSIT_WINDOW_MINUTES,
  confirmationPath,
  depositOpen,
  describeReservation,
  icsPath,
  reservationByCode,
  reservationCalendarUrl,
} from '@/lib/server/reservations';
import { googleDirectionsUrl, telUrl, whatsappUrl } from '@/lib/services/maps';
import { pageMetadata } from '@/lib/site/metadata';

type Params = Promise<{ locale: string; code: string }>;
type Search = Promise<Record<string, string | string[] | undefined>>;

const one = (v: string | string[] | undefined): string | null => (Array.isArray(v) ? (v[0] ?? null) : (v ?? null));

export async function generateMetadata({ params }: { params: Params }): Promise<Metadata> {
  const { locale, code } = await params;
  const t = await getTranslations({ locale, namespace: 'reserve.confirmed' });
  return pageMetadata({ locale, path: `/reserve/confirmed/${code}`, title: t('meta'), noindex: true });
}

export default async function ReservationConfirmedPage({ params, searchParams }: { params: Params; searchParams: Search }) {
  const { locale, code } = await params;
  setRequestLocale(locale);
  const sp = await searchParams;
  const token = one(sp.token);
  const [t, tc, tsun] = await Promise.all([getTranslations('reserve'), getTranslations('common'), getTranslations('visit.locations.sun')]);

  let r = await reservationByCode(code, token);
  if (!r) {
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

  // Back from the payment provider: settle the deposit now in case its webhook has not arrived yet.
  const paymentId = one(sp.payment);
  if (paymentId && r.depositStatus === 'pending') {
    const payment = await paymentFor(paymentId, r.id);
    if (payment) {
      await syncPayment(payment.id);
      r = (await reservationByCode(code, token)) ?? r;
    }
  }

  const branch = await getBranch(r.branchId);
  if (!branch) notFound();
  const labels = await describeReservation(r, branch, locale);
  const manageHref = `/reserve/manage/${r.code}?token=${encodeURIComponent(token ?? '')}`;
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
  const hour = (
    <HourCard
      at={r.startsAt}
      branch={branch}
      locale={locale}
      dateLabel={labels.dateLabel}
      timeLabel={labels.timeLabel}
      lightLabel={(phase) => t(`light.${phase}`)}
      sunLabels={{ sunrise: tsun('sunrise'), sunset: tsun('maghrib') }}
    />
  );

  // ——— Cancelled, or released because the deposit never arrived.
  if (r.status === 'cancelled' || r.status === 'no_show') {
    const lapsed = r.status === 'cancelled' && r.depositAmount > 0 && r.depositStatus !== 'paid' && !r.paymentId;
    return (
      <article className="site-grid gap-y-10 pt-10 pb-[var(--spacing-section)] lg:pt-16">
        <header className="col-span-full flex flex-col gap-6 lg:col-span-7">
          <p className="t-label text-muted">{t('confirmed.eyebrow', { code: r.code })}</p>
          <h1 className="t-display-lg">
            <SplitWords text={t(`manage.status.${r.status}`)} />
          </h1>
          <p className="t-body-lg measure">{lapsed ? t('confirmed.lapsed') : t('confirmed.cancelledBody')}</p>
          <ButtonLink href={{ pathname: '/reserve', query: { branch: branch.slug, party: String(r.partySize) } }} className="self-start">
            {t('offer.search')}
          </ButtonLink>
        </header>
        <BookingRows rows={rows} className="col-span-full lg:col-span-5 lg:col-start-8" />
      </article>
    );
  }

  // ——— Waiting for the deposit: the table is held while the guest pays.
  if (r.status === 'pending') {
    const payment = depositOpen(r, new Date())
      ? await resumablePayment({ purpose: 'deposit', referenceId: r.id, amount: r.depositAmount, description: `${branch.shortName.en} · ${r.code}`, email: r.email, locale, returnPath: confirmationPath(r, locale) })
      : null;
    return (
      <article className="site-grid gap-y-12 pt-10 pb-[var(--spacing-section)] lg:pt-16">
        <header className="col-span-full flex flex-col gap-6 lg:col-span-6">
          <p className="t-label text-muted">{t('confirmed.eyebrow', { code: r.code })}</p>
          <h1 className="t-display-lg">
            <SplitWords text={t('confirmed.pending')} />
          </h1>
          <p className="t-body-lg measure">{t('deposit.title', { amount: formatMoney(r.depositAmount, locale) })}</p>
          <p className="t-small text-muted measure">{t('deposit.later', { minutes: formatNumber(DEPOSIT_WINDOW_MINUTES, locale) })}</p>
          {payment ? (
            <div className="flex flex-col gap-4 border-t border-ink pt-6">
              <h2 className="t-heading-md">{t('confirmed.payNow')}</h2>
              <DepositResume payment={payment} />
            </div>
          ) : (
            <ButtonAnchor href={telUrl(branch.phone)} icon="phone" className="self-start">
              {t('manage.call', { house: labels.branchName })}
            </ButtonAnchor>
          )}
        </header>
        <div className="col-span-full flex flex-col gap-8 lg:col-span-5 lg:col-start-8">
          {hour}
          <BookingRows rows={rows} />
        </div>
      </article>
    );
  }

  // ——— Booked.
  const share = whatsappUrl('', t('confirmed.shareText', { house: labels.branchName, date: labels.dateLabel, time: labels.timeLabel, guests: labels.partyLabel }));
  return (
    <article className="site-grid gap-y-12 pt-10 pb-[var(--spacing-section)] lg:pt-16">
      <header className="col-span-full flex flex-col gap-6 lg:col-span-6">
        <p className="t-label flex items-center gap-2 text-muted">
          <Icon name="check" size={16} />
          {t('confirmed.eyebrow', { code: r.code })}
        </p>
        <h1 className="t-display-lg">
          <SplitWords text={t('confirmed.title')} />
        </h1>
        <p className="t-body-lg measure">{t('confirmed.body', { email: r.email })}</p>
        {r.depositStatus === 'paid' ? <p className="t-body">{t('confirmed.deposit', { amount: formatMoney(r.depositAmount, locale) })}</p> : null}
      </header>
      <aside className="col-span-full flex flex-col gap-10 lg:col-span-5 lg:col-start-8 lg:row-span-2">
        {hour}
        <section aria-labelledby="booking-actions" className="flex flex-col gap-6">
          <h2 id="booking-actions" className="t-heading-md">
            {t('confirmed.addCalendar')}
          </h2>
          <div className="flex flex-wrap gap-3">
            <ButtonAnchor href={reservationCalendarUrl(r, branch, labels)} target="_blank" size="sm" newWindowLabel={tc('a11y.newWindow')}>
              {t('confirmed.google')}
            </ButtonAnchor>
            <ButtonAnchor href={icsPath(r)} download={`zill-${r.code}.ics`} icon="download" size="sm">
              {t('confirmed.ics')}
            </ButtonAnchor>
          </div>
          <div className="flex flex-wrap gap-3 border-t border-line pt-6">
            <ButtonAnchor href={share} target="_blank" icon="chat" size="sm" newWindowLabel={tc('a11y.newWindow')}>
              {t('confirmed.share')}
            </ButtonAnchor>
            <ButtonAnchor href={googleDirectionsUrl({ lat: branch.lat, lng: branch.lng })} target="_blank" icon="pin" size="sm" newWindowLabel={tc('a11y.newWindow')}>
              {t('confirmed.directions')}
            </ButtonAnchor>
          </div>
        </section>
      </aside>

      <section aria-labelledby="booking-details" className="col-span-full flex flex-col gap-6 lg:col-span-6">
        <h2 id="booking-details" className="sr-only">
          {t('summary.title')}
        </h2>
        <BookingRows rows={rows} />
        <p className="t-small text-muted measure">{labels.branchAddress}</p>
        <div className="flex flex-col gap-3 pt-2">
          <ButtonLink href={manageHref} variant="secondary" className="self-start">
            {t('confirmed.manage')}
          </ButtonLink>
          <p className="t-small text-muted measure">{t('confirmed.policy', { hours: labels.cutoffLabel })}</p>
        </div>
      </section>
    </article>
  );
}
