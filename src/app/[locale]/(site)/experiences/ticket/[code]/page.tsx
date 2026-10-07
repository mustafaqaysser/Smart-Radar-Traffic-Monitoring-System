import type { Metadata } from 'next';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import { SplitWords } from '@/components/motion/split-words';
import { PaymentResume } from '@/components/site/payment-resume';
import { BookingRows } from '@/components/site/reserve/booking-pieces';
import { ButtonAnchor, ButtonLink } from '@/components/site/ui/button';
import { PageHeader } from '@/components/site/ui/page-header';
import { googleCalendarUrl } from '@/lib/domain/calendar';
import { formatClock } from '@/lib/i18n/format';
import { tr } from '@/lib/i18n/localized';
import { bookingByCode, ticketDetails, ticketIcsPath, ticketPath, ticketPaymentOpen, ticketQrUrl } from '@/lib/server/events';
import { paymentFor, resumablePayment, syncPayment } from '@/lib/server/payments';
import { googleDirectionsUrl } from '@/lib/services/maps';
import { pageMetadata } from '@/lib/site/metadata';

type Params = Promise<{ locale: string; code: string }>;
type Search = Promise<Record<string, string | string[] | undefined>>;
const one = (v: string | string[] | undefined): string | null => (Array.isArray(v) ? (v[0] ?? null) : (v ?? null));

export async function generateMetadata({ params }: { params: Params }): Promise<Metadata> {
  const { locale, code } = await params;
  const t = await getTranslations({ locale, namespace: 'gather.experiences.ticket' });
  return pageMetadata({ locale, path: `/experiences/ticket/${code}`, title: t('meta'), noindex: true });
}

export default async function TicketPage({ params, searchParams }: { params: Params; searchParams: Search }) {
  const { locale, code } = await params;
  setRequestLocale(locale);
  const sp = await searchParams;
  const token = one(sp.token);
  const [t, tc, tr2] = await Promise.all([getTranslations('gather.experiences'), getTranslations('common'), getTranslations('reserve.confirmed')]);
  let booking = await bookingByCode(code, token);
  if (!booking) {
    return (
      <PageHeader eyebrow={t('eyebrow')} title={t('ticket.meta')} intro={<p>{t('ticket.invalid')}</p>}>
        <ButtonLink href="/experiences" className="self-start">
          {t('back')}
        </ButtonLink>
      </PageHeader>
    );
  }
  const paymentId = one(sp.payment);
  if (paymentId && booking.status === 'pending_payment') {
    const payment = await paymentFor(paymentId, booking.id);
    if (payment) {
      await syncPayment(payment.id);
      booking = (await bookingByCode(code, token)) ?? booking;
    }
  }
  const d = await ticketDetails(booking, locale);
  if (!d) {
    return <PageHeader eyebrow={t('eyebrow')} title={t('ticket.meta')} intro={<p>{t('ticket.invalid')}</p>} />;
  }
  const open = ticketPaymentOpen(booking, new Date());
  const payment = open ? await resumablePayment({ purpose: 'event', referenceId: booking.id, amount: booking.total, description: `${tr(d.event.title, 'en')} · ${booking.code}`, email: booking.email, locale, returnPath: `/${locale}${ticketPath(booking)}` }) : null;
  const active = booking.status === 'confirmed' || booking.status === 'checked_in';
  const title = booking.status === 'pending_payment' ? t('ticket.pending') : booking.status === 'cancelled' ? t('ticket.cancelled') : t('ticket.title');

  return (
    <article className="site-grid gap-y-12 pt-10 pb-[var(--spacing-section)] lg:pt-16">
      <header className="col-span-full flex flex-col gap-6 lg:col-span-6">
        <p className="t-label text-muted">{d.title}</p>
        <h1 className="t-display-lg">
          <SplitWords text={title} />
        </h1>
        {active ? <p className="t-body-lg measure">{t('ticket.body', { email: booking.email })}</p> : null}
        {booking.status === 'cancelled' ? <p className="t-body-lg measure">{t('ticket.lapsed')}</p> : null}
        {booking.status === 'checked_in' && booking.checkedInAt ? <p className="t-body">{t('ticket.checkedIn', { time: formatClock(booking.checkedInAt, locale, d.branch.timeZone) })}</p> : null}
        <BookingRows
          rows={[
            { label: t('when'), value: `${d.dateLabel} · ${d.timeLabel}` },
            { label: t('where'), value: d.branchName },
            { label: t('tickets'), value: `${d.ticketLabel} × ${d.quantityLabel}` },
            { label: t('total'), value: <bdi>{d.totalLabel}</bdi> },
            { label: t('ticket.code'), value: <bdi>{booking.code}</bdi>, strong: true },
          ]}
        />
        {active ? (
          <div className="flex flex-wrap gap-3">
            <ButtonAnchor href={googleCalendarUrl({ title: d.title, start: d.event.startsAt, end: d.event.endsAt, location: tr(d.branch.address, locale), details: booking.code })} target="_blank" size="sm" newWindowLabel={tc('a11y.newWindow')}>
              {tr2('google')}
            </ButtonAnchor>
            <ButtonAnchor href={ticketIcsPath(booking)} download={`zill-${booking.code}.ics`} icon="download" size="sm">
              {tr2('ics')}
            </ButtonAnchor>
            <ButtonAnchor href={googleDirectionsUrl({ lat: d.branch.lat, lng: d.branch.lng })} target="_blank" icon="pin" size="sm" newWindowLabel={tc('a11y.newWindow')}>
              {tr2('directions')}
            </ButtonAnchor>
          </div>
        ) : null}
        {payment ? (
          <section aria-labelledby="pay-ticket" className="flex flex-col gap-4 border-t border-ink pt-6">
            <h2 id="pay-ticket" className="t-heading-md">
              {t('ticket.payNow')}
            </h2>
            <PaymentResume payment={payment} />
          </section>
        ) : null}
      </header>
      {active ? (
        <figure className="col-span-full flex flex-col items-center gap-4 self-start bg-raised p-8 lg:col-span-4 lg:col-start-9">
          {/* eslint-disable-next-line @next/next/no-img-element -- generated per ticket by our own route */}
          <img src={ticketQrUrl(booking)} alt={t('ticket.qrAlt', { code: booking.code })} width={360} height={360} className="w-full max-w-72" />
          <figcaption className="t-heading-md tabular" dir="ltr">
            {booking.code}
          </figcaption>
        </figure>
      ) : null}
    </article>
  );
}
