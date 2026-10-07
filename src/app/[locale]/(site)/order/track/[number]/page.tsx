import type { Metadata } from 'next';
import { notFound } from 'next/navigation';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import { OrderTracker } from '@/components/site/order/order-tracker';
import { PaymentResume } from '@/components/site/payment-resume';
import { BookingRows } from '@/components/site/reserve/booking-pieces';
import { ButtonAnchor, ButtonLink } from '@/components/site/ui/button';
import { PageHeader } from '@/components/site/ui/page-header';
import { formatDateTime, formatMoney, formatNumber, joinParts } from '@/lib/i18n/format';
import { tr } from '@/lib/i18n/localized';
import { getBranch } from '@/lib/queries/branches';
import { orderByNumber, orderItems, PAYMENT_WINDOW_MINUTES, totalsRows, trackingPath, trackingSnapshot } from '@/lib/server/orders';
import { paymentFor, resumablePayment, syncPayment } from '@/lib/server/payments';
import { googleDirectionsUrl, telUrl } from '@/lib/services/maps';
import { pageMetadata } from '@/lib/site/metadata';

type Params = Promise<{ locale: string; number: string }>;
type Search = Promise<Record<string, string | string[] | undefined>>;

const one = (v: string | string[] | undefined): string | null => (Array.isArray(v) ? (v[0] ?? null) : (v ?? null));

export async function generateMetadata({ params }: { params: Params }): Promise<Metadata> {
  const { locale, number } = await params;
  const t = await getTranslations({ locale, namespace: 'order.track' });
  return pageMetadata({ locale, path: `/order/track/${number}`, title: t('meta', { number }), noindex: true });
}

export default async function TrackOrderPage({ params, searchParams }: { params: Params; searchParams: Search }) {
  const { locale, number } = await params;
  setRequestLocale(locale);
  const sp = await searchParams;
  const token = one(sp.token);
  const [t, to, tc] = await Promise.all([getTranslations('order.track'), getTranslations('order'), getTranslations('common')]);
  let order = await orderByNumber(number, token);
  if (!order || !token) {
    return (
      <PageHeader eyebrow={to('eyebrow')} title={t('meta', { number })} intro={<p>{t('invalid')}</p>}>
        <ButtonLink href="/order" className="self-start">
          {to('basket.browse')}
        </ButtonLink>
      </PageHeader>
    );
  }

  // Back from the payment provider: settle the payment now in case its webhook is late.
  const paymentId = one(sp.payment);
  if (paymentId && order.status === 'pending_payment') {
    const payment = await paymentFor(paymentId, order.id);
    if (payment) {
      await syncPayment(payment.id);
      order = (await orderByNumber(number, token)) ?? order;
    }
  }

  const branch = await getBranch(order.branchId);
  if (!branch) notFound();
  const [items, totals, snapshot] = await Promise.all([orderItems(order.id), totalsRows(order, locale), trackingSnapshot(order)]);
  const now = new Date();
  const payable = order.status === 'pending_payment' && now.getTime() - order.createdAt.getTime() < PAYMENT_WINDOW_MINUTES * 60_000;
  const payment = payable
    ? await resumablePayment({ purpose: 'order', referenceId: order.id, amount: order.total - order.giftCardAmount, description: `${branch.shortName.en} · ${order.number}`, email: order.email, locale, returnPath: `/${locale}${trackingPath(order)}` })
    : null;
  const lapsed = order.status === 'cancelled' && order.paymentMethod === 'card' && !order.acceptedAt && order.paymentStatus !== 'paid';
  const address = order.address ? joinParts([order.address.area, order.address.street, order.address.building, order.address.floor], locale) : null;

  return (
    <article className="site-grid gap-y-12 pt-10 pb-[var(--spacing-section)] lg:pt-16">
      <div className="col-span-full flex flex-col gap-6 lg:col-span-7">
        <p className="t-label text-muted">
          {t('eyebrow', { number: order.number })} · {t('placedAt', { time: formatDateTime(order.createdAt, locale, branch.timeZone) })}
        </p>
        <OrderTracker number={order.number} token={token} channel={order.channel} timeZone={branch.timeZone} initial={snapshot} canReorder={order.channel !== 'dine_in'} />
        {order.status === 'rejected' && order.rejectReason ? <p className="t-body">{t('reason', { reason: order.rejectReason })}</p> : null}
        {lapsed ? <p className="t-body">{t('lapsed')}</p> : null}
        {(order.status === 'rejected' || order.status === 'cancelled') && order.paymentStatus === 'refunded' ? <p className="t-body">{t('refunded')}</p> : null}
        {payment ? (
          <section aria-labelledby="pay-now" className="flex flex-col gap-4 border-t border-ink pt-6">
            <h2 id="pay-now" className="t-heading-md">
              {t('payNow')}
            </h2>
            <PaymentResume payment={payment} />
          </section>
        ) : null}
      </div>

      <aside className="col-span-full flex flex-col gap-8 lg:col-span-4 lg:col-start-9">
        <section aria-labelledby="order-details" className="flex flex-col gap-4">
          <h2 id="order-details" className="t-heading-md">
            {t('details')}
          </h2>
          <ul className="flex flex-col border-t border-ink">
            {items.map((i) => (
              <li key={i.id} className="flex flex-col gap-1 border-b border-line py-3">
                <div className="flex items-baseline justify-between gap-3">
                  <span className="t-body">
                    <bdi className="tabular">{formatNumber(i.quantity, locale)}</bdi> × {tr(i.name, locale)}
                  </span>
                  <span className="t-small tabular">
                    <bdi>{formatMoney(i.lineTotal, locale)}</bdi>
                  </span>
                </div>
                {i.modifiers.length || i.notes ? <span className="t-small text-muted">{[...i.modifiers.map((m) => tr(m.name, locale)), i.notes ? `“${i.notes}”` : null].filter(Boolean).join(' · ')}</span> : null}
              </li>
            ))}
          </ul>
          <BookingRows rows={totals.map((r) => ({ label: r.label, value: <bdi>{r.value}</bdi>, strong: r.strong }))} className="border-t-0" />
          <p className="t-small text-muted">{to(`payment.${order.paymentMethod}`)}</p>
        </section>

        <section aria-labelledby="order-where" className="flex flex-col gap-3">
          <h2 id="order-where" className="t-label text-muted">
            {order.channel === 'delivery' ? t('deliverTo') : t('collectFrom')}
          </h2>
          <p className="t-body">{order.channel === 'delivery' ? address : `${tr(branch.name, locale)} — ${tr(branch.address, locale)}`}</p>
          <div className="flex flex-wrap gap-3">
            {order.channel !== 'delivery' ? (
              <ButtonAnchor href={googleDirectionsUrl({ lat: branch.lat, lng: branch.lng })} target="_blank" icon="pin" size="sm" newWindowLabel={tc('a11y.newWindow')}>
                {tc('actions.directions')}
              </ButtonAnchor>
            ) : null}
            <ButtonAnchor href={telUrl(branch.phone)} icon="phone" size="sm">
              {t('help', { house: tr(branch.shortName, locale) })}
            </ButtonAnchor>
          </div>
        </section>
      </aside>
    </article>
  );
}
