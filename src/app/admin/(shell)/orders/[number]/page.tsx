import type { Metadata } from 'next';
import Link from 'next/link';
import { notFound } from 'next/navigation';
import { getTranslations } from 'next-intl/server';
import { ArrowLeft, ExternalLink, Mail, MessageCircle, Phone, Printer } from 'lucide-react';
import { adminPage } from '@/lib/admin/context';
import { orderDetail } from '@/lib/admin/orders';
import { STATUS_TONE } from '@/lib/admin/order-flow';
import { can } from '@/lib/auth/permissions';
import { formatDateTime, formatMoney, formatNumber } from '@/lib/i18n/format';
import { totalsRows, trackingUrl } from '@/lib/server/orders';
import { CopyButton } from '@/components/admin/copy-button';
import { OrderActions } from '@/components/admin/orders/order-actions';
import { ChannelBadge } from '@/components/admin/orders/order-card';
import { PaymentBadge } from '@/components/admin/orders/payment-badge';
import { LiveRefresh } from '@/components/admin/shell/live';
import { Badge } from '@/components/admin/ui/badge';
import { Button } from '@/components/admin/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/admin/ui/card';
import { DetailList, PageHeader } from '@/components/admin/ui/page-header';

export async function generateMetadata({ params }: { params: Promise<{ number: string }> }): Promise<Metadata> {
  const { number } = await params;
  const t = await getTranslations('admin.orders.detail');
  return { title: t('title', { number }) };
}

export default async function OrderPage({ params }: { params: Promise<{ number: string }> }) {
  const { number } = await params;
  const { user, locale, scope } = await adminPage('orders:view');
  const detail = await orderDetail(number.toUpperCase(), scope, locale);
  if (!detail) notFound();
  const { order, card, events } = detail;
  const [t, ts, totals] = await Promise.all([getTranslations('admin.orders'), getTranslations('admin.orders.status'), totalsRows(order, locale)]);
  const branch = scope.branches.find((b) => b.id === order.branchId);
  const tz = branch?.timeZone ?? scope.timeZone;
  const phoneDigits = order.phone.replace(/[^\d+]/g, '');
  const mapsHref = order.address?.lat && order.address.lng ? `https://www.google.com/maps/search/?api=1&query=${order.address.lat},${order.address.lng}` : card.address ? `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(card.address)}` : null;

  return (
    <div className="flex flex-col gap-5">
      <LiveRefresh topics={['orders']} />
      <PageHeader
        eyebrow={
          <Link href="/admin/orders" className="inline-flex items-center gap-1 text-muted no-underline hover-capable:hover:text-ink">
            <ArrowLeft className="size-3.5 rtl:-scale-x-100" aria-hidden="true" />
            {t('title')}
          </Link>
        }
        title={
          <span className="flex flex-wrap items-center gap-3">
            <span>
              {t('detail.titleShort')}{' '}
              <span className="font-mono" dir="ltr">
                {order.number}
              </span>
            </span>
            <Badge tone={STATUS_TONE[order.status]}>{ts(order.status)}</Badge>
          </span>
        }
        description={t('detail.placed', { when: formatDateTime(order.createdAt, locale, tz), house: card.branchName })}
        actions={
          <>
            <Button asChild>
              <a href={`/admin/orders/${order.number}/ticket?print=1`} target="_blank" rel="noopener">
                <Printer aria-hidden="true" />
                {t('actions.print')}
              </a>
            </Button>
            <OrderActions order={card} canManage={can(user.role, 'orders:manage')} size="md" showOpen={false} />
          </>
        }
      />

      <div className="grid gap-5 lg:grid-cols-[minmax(0,2fr)_minmax(0,1fr)]">
        <div className="flex min-w-0 flex-col gap-5">
          <Card>
            <CardHeader>
              <CardTitle>{t('detail.items', { n: formatNumber(card.itemCount, locale) })}</CardTitle>
              <ChannelBadge channel={order.channel} table={card.table} />
            </CardHeader>
            <ul className="divide-y divide-line">
              {card.items.map((i) => (
                <li key={i.id} className="flex gap-3 px-5 py-3 text-[0.875rem]">
                  <span className="w-7 shrink-0 font-semibold tabular">{formatNumber(i.quantity, locale)}×</span>
                  <span className="min-w-0 flex-1">
                    <span className="font-medium">{i.name}</span>
                    {i.modifiers.length ? <span className="block text-xs text-muted">{i.modifiers.join(locale === 'ar' ? '، ' : ', ')}</span> : null}
                    {i.notes ? (
                      <span className="block text-xs text-warning">
                        “<bdi>{i.notes}</bdi>”
                      </span>
                    ) : null}
                  </span>
                  <span className="shrink-0 tabular">{formatMoney(i.lineTotal, locale)}</span>
                </li>
              ))}
            </ul>
            <CardContent className="border-t border-line">
              <dl className="ms-auto flex max-w-sm flex-col gap-1.5 text-[0.875rem]">
                {totals.map((row) => (
                  <div key={row.label} className={row.strong ? 'flex justify-between gap-4 font-semibold' : 'flex justify-between gap-4 text-muted'}>
                    <dt>{row.label}</dt>
                    <dd className="tabular">{row.value}</dd>
                  </div>
                ))}
              </dl>
            </CardContent>
          </Card>

          {order.notes ? (
            <Card>
              <CardHeader>
                <CardTitle>{t('detail.notes')}</CardTitle>
              </CardHeader>
              <CardContent>
                <p className="text-[0.875rem]">
                  <bdi>{order.notes}</bdi>
                </p>
              </CardContent>
            </Card>
          ) : null}

          <Card>
            <CardHeader>
              <CardTitle>{t('detail.timeline')}</CardTitle>
            </CardHeader>
            <ol className="flex flex-col gap-0 px-5 py-4">
              {events.map((e, i) => (
                <li key={`${e.status}-${e.at}`} className="relative flex gap-3 pb-4 last:pb-0">
                  <span aria-hidden="true" className="relative flex w-3 justify-center">
                    <span className={`mt-1.5 size-2.5 rounded-full ${i === events.length - 1 ? 'bg-accent' : 'bg-field'}`} />
                    {i < events.length - 1 ? <span className="absolute top-4 bottom-[-0.25rem] w-px bg-line" /> : null}
                  </span>
                  <span className="min-w-0 text-[0.875rem]">
                    <span className="font-medium">{ts(e.status)}</span>
                    <span className="ms-2 text-xs text-muted">{formatDateTime(new Date(e.at), locale, tz)}</span>
                    {e.actor ? <span className="block text-xs text-muted">{t('detail.by', { name: e.actor })}</span> : null}
                    {e.note ? (
                      <span className="block text-xs">
                        <bdi>{e.note}</bdi>
                      </span>
                    ) : null}
                  </span>
                </li>
              ))}
            </ol>
          </Card>
        </div>

        <div className="flex min-w-0 flex-col gap-5">
          <Card>
            <CardHeader>
              <CardTitle>{t('detail.guest')}</CardTitle>
              {detail.customerId && can(user.role, 'customers:view') ? (
                <Link href={`/admin/customers/${detail.customerId}`} className="text-[0.8125rem] text-link">
                  {t('detail.profile')}
                </Link>
              ) : null}
            </CardHeader>
            <CardContent className="flex flex-col gap-3">
              <p className="font-medium">
                <bdi>{order.name}</bdi>
              </p>
              <div className="flex flex-wrap gap-2">
                {order.phone ? (
                  <>
                    <Button asChild size="sm">
                      <a href={`tel:${phoneDigits}`} dir="ltr">
                        <Phone aria-hidden="true" />
                        {order.phone}
                      </a>
                    </Button>
                    <Button asChild size="sm" variant="ghost">
                      <a href={`https://wa.me/${phoneDigits.replace('+', '')}`} target="_blank" rel="noopener noreferrer" aria-label={t('detail.whatsapp')}>
                        <MessageCircle aria-hidden="true" />
                      </a>
                    </Button>
                  </>
                ) : null}
                {order.email ? (
                  <Button asChild size="sm" variant="ghost">
                    <a href={`mailto:${order.email}`} dir="ltr">
                      <Mail aria-hidden="true" />
                      {order.email}
                    </a>
                  </Button>
                ) : null}
              </div>
              <p className="text-xs text-muted">{t('detail.language', { language: t(`detail.languages.${order.locale === 'en' ? 'en' : 'ar'}`) })}</p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>{t(`detail.fulfilment.${order.channel}`)}</CardTitle>
            </CardHeader>
            <CardContent>
              <DetailList
                items={[
                  ...(card.table ? [{ label: t('detail.table'), value: card.table }] : []),
                  ...(card.address ? [{ label: t('detail.address'), value: <bdi>{card.address}</bdi> }] : []),
                  ...(card.addressNotes ? [{ label: t('detail.directions'), value: <bdi>{card.addressNotes}</bdi> }] : []),
                  { label: order.asap ? t('detail.promised') : t('detail.scheduled'), value: order.promisedAt ? formatDateTime(order.promisedAt, locale, tz) : '—' },
                  ...(order.prepMinutes ? [{ label: t('detail.prep'), value: t('detail.minutes', { n: formatNumber(order.prepMinutes, locale) }) }] : []),
                ]}
              />
              {mapsHref ? (
                <a href={mapsHref} target="_blank" rel="noopener noreferrer" className="mt-3 inline-flex items-center gap-1 text-[0.8125rem] text-link">
                  {t('detail.map')}
                  <ExternalLink className="size-3.5" aria-hidden="true" />
                </a>
              ) : null}
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>{t('detail.payment')}</CardTitle>
              <PaymentBadge order={card} />
            </CardHeader>
            <CardContent className="flex flex-col gap-3">
              <DetailList
                items={[
                  { label: t('detail.method'), value: t(`methods.${order.paymentMethod}`) },
                  ...(order.promoCode ? [{ label: t('detail.promo'), value: <span className="font-mono">{order.promoCode}</span> }] : []),
                  ...(order.loyaltyPointsRedeemed ? [{ label: t('detail.pointsRedeemed'), value: formatNumber(order.loyaltyPointsRedeemed, locale) }] : []),
                  ...(order.loyaltyPointsEarned ? [{ label: t('detail.pointsEarned'), value: formatNumber(order.loyaltyPointsEarned, locale) }] : []),
                  ...(order.rejectReason ? [{ label: t('detail.reason'), value: <bdi>{order.rejectReason}</bdi> }] : []),
                ]}
              />
              {order.email ? <CopyButton value={trackingUrl(order)} label={t('detail.copyTracking')} copiedLabel={t('detail.copied')} /> : null}
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}
