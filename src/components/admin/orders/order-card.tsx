'use client';

import Link from 'next/link';
import { Clock, MapPin, ShoppingBag, Truck, Utensils } from 'lucide-react';
import { useLocale, useTranslations } from 'next-intl';
import type { OrderCard as Card } from '@/lib/admin/orders';
import { formatClock, formatMoney, formatNumber } from '@/lib/i18n/format';
import { cn } from '@/lib/utils/cn';
import { useMinuteClock } from '../shell/relative-time';
import { Badge } from '../ui/badge';
import { OrderActions } from './order-actions';

const CHANNEL_ICONS = { delivery: Truck, pickup: ShoppingBag, dine_in: Utensils } as const;

/** How the order is paid, in the words the floor uses: paid, cash to collect, to pay at the table… */
export function usePaymentLabel() {
  const t = useTranslations('admin.orders.payment');
  return (o: Pick<Card, 'paymentMethod' | 'paymentStatus' | 'channel'>) => {
    if (o.paymentStatus === 'refunded') return { text: t('refunded'), tone: 'warning' as const };
    if (o.paymentStatus === 'failed') return { text: t('failed'), tone: 'danger' as const };
    if (o.paymentStatus === 'paid' || o.paymentStatus === 'authorized') return { text: t(`paid.${o.paymentMethod}`), tone: 'success' as const };
    if (o.paymentMethod === 'cash') return { text: t('cash'), tone: 'warning' as const };
    if (o.paymentMethod === 'pay_at_venue') return { text: o.channel === 'dine_in' ? t('atTable') : t('atHouse'), tone: 'warning' as const };
    return { text: t('pending'), tone: 'neutral' as const };
  };
}

export function ChannelBadge({ channel, table }: { channel: Card['channel']; table: string | null }) {
  const t = useTranslations('admin.orders.channels');
  const Icon = CHANNEL_ICONS[channel];
  return (
    <Badge tone={channel === 'dine_in' ? 'sun' : 'outline'}>
      <Icon aria-hidden="true" />
      {channel === 'dine_in' && table ? t('table', { table }) : t(channel)}
    </Badge>
  );
}

/** Minutes since the order came in, and whether its promise is slipping. */
function Timing({ order, timeZone }: { order: Card; timeZone: string }) {
  const t = useTranslations('admin.orders.card');
  const locale = useLocale();
  const now = useMinuteClock();
  if (!now) return null;
  const age = Math.max(0, Math.round((now - new Date(order.createdAt).getTime()) / 60000));
  const promised = order.promisedAt ? new Date(order.promisedAt) : null;
  const left = promised ? Math.round((promised.getTime() - now) / 60000) : null;
  const handedOver = order.status === 'out_for_delivery' || (order.status === 'ready' && order.channel !== 'delivery');
  const late = left !== null && left < 0 && !handedOver;
  return (
    <p className="flex flex-wrap items-center gap-x-3 gap-y-0.5 text-xs text-muted">
      <span className="inline-flex items-center gap-1">
        <Clock className="size-3.5" aria-hidden="true" />
        {t('age', { n: formatNumber(age, locale) })}
      </span>
      {promised ? (
        <span className={cn(late && 'font-medium text-danger')}>
          {order.asap ? '' : `${t('scheduled')} · `}
          {handedOver || left === null
            ? t('promised', { time: formatClock(promised, locale, timeZone) })
            : late
              ? t('late', { time: formatClock(promised, locale, timeZone), n: formatNumber(-left, locale) })
              : t('due', { time: formatClock(promised, locale, timeZone), n: formatNumber(Math.max(0, left), locale) })}
        </span>
      ) : null}
    </p>
  );
}

export function OrderCardView({ order, canManage, fresh, showBranch, timeZone }: { order: Card; canManage: boolean; fresh: boolean; showBranch: boolean; timeZone: string }) {
  const t = useTranslations('admin.orders');
  const locale = useLocale();
  const payment = usePaymentLabel()(order);
  return (
    <article
      aria-labelledby={`order-${order.id}`}
      className={cn('flex flex-col gap-2.5 rounded-soft border bg-raised p-3.5 transition-shadow', fresh ? 'border-accent shadow-[0_0_0_3px_color-mix(in_oklab,var(--c-accent)_22%,transparent)] animate-[ticket-in_var(--dur-calm)_var(--ease-shade)]' : 'border-line')}
    >
      <header className="flex items-start justify-between gap-2">
        <div className="min-w-0">
          <h3 id={`order-${order.id}`} className="flex flex-wrap items-center gap-2 text-[0.9375rem] font-semibold">
            <Link href={`/admin/orders/${order.number}`} className="font-mono no-underline hover-capable:hover:underline" dir="ltr">
              {order.number}
            </Link>
            <ChannelBadge channel={order.channel} table={order.table} />
            {fresh ? <Badge tone="accent">{t('card.new')}</Badge> : null}
          </h3>
          <p className="mt-0.5 truncate text-[0.8125rem]">
            <bdi>{order.name}</bdi>
            {showBranch ? <span className="text-muted"> · {order.branchName}</span> : null}
          </p>
        </div>
        <p className="shrink-0 text-end text-[0.8125rem] font-medium tabular">{formatMoney(order.total, locale)}</p>
      </header>
      <Timing order={order} timeZone={timeZone} />
      <ul className="flex flex-col gap-1 border-t border-line pt-2 text-[0.8125rem]">
        {order.items.map((i) => (
          <li key={i.id} className="flex gap-2">
            <span className="w-6 shrink-0 font-semibold tabular">{formatNumber(i.quantity, locale)}×</span>
            <span className="min-w-0">
              <span>{i.name}</span>
              {i.modifiers.length ? <span className="block text-xs text-muted">{i.modifiers.join(locale === 'ar' ? '، ' : ', ')}</span> : null}
              {i.notes ? (
                <span className="block text-xs text-warning">
                  “<bdi>{i.notes}</bdi>”
                </span>
              ) : null}
            </span>
          </li>
        ))}
      </ul>
      {order.notes ? (
        <p className="rounded-hair border-s-2 border-sun bg-sun/10 px-2 py-1 text-xs">
          <bdi>{order.notes}</bdi>
        </p>
      ) : null}
      {order.channel === 'delivery' && order.address ? (
        <p className="flex items-start gap-1.5 text-xs text-muted">
          <MapPin className="mt-0.5 size-3.5" aria-hidden="true" />
          <bdi>{order.address}</bdi>
        </p>
      ) : null}
      <div className="flex flex-wrap items-center justify-between gap-2 border-t border-line pt-2.5">
        <Badge tone={payment.tone}>{payment.text}</Badge>
        <OrderActions order={order} canManage={canManage} />
      </div>
    </article>
  );
}
