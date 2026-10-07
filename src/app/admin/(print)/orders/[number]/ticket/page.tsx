import type { Metadata } from 'next';
import { notFound } from 'next/navigation';
import { getTranslations } from 'next-intl/server';
import restaurantConfig from '@config';
import { adminPage } from '@/lib/admin/context';
import { orderDetail } from '@/lib/admin/orders';
import { formatClock, formatDate, formatMoney, formatNumber } from '@/lib/i18n/format';
import { tr } from '@/lib/i18n/localized';
import { PrintOnLoad } from '@/components/admin/print-on-load';

export async function generateMetadata({ params }: { params: Promise<{ number: string }> }): Promise<Metadata> {
  const { number } = await params;
  const t = await getTranslations('admin.orders.ticket');
  return { title: t('title', { number }) };
}

/**
 * The 80 mm kitchen ticket: large number and channel, items with their changes and notes, the promise, how it
 * is paid, and — for delivery — where it goes. Printed from the board, the order page or the kitchen display.
 */
export default async function TicketPage({ params, searchParams }: { params: Promise<{ number: string }>; searchParams: Promise<{ print?: string }> }) {
  const [{ number }, { print }] = await Promise.all([params, searchParams]);
  const { locale, scope } = await adminPage('orders:view');
  const detail = await orderDetail(number.toUpperCase(), scope, locale);
  if (!detail) notFound();
  const { order, card } = detail;
  const t = await getTranslations('admin.orders');
  const branch = scope.branches.find((b) => b.id === order.branchId);
  const tz = branch?.timeZone ?? scope.timeZone;
  const now = new Date();
  const paid = order.paymentStatus === 'paid' || order.paymentStatus === 'authorized';
  const channel = order.channel === 'dine_in' && card.table ? t('channels.table', { table: card.table }) : t(`channels.${order.channel}`);

  return (
    <main className="min-h-dvh bg-surface py-8 print:bg-white print:py-0">
      <style>{`@page { size: 80mm auto; margin: 3mm 2mm; } @media print { html, body { width: 76mm; } }`}</style>
      <div className="mx-auto mb-4 flex w-[80mm] justify-end print:hidden">
        <PrintOnLoad auto={print === '1'} label={t('actions.print')} />
      </div>
      <article className="mx-auto w-[80mm] bg-white px-[4mm] py-[5mm] text-[12px] leading-snug text-black shadow-sm print:w-auto print:p-0 print:shadow-none">
        <header className="border-b border-dashed border-black pb-2 text-center">
          <p className="text-[13px] font-semibold">{tr(restaurantConfig.name, locale)}</p>
          {branch ? <p>{tr(branch.shortName, locale)}</p> : null}
        </header>
        <section className="border-b border-dashed border-black py-2">
          <div className="flex items-baseline justify-between gap-2">
            <p className="font-mono text-[26px] leading-none font-semibold" dir="ltr">
              {order.number}
            </p>
            <p className="text-[14px] font-semibold">{channel}</p>
          </div>
          <p className="mt-1.5">
            {t('ticket.placed', { time: formatClock(order.createdAt, locale, tz) })}
            {order.promisedAt ? ` · ${order.asap ? t('ticket.due', { time: formatClock(order.promisedAt, locale, tz) }) : t('ticket.scheduledFor', { time: formatClock(order.promisedAt, locale, tz), date: formatDate(order.promisedAt, locale, tz, { day: 'numeric', month: 'short' }) })}` : ''}
          </p>
        </section>
        <ul className="border-b border-dashed border-black py-2">
          {card.items.map((i) => (
            <li key={i.id} className="py-1">
              <p className="flex gap-2 text-[14px] font-semibold">
                <span className="w-7 shrink-0 tabular">{formatNumber(i.quantity, locale)}×</span>
                <span>{i.name}</span>
              </p>
              {i.modifiers.map((m) => (
                <p key={m} className="ps-9">
                  — {m}
                </p>
              ))}
              {i.notes ? (
                <p className="ps-9 font-semibold">
                  « <bdi>{i.notes}</bdi> »
                </p>
              ) : null}
            </li>
          ))}
        </ul>
        {order.notes ? (
          <section className="border-b border-dashed border-black py-2">
            <p className="font-semibold">{t('ticket.note')}</p>
            <p>
              <bdi>{order.notes}</bdi>
            </p>
          </section>
        ) : null}
        <section className="border-b border-dashed border-black py-2">
          <p>
            <bdi>{order.name}</bdi>
            {order.channel !== 'dine_in' && order.phone ? (
              <>
                {' · '}
                <span dir="ltr">{order.phone}</span>
              </>
            ) : null}
          </p>
          {card.address ? (
            <p className="mt-1">
              <bdi>{card.address}</bdi>
            </p>
          ) : null}
          {card.addressNotes ? (
            <p>
              <bdi>{card.addressNotes}</bdi>
            </p>
          ) : null}
        </section>
        <section className="flex items-baseline justify-between gap-2 py-2">
          <p className="text-[14px] font-semibold tabular">{formatMoney(order.total, locale)}</p>
          <p className="font-semibold">{paid ? t('ticket.paid') : order.paymentMethod === 'cash' ? t('ticket.collectCash') : order.paymentMethod === 'pay_at_venue' ? t('ticket.payAtHouse') : t('ticket.unpaid')}</p>
        </section>
        <p className="pt-1 text-center text-[10px]">{t('ticket.printed', { time: formatClock(now, locale, tz) })}</p>
      </article>
    </main>
  );
}
