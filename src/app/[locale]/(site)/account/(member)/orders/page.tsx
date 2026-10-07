import type { Metadata } from 'next';
import { notFound } from 'next/navigation';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import { AccountHeader } from '@/components/site/account/account-header';
import { ReorderButton } from '@/components/site/account/reorder-button';
import { ButtonLink } from '@/components/site/ui/button';
import { Link } from '@/i18n/navigation';
import { getCurrentUser } from '@/lib/auth/session';
import { formatDate, formatMoney } from '@/lib/i18n/format';
import { tr } from '@/lib/i18n/localized';
import { plural } from '@/lib/i18n/plural';
import { getBranches } from '@/lib/queries/branches';
import { accountOrders, orderItemCounts, orderToken } from '@/lib/server/account';
import { trackingPath } from '@/lib/server/orders';
import { featureEnabled } from '@/lib/server/settings';
import { pageMetadata } from '@/lib/site/metadata';
import { cn } from '@/lib/utils/cn';

type Params = Promise<{ locale: string }>;

export async function generateMetadata({ params }: { params: Params }): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: 'account.meta' });
  return pageMetadata({ locale, path: '/account/orders', title: t('orders'), noindex: true });
}

export default async function OrdersPage({ params }: { params: Params }) {
  const { locale } = await params;
  setRequestLocale(locale);
  if (!(await featureEnabled('ordering'))) notFound();
  const user = await getCurrentUser();
  if (!user) return null;
  const [t, to, tu, orders, branches] = await Promise.all([getTranslations('account'), getTranslations('order'), getTranslations('common.units'), accountOrders(user, 40), getBranches()]);
  const counts = await orderItemCounts(orders.map((o) => o.id));
  const branchOf = new Map(branches.map((b) => [b.id, b]));
  return (
    <>
      <AccountHeader title={t('orders.title')} intro={t('orders.intro')} />
      {orders.length ? (
        <ul className="border-t border-ink">
          {orders.map((o) => {
            const branch = branchOf.get(o.branchId);
            const tz = branch?.timeZone ?? 'Asia/Riyadh';
            const done = ['completed', 'rejected', 'cancelled'].includes(o.status);
            return (
              <li key={o.id} className="grid gap-3 border-b border-line py-6 md:grid-cols-12 md:items-baseline md:gap-6">
                <div className="flex flex-col gap-1 md:col-span-5">
                  <p className="t-heading-sm">
                    <bdi>{o.number}</bdi>
                    {branch ? <span className="text-muted"> · {tr(branch.shortName, locale)}</span> : null}
                  </p>
                  <p className="t-small text-muted">
                    {t('orders.placed', { date: formatDate(o.createdAt, locale, tz, { day: 'numeric', month: 'long', year: 'numeric' }) })} · {to(`channels.${o.channel}`)} · {tu('items', plural(counts.get(o.id) ?? 0, locale))}
                  </p>
                </div>
                <p className="t-body tabular md:col-span-2">
                  <bdi>{formatMoney(o.total, locale)}</bdi>
                </p>
                <p className={cn('t-label md:col-span-2', done ? 'text-muted' : 'text-accent', (o.status === 'rejected' || o.status === 'cancelled') && 'text-danger')}>{t(`status.order.${o.status}`)}</p>
                <div className="flex flex-wrap gap-x-5 md:col-span-3 md:justify-end">
                  <Link href={trackingPath(o)} className="t-small inline-flex min-h-11 items-center underline decoration-line underline-offset-4">
                    {done ? t('orders.receipt') : t('orders.track')}
                  </Link>
                  {o.status === 'completed' ? <ReorderButton number={o.number} token={orderToken(o)} /> : null}
                </div>
              </li>
            );
          })}
        </ul>
      ) : (
        <div className="flex flex-col items-start gap-5 border-t border-ink pt-6">
          <p className="t-body text-muted">{t('orders.none')}</p>
          <ButtonLink href="/order">{t('orders.start')}</ButtonLink>
        </div>
      )}
    </>
  );
}
