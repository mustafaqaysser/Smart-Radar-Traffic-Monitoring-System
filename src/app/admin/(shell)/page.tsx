import type { Metadata } from 'next';
import Link from 'next/link';
import { getTranslations } from 'next-intl/server';
import { CalendarPlus, ChefHat, HandPlatter, ReceiptText } from 'lucide-react';
import { adminPage } from '@/lib/admin/context';
import { dashboardData, recentActivity, type DayFigures } from '@/lib/admin/dashboard';
import { can } from '@/lib/auth/permissions';
import { formatDate, formatHijri, formatMoney, formatNumber, formatWallTime, formatWeekday } from '@/lib/i18n/format';
import { tr } from '@/lib/i18n/localized';
import { plural } from '@/lib/i18n/plural';
import { weekdayOf } from '@/lib/time/zoned';
import { HourlyChart, SalesChart, ShareBars, VisitorsChart } from '@/components/admin/charts';
import { ActivityFeed } from '@/components/admin/dashboard/activity-feed';
import { LiveRefresh } from '@/components/admin/shell/live';
import { Badge } from '@/components/admin/ui/badge';
import { Button } from '@/components/admin/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/admin/ui/card';
import { EmptyState, PageHeader, Stat } from '@/components/admin/ui/page-header';

export async function generateMetadata(): Promise<Metadata> {
  const t = await getTranslations('admin.dashboard');
  return { title: t('title') };
}

function change(now: number, before: number): number | null {
  if (!before) return null;
  return (now - before) / before;
}

export default async function DashboardPage() {
  const { user, locale, scope } = await adminPage('dashboard:view');
  const now = new Date();
  const [t, ts, tu, data, activity] = await Promise.all([getTranslations('admin.dashboard'), getTranslations('admin.reservations.status'), getTranslations('common.units'), dashboardData(scope, now), recentActivity(scope)]);
  const money = can(user.role, 'reports:view');
  const lastWeekday = formatWeekday(weekdayOf(data.today), locale);
  const compare = (key: keyof DayFigures, lowerIsBetter = false) => {
    const c = change(data.figures[key], data.lastWeek[key]);
    if (c === null) return { note: data.figures[key] ? t('compare.none', { day: lastWeekday }) : t('compare.same', { day: lastWeekday }), tone: 'flat' as const };
    const pct = formatNumber(c, locale, { style: 'percent', signDisplay: 'exceptZero', maximumFractionDigits: 0 });
    const better = lowerIsBetter ? c < 0 : c > 0;
    return { note: t('compare.vs', { change: pct, day: lastWeekday }), tone: c === 0 ? ('flat' as const) : better ? ('up' as const) : ('down' as const) };
  };
  const stats: { key: keyof DayFigures; value: string; lowerIsBetter?: boolean; money?: boolean }[] = [
    { key: 'covers', value: formatNumber(data.figures.covers, locale) },
    { key: 'reservations', value: formatNumber(data.figures.reservations, locale) },
    { key: 'orders', value: formatNumber(data.figures.orders, locale) },
    { key: 'revenue', value: formatMoney(data.figures.revenue, locale), money: true },
    { key: 'averageOrder', value: formatMoney(data.figures.averageOrder, locale), money: true },
    { key: 'noShows', value: formatNumber(data.figures.noShows, locale), lowerIsBetter: true },
  ];
  const branchName = (id: string) => tr(scope.branches.find((b) => b.id === id)?.shortName, locale);
  const kitchenTotal = Object.values(data.kitchen).reduce((a, b) => a + (b ?? 0), 0);

  return (
    <div className="flex flex-col gap-6">
      <LiveRefresh topics={['orders', 'reservations', 'requests']} />
      <PageHeader
        eyebrow={
          <>
            {formatDate(now, locale, scope.timeZone, { weekday: 'long', day: 'numeric', month: 'long' })} · {formatHijri(now, locale, scope.timeZone)}
          </>
        }
        title={scope.branch ? t('titleAt', { name: tr(scope.branch.shortName, locale) }) : t('title')}
        description={t('intro', { day: lastWeekday })}
        actions={
          <>
            {can(user.role, 'reservations:manage') ? (
              <Button asChild>
                <Link href="/admin/reservations/new">
                  <CalendarPlus aria-hidden="true" />
                  {t('actions.newBooking')}
                </Link>
              </Button>
            ) : null}
            {can(user.role, 'orders:view') ? (
              <Button asChild variant="primary">
                <Link href="/admin/orders">
                  <ReceiptText aria-hidden="true" />
                  {t('actions.orders')}
                </Link>
              </Button>
            ) : null}
          </>
        }
      />

      <section aria-label={t('figures')} className="grid grid-cols-2 gap-3 md:grid-cols-3 xl:grid-cols-6">
        {stats
          .filter((s) => money || !s.money)
          .map((s) => {
            const c = compare(s.key, s.lowerIsBetter);
            return <Stat key={s.key} label={t(`stats.${s.key}`)} value={s.value} note={c.note} tone={c.tone} />;
          })}
      </section>

      <div className="grid gap-6 xl:grid-cols-[minmax(0,2fr)_minmax(0,1fr)] xl:items-start">
        {/* Operational first in reading order (and first on a phone); beside the charts on wide screens. */}
        <div className="flex min-w-0 flex-col gap-6 xl:col-start-2 xl:row-start-1">
          <Card>
            <CardHeader>
              <CardTitle>{t('now.title')}</CardTitle>
            </CardHeader>
            <CardContent className="grid grid-cols-2 gap-3">
              <Link href="/admin/orders" className="flex flex-col gap-1 rounded-soft border border-line p-3 no-underline hover-capable:hover:border-field">
                <span className="flex items-center gap-1.5 text-xs text-muted">
                  <ChefHat className="size-3.5" aria-hidden="true" />
                  {t('now.kitchen')}
                </span>
                <span className="text-xl font-semibold tabular">{formatNumber(kitchenTotal, locale)}</span>
                <span className="text-xs text-muted">{data.kitchen.placed ? t('now.waiting', { n: formatNumber(data.kitchen.placed, locale) }) : t('now.noneWaiting')}</span>
              </Link>
              <Link href="/admin/tables" className="flex flex-col gap-1 rounded-soft border border-line p-3 no-underline hover-capable:hover:border-field">
                <span className="flex items-center gap-1.5 text-xs text-muted">
                  <HandPlatter className="size-3.5" aria-hidden="true" />
                  {t('now.tables')}
                </span>
                <span className="text-xl font-semibold tabular">{formatNumber(data.openRequests, locale)}</span>
                <span className="text-xs text-muted">{data.openRequests ? t('now.callsOpen') : t('now.callsNone')}</span>
              </Link>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <div>
                <CardTitle>{t('upcoming.title')}</CardTitle>
                <CardDescription>{t('upcoming.description')}</CardDescription>
              </div>
              <Link href={`/admin/reservations?date=${data.today}`} className="text-[0.8125rem] text-link">
                {t('upcoming.all')}
              </Link>
            </CardHeader>
            {data.upcoming.length ? (
              <ul className="divide-y divide-line">
                {data.upcoming.map((r) => (
                  <li key={r.id}>
                    <Link href={`/admin/reservations/${r.code}`} className="flex items-center gap-3 px-5 py-2.5 no-underline hover-capable:hover:bg-surface/60">
                      <span className="w-16 shrink-0 text-[0.8125rem] font-medium tabular">{formatWallTime(r.time, locale)}</span>
                      <span className="min-w-0 flex-1">
                        <span className="block truncate text-[0.875rem]">
                          <bdi>{r.name}</bdi>
                        </span>
                        <span className="block truncate text-xs text-muted">
                          {tu('guests', plural(r.partySize, locale))}
                          {r.tables.length ? ` · ${r.tables.join(locale === 'ar' ? '، ' : ', ')}` : ''}
                          {scope.branch ? '' : ` · ${branchName(r.branchId)}`}
                        </span>
                      </span>
                      {r.status === 'seated' ? <Badge tone="success">{ts('seated')}</Badge> : r.status === 'pending' ? <Badge tone="warning">{ts('pending')}</Badge> : r.notes ? <Badge tone="sun">{t('upcoming.notes')}</Badge> : null}
                    </Link>
                  </li>
                ))}
              </ul>
            ) : (
              <EmptyState title={t('upcoming.empty')} />
            )}
          </Card>
        </div>

        <div className="flex min-w-0 flex-col gap-6 xl:col-start-1 xl:row-span-2 xl:row-start-1">
          {money ? (
            <Card>
              <CardHeader>
                <div>
                  <CardTitle>{t('charts.sales.title')}</CardTitle>
                  <CardDescription>{t('charts.sales.description')}</CardDescription>
                </div>
              </CardHeader>
              <CardContent>
                <SalesChart data={data.salesByDay} labels={{ title: t('charts.sales.title'), date: t('charts.date'), revenue: t('stats.revenue'), orders: t('stats.orders') }} />
              </CardContent>
            </Card>
          ) : null}
          <Card>
            <CardHeader>
              <div>
                <CardTitle>{t('charts.hourly.title')}</CardTitle>
                <CardDescription>{t('charts.hourly.description', { day: lastWeekday })}</CardDescription>
              </div>
            </CardHeader>
            <CardContent>
              <HourlyChart data={data.ordersByHour} labels={{ title: t('charts.hourly.title'), hour: t('charts.hour'), today: t('charts.hourly.today'), lastWeek: t('charts.hourly.lastWeek', { day: lastWeekday }) }} />
            </CardContent>
          </Card>
          <div className="grid gap-6 lg:grid-cols-2">
            <Card>
              <CardHeader>
                <div>
                  <CardTitle>{t('charts.channels.title')}</CardTitle>
                  <CardDescription>{t('charts.channels.description')}</CardDescription>
                </div>
              </CardHeader>
              <CardContent>
                <ShareBars
                  rows={data.channels.map((c) => ({
                    label: t(`channels.${c.channel}`),
                    value: money ? c.revenue : c.orders,
                    display: money ? formatMoney(c.revenue, locale) : formatNumber(c.orders, locale),
                    note: money ? t('charts.channels.orders', plural(c.orders, locale)) : undefined,
                  }))}
                />
              </CardContent>
            </Card>
            <Card>
              <CardHeader>
                <div>
                  <CardTitle>{t('charts.visitors.title')}</CardTitle>
                  <CardDescription>{t('charts.visitors.description')}</CardDescription>
                </div>
              </CardHeader>
              <CardContent>
                <VisitorsChart data={data.visitors} labels={{ title: t('charts.visitors.title'), date: t('charts.date'), views: t('charts.visitors.views'), visitors: t('charts.visitors.visitors') }} />
              </CardContent>
            </Card>
          </div>
        </div>

        <div className="min-w-0 xl:col-start-2 xl:row-start-2">
          <Card>
            <CardHeader>
              <CardTitle>{t('activity.title')}</CardTitle>
            </CardHeader>
            <ActivityFeed items={activity} timeZone={scope.timeZone} />
          </Card>
        </div>
      </div>
    </div>
  );
}
