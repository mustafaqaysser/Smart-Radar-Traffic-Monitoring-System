import type { Metadata } from 'next';
import Link from 'next/link';
import { getTranslations } from 'next-intl/server';
import { CalendarPlus } from 'lucide-react';
import { adminPage, singleBranch } from '@/lib/admin/context';
import { can } from '@/lib/auth/permissions';
import { db } from '@/lib/db/client';
import * as s from '@/lib/db/schema';
import { formatDateString, formatNumber } from '@/lib/i18n/format';
import { tr } from '@/lib/i18n/localized';
import { dayView, monthCounts, openWaitlist, rangeBookings } from '@/lib/server/reservations-staff';
import { addDays, toDateString } from '@/lib/time/zoned';
import { DayNav } from '@/components/admin/reservations/day-nav';
import { MonthCalendar } from '@/components/admin/reservations/month-calendar';
import { ReservationList } from '@/components/admin/reservations/reservation-list';
import { ReservationTimeline } from '@/components/admin/reservations/timeline';
import { WaitlistPanel } from '@/components/admin/reservations/waitlist-panel';
import { LiveRefresh } from '@/components/admin/shell/live';
import { Button } from '@/components/admin/ui/button';
import { Card } from '@/components/admin/ui/card';
import { LinkTabs } from '@/components/admin/ui/link-tabs';
import { PageHeader, Stat } from '@/components/admin/ui/page-header';
import { inArray } from 'drizzle-orm';

export async function generateMetadata(): Promise<Metadata> {
  const t = await getTranslations('admin.reservations');
  return { title: t('title') };
}

const DATE = /^\d{4}-\d{2}-\d{2}$/;
const VIEWS = ['timeline', 'list', 'calendar', 'waitlist'] as const;
type View = (typeof VIEWS)[number];

export default async function ReservationsPage({ searchParams }: { searchParams: Promise<Record<string, string | undefined>> }) {
  const { user, locale, scope } = await adminPage('reservations:view');
  const params = await searchParams;
  const t = await getTranslations('admin.reservations');
  const tu = await getTranslations('common.units');
  const view: View = (VIEWS as readonly string[]).includes(params.view ?? '') ? (params.view as View) : 'timeline';
  const branch = singleBranch(scope, params.branch);
  const tz = view === 'timeline' ? branch.timeZone : scope.timeZone;
  const today = toDateString(new Date(), tz);
  const date = params.date && DATE.test(params.date) ? params.date : today;
  const canManage = can(user.role, 'reservations:manage');
  const branchParam = params.branch ? `&branch=${params.branch}` : '';
  const branchName = (id: string) => tr(scope.branches.find((b) => b.id === id)?.shortName, locale);

  let body: React.ReactNode = null;
  let summary: { bookings: number; covers: number; seated: number; waiting: number } | null = null;

  if (view === 'timeline') {
    const day = await dayView(branch, date);
    const live = day.bookings.filter((b) => b.status !== 'cancelled');
    summary = { bookings: live.length, covers: live.filter((b) => b.status !== 'no_show').reduce((n, b) => n + b.partySize, 0), seated: live.filter((b) => b.status === 'seated').length, waiting: day.waitlist.filter((w) => w.status === 'waiting' || w.status === 'offered').length };
    body = (
      <>
        <ReservationTimeline day={day} date={date} timeZone={branch.timeZone} today={date === today} canManage={canManage} />
        {day.bookings.some((b) => b.status === 'cancelled') ? (
          <p className="text-xs text-muted">{t('timeline.cancelledNote', { n: formatNumber(day.bookings.filter((b) => b.status === 'cancelled').length, locale) })}</p>
        ) : null}
      </>
    );
  } else if (view === 'list') {
    const to = params.range === 'week' ? addDays(date, 6) : date;
    const branchIds = params.branch ? [branch.id] : scope.branchIds;
    const rows = await rangeBookings(branchIds, date, to);
    const tableIds = [...new Set(rows.flatMap((r) => r.tableIds))];
    const tables = tableIds.length ? await db.select({ id: s.diningTables.id, label: s.diningTables.label }).from(s.diningTables).where(inArray(s.diningTables.id, tableIds)) : [];
    const label = new Map(tables.map((x) => [x.id, x.label]));
    const live = rows.filter((r) => r.status !== 'cancelled');
    summary = { bookings: live.length, covers: live.filter((b) => b.status !== 'no_show').reduce((n, b) => n + b.partySize, 0), seated: live.filter((b) => b.status === 'seated').length, waiting: 0 };
    body = (
      <Card>
        <ReservationList
          rows={rows.map((r) => ({ ...r, branchName: branchName(r.branchId), tableLabels: r.tableIds.map((id) => label.get(id) ?? id), dateLabel: formatDateString(r.date, locale, { weekday: 'short', day: 'numeric', month: 'short' }) }))}
          canManage={canManage}
          showBranch={branchIds.length > 1}
          multiDay={to !== date}
        />
      </Card>
    );
  } else if (view === 'calendar') {
    const month = params.month && /^\d{4}-\d{2}$/.test(params.month) ? params.month : date.slice(0, 7);
    const [y, m] = month.split('-').map(Number) as [number, number];
    const last = `${month}-${String(new Date(Date.UTC(y, m, 0)).getUTCDate()).padStart(2, '0')}`;
    const counts = await monthCounts(params.branch ? [branch.id] : scope.branchIds, `${month}-01`, last);
    const prevMonth = new Date(Date.UTC(y, m - 2, 1)).toISOString().slice(0, 7);
    const nextMonth = new Date(Date.UTC(y, m, 1)).toISOString().slice(0, 7);
    body = (
      <Card>
        <div className="flex items-center justify-between gap-3 border-b border-line px-5 py-3">
          <h2 className="text-[0.9375rem] font-semibold">{formatDateString(`${month}-01`, locale, { month: 'long', year: 'numeric' })}</h2>
          <div className="flex gap-1.5">
            <Button asChild size="sm">
              <Link href={`/admin/reservations?view=calendar&month=${prevMonth}${branchParam}`}>{t('nav.previousMonth')}</Link>
            </Button>
            <Button asChild size="sm">
              <Link href={`/admin/reservations?view=calendar&month=${nextMonth}${branchParam}`}>{t('nav.nextMonth')}</Link>
            </Button>
          </div>
        </div>
        <div className="p-4">
          <MonthCalendar
            month={month}
            counts={counts}
            today={today}
            locale={locale}
            branchParam={branchParam}
            labels={{ bookings: (n) => t('calendar.bookings', { n }), covers: (n) => t('calendar.covers', { n }), weekdays: '' }}
          />
        </div>
      </Card>
    );
  } else {
    const branchIds = params.branch ? [branch.id] : scope.branchIds;
    const entries = await openWaitlist(branchIds, today);
    body = (
      <WaitlistPanel
        rows={entries.map((e) => ({ id: e.id, branchId: e.branchId, branchName: branchIds.length > 1 ? branchName(e.branchId) : '', date: e.date, dateLabel: formatDateString(e.date, locale, { weekday: 'long', day: 'numeric', month: 'long' }), preferredTime: e.preferredTime, flexibleMinutes: e.flexibleMinutes, partySize: e.partySize, name: e.name, email: e.email, phone: e.phone, notes: e.notes, status: e.status }))}
        canManage={canManage}
        offerTarget={params.date ? { branchId: branch.id, date } : null}
      />
    );
  }

  const tabs = VIEWS.map((v) => ({ value: v, href: `/admin/reservations?view=${v}&date=${date}${branchParam}`, label: t(`views.${v}`) }));

  return (
    <div className="flex flex-col gap-5">
      <LiveRefresh topics={['reservations']} />
      <PageHeader
        title={t('title')}
        description={view === 'timeline' ? t('descriptions.timeline', { house: tr(branch.shortName, locale) }) : t(`descriptions.${view}`)}
        actions={
          canManage ? (
            <Button asChild variant="primary">
              <Link href={`/admin/reservations/new?date=${date}${view === 'timeline' ? `&branch=${branch.id}` : ''}`}>
                <CalendarPlus aria-hidden="true" />
                {t('actions.new')}
              </Link>
            </Button>
          ) : null
        }
      />
      <LinkTabs label={t('viewsLabel')} param="view" items={tabs} />
      {view !== 'calendar' && view !== 'waitlist' ? (
        <div className="flex flex-wrap items-center justify-between gap-3">
          <DayNav date={date} today={today} prev={addDays(date, -1)} next={addDays(date, 1)} path="/admin/reservations" query={`view=${view}${params.range === 'week' ? '&range=week' : ''}${branchParam}`} label={t('nav.date')} />
          <div className="flex flex-wrap items-center gap-3">
            {view === 'list' ? (
              <Link href={`/admin/reservations?view=list&date=${date}${params.range === 'week' ? '' : '&range=week'}${branchParam}`} className="text-[0.8125rem] text-link">
                {params.range === 'week' ? t('nav.oneDay') : t('nav.week')}
              </Link>
            ) : null}
            {view === 'timeline' && scope.branches.length > 1 ? (
              <nav aria-label={t('nav.house')} className="flex gap-1">
                {scope.branches.map((b) => (
                  <Button key={b.id} asChild size="sm" variant={b.id === branch.id ? 'secondary' : 'outline'}>
                    <Link href={`/admin/reservations?view=timeline&date=${date}&branch=${b.id}`} aria-current={b.id === branch.id ? 'page' : undefined}>
                      {tr(b.shortName, locale)}
                    </Link>
                  </Button>
                ))}
              </nav>
            ) : null}
          </div>
        </div>
      ) : null}
      {summary ? (
        <section aria-label={t('summary.label')} className="grid grid-cols-2 gap-3 sm:grid-cols-4">
          <Stat label={t('summary.bookings')} value={formatNumber(summary.bookings, locale)} />
          <Stat label={t('summary.covers')} value={tu('guests', { count: summary.covers, n: formatNumber(summary.covers, locale) })} />
          <Stat label={t('summary.seated')} value={formatNumber(summary.seated, locale)} />
          <Stat label={t('summary.waitlist')} value={formatNumber(summary.waiting, locale)} />
        </section>
      ) : null}
      {body}
    </div>
  );
}
