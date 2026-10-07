import type { Metadata } from 'next';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import { AccountHeader, LedgerBlock } from '@/components/site/account/account-header';
import { ButtonLink } from '@/components/site/ui/button';
import { Link } from '@/i18n/navigation';
import { getCurrentUser } from '@/lib/auth/session';
import { formatClock, formatDate } from '@/lib/i18n/format';
import { tr } from '@/lib/i18n/localized';
import { plural } from '@/lib/i18n/plural';
import { getBranches } from '@/lib/queries/branches';
import type { BranchDTO } from '@/lib/queries/types';
import { accountReservations, accountTickets } from '@/lib/server/account';
import { ticketPath } from '@/lib/server/events';
import { bookingPath, changeable, type Reservation } from '@/lib/server/reservations';
import { getSettings } from '@/lib/server/settings';
import { linkToken } from '@/lib/server/tokens';
import { pageMetadata } from '@/lib/site/metadata';
import { cn } from '@/lib/utils/cn';

type Params = Promise<{ locale: string }>;

export async function generateMetadata({ params }: { params: Params }): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: 'account.meta' });
  return pageMetadata({ locale, path: '/account/reservations', title: t('reservations'), noindex: true });
}

export default async function ReservationsPage({ params }: { params: Params }) {
  const { locale } = await params;
  setRequestLocale(locale);
  const user = await getCurrentUser();
  if (!user) return null;
  const now = new Date();
  const [t, tu, settings, branches, { upcoming, past }, tickets] = await Promise.all([
    getTranslations('account'),
    getTranslations('common.units'),
    getSettings(),
    getBranches(),
    accountReservations(user, now),
    accountTickets(user),
  ]);
  const branchOf = new Map(branches.map((b) => [b.id, b]));
  const f = settings.features;

  const row = (r: Reservation, branch: BranchDTO | undefined, upcomingRow: boolean) => {
    const tz = branch?.timeZone ?? 'Asia/Riyadh';
    return (
      <li key={r.id} className="grid gap-3 border-b border-line py-6 md:grid-cols-12 md:items-baseline md:gap-6">
        <div className="flex flex-col gap-1 md:col-span-6">
          <p className="t-heading-sm">
            {formatDate(r.startsAt, locale, tz, { weekday: 'long', day: 'numeric', month: 'long' })} · <bdi>{formatClock(r.startsAt, locale, tz)}</bdi>
          </p>
          <p className="t-small text-muted">
            {branch ? tr(branch.shortName, locale) : null} · {tu('guests', plural(r.partySize, locale))} · <bdi>{r.code}</bdi>
          </p>
        </div>
        <p className={cn('t-label md:col-span-3', r.status === 'cancelled' || r.status === 'no_show' ? 'text-danger' : upcomingRow ? 'text-accent' : 'text-muted')}>{t(`status.reservation.${r.status}`)}</p>
        <div className="flex flex-wrap gap-x-5 md:col-span-3 md:justify-end">
          {upcomingRow && changeable(r, now) ? (
            <Link href={`/reserve/manage/${r.code}?token=${linkToken('reservation', r.id)}`} className="t-small inline-flex min-h-11 items-center underline decoration-line underline-offset-4">
              {t('reservations.manage')}
            </Link>
          ) : null}
          {upcomingRow ? (
            <Link href={bookingPath(r)} className="t-small inline-flex min-h-11 items-center underline decoration-line underline-offset-4">
              {t('reservations.view')}
            </Link>
          ) : branch ? (
            <Link href={{ pathname: '/reserve', query: { branch: branch.slug } }} className="t-small inline-flex min-h-11 items-center underline decoration-line underline-offset-4">
              {t('reservations.book')}
            </Link>
          ) : null}
        </div>
      </li>
    );
  };

  return (
    <>
      <AccountHeader title={t('reservations.title')} intro={t('reservations.intro')} />
      {f.reservations ? (
        <LedgerBlock title={t('reservations.upcoming')} action={upcoming.length ? <Link href="/reserve" className="t-small underline underline-offset-4">{t('reservations.book')}</Link> : null}>
          {upcoming.length ? (
            <ul className="-mt-5">{upcoming.map((r) => row(r, branchOf.get(r.branchId), true))}</ul>
          ) : (
            <div className="flex flex-wrap items-center justify-between gap-4">
              <p className="t-body text-muted">{t('reservations.none')}</p>
              <ButtonLink href="/reserve">{t('reservations.book')}</ButtonLink>
            </div>
          )}
        </LedgerBlock>
      ) : null}
      {f.events ? (
        <LedgerBlock title={t('reservations.tickets')}>
          {tickets.length ? (
            <ul className="-mt-5">
              {tickets.map(({ booking, event }) => {
                const branch = branchOf.get(event.branchId);
                const tz = branch?.timeZone ?? 'Asia/Riyadh';
                const ended = event.endsAt < now;
                return (
                  <li key={booking.id} className="grid gap-3 border-b border-line py-6 md:grid-cols-12 md:items-baseline md:gap-6">
                    <div className="flex flex-col gap-1 md:col-span-6">
                      <p className="t-heading-sm">{tr(event.title, locale)}</p>
                      <p className="t-small text-muted">
                        {formatDate(event.startsAt, locale, tz, { weekday: 'long', day: 'numeric', month: 'long' })} · <bdi>{formatClock(event.startsAt, locale, tz)}</bdi> · {tu('seats', plural(booking.quantity, locale))}
                      </p>
                    </div>
                    <p className={cn('t-label md:col-span-3', ended ? 'text-muted' : 'text-accent')}>{t(`status.ticket.${booking.status === 'checked_in' ? 'checked_in' : 'confirmed'}`)}</p>
                    <div className="md:col-span-3 md:text-end">
                      <Link href={ticketPath(booking)} className="t-small inline-flex min-h-11 items-center underline decoration-line underline-offset-4">
                        {t('reservations.ticket')}
                      </Link>
                    </div>
                  </li>
                );
              })}
            </ul>
          ) : (
            <div className="flex flex-wrap items-center justify-between gap-4">
              <p className="t-body text-muted">{t('reservations.noTickets')}</p>
              <ButtonLink href="/experiences" variant="secondary" icon={null}>
                {t('reservations.browseEvents')}
              </ButtonLink>
            </div>
          )}
        </LedgerBlock>
      ) : null}
      {f.reservations ? (
        <LedgerBlock title={t('reservations.past')}>{past.length ? <ul className="-mt-5">{past.map((r) => row(r, branchOf.get(r.branchId), false))}</ul> : <p className="t-body text-muted">{t('reservations.nonePast')}</p>}</LedgerBlock>
      ) : null}
    </>
  );
}
