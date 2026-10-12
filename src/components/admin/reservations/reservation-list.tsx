'use client';

import Link from 'next/link';
import { useLocale, useTranslations } from 'next-intl';
import { useId, useState } from 'react';
import { RESERVATION_TONE } from '@/lib/admin/reservation-flow';
import { RESERVATION_STATUSES, type ReservationStatus } from '@/lib/db/schema';
import type { DayBooking } from '@/lib/server/reservations-staff';
import { formatNumber, formatWallTime } from '@/lib/i18n/format';
import { Badge } from '../ui/badge';
import { DataTable, type ColumnDef } from '../ui/data-table';
import { Select } from '../ui/input';
import { BookingActions } from './booking-actions';

/** `dateLabel` is formatted on the server (Node and browsers punctuate dates differently). */
export type ListBooking = DayBooking & { branchId: string; date: string; dateLabel: string; branchName: string; tableLabels: string[] };

/** Bookings over the chosen dates: search by name, phone, email or code; filter by status; act inline. */
export function ReservationList({ rows, canManage, showBranch, multiDay }: { rows: ListBooking[]; canManage: boolean; showBranch: boolean; multiDay: boolean }) {
  const t = useTranslations('admin.reservations');
  const locale = useLocale();
  const id = useId();
  const [status, setStatus] = useState<ReservationStatus | 'live' | ''>('live');
  const filtered = rows.filter((r) => (status === 'live' ? r.status !== 'cancelled' : !status || r.status === status));

  const columns: ColumnDef<ListBooking, unknown>[] = [
    {
      id: 'when',
      accessorFn: (r) => r.startsAt,
      header: t('list.columns.time'),
      cell: ({ row }) => (
        <span className="whitespace-nowrap font-medium tabular">
          {multiDay ? `${row.original.dateLabel} · ` : ''}
          {formatWallTime(row.original.time, locale)}
        </span>
      ),
    },
    {
      accessorKey: 'name',
      header: t('list.columns.guest'),
      cell: ({ row }) => (
        <Link href={`/admin/reservations/${row.original.code}`} className="block max-w-56 no-underline hover-capable:hover:underline">
          <span className="block truncate font-medium">
            <bdi>{row.original.name}</bdi>
          </span>
          <span className="block font-mono text-[0.6875rem] text-muted" dir="ltr">
            {row.original.code}
          </span>
        </Link>
      ),
    },
    { accessorKey: 'partySize', header: t('list.columns.party'), meta: { align: 'end' }, cell: ({ row }) => formatNumber(row.original.partySize, locale) },
    { id: 'tables', header: t('list.columns.tables'), enableSorting: false, cell: ({ row }) => <span className="whitespace-nowrap">{row.original.tableLabels.join(locale === 'ar' ? '، ' : ', ')}</span> },
    ...(showBranch ? [{ accessorKey: 'branchName', header: t('list.columns.house') } as ColumnDef<ListBooking, unknown>] : []),
    { accessorKey: 'status', header: t('list.columns.status'), cell: ({ row }) => <Badge tone={RESERVATION_TONE[row.original.status]}>{t(`status.${row.original.status}`)}</Badge> },
    {
      id: 'flags',
      header: t('list.columns.notes'),
      enableSorting: false,
      cell: ({ row }) => {
        const r = row.original;
        return (
          <span className="flex flex-wrap gap-1">
            {r.occasion ? <Badge tone="sun">{t(`occasions.${r.occasion}`)}</Badge> : null}
            {r.dietaryNotes ? <Badge tone="warning">{t('list.dietary')}</Badge> : null}
            {r.notes || r.staffNotes ? <Badge tone="outline">{t('list.notes')}</Badge> : null}
          </span>
        );
      },
    },
    ...(canManage
      ? [
          {
            id: 'actions',
            header: () => <span className="sr-only">{t('list.columns.actions')}</span>,
            enableSorting: false,
            cell: ({ row }) => (['confirmed', 'seated', 'pending'].includes(row.original.status) ? <BookingActions booking={row.original} /> : null),
          } as ColumnDef<ListBooking, unknown>,
        ]
      : []),
  ];

  return (
    <DataTable
      data={filtered}
      columns={columns}
      searchText={(r) => `${r.name} ${r.phone} ${r.email} ${r.code}`}
      searchLabel={t('list.search')}
      rowHref={(r) => `/admin/reservations/${r.code}`}
      empty={{ title: t('list.empty') }}
      pageSize={50}
      toolbar={
        <>
          <label htmlFor={`${id}-status`} className="sr-only">
            {t('list.columns.status')}
          </label>
          <Select id={`${id}-status`} value={status} onChange={(e) => setStatus(e.target.value as ReservationStatus | 'live' | '')} className="w-auto">
            <option value="live">{t('list.live')}</option>
            <option value="">{t('list.all')}</option>
            {RESERVATION_STATUSES.map((st) => (
              <option key={st} value={st}>
                {t(`status.${st}`)}
              </option>
            ))}
          </Select>
        </>
      }
    />
  );
}
