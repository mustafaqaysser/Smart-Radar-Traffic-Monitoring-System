'use client';

import Link from 'next/link';
import { useLocale, useTranslations } from 'next-intl';
import { useRouter } from 'next/navigation';
import { useId, useState } from 'react';
import type { OrderRow } from '@/lib/admin/orders';
import { STATUS_TONE } from '@/lib/admin/order-flow';
import { ORDER_STATUSES, type OrderChannel, type OrderStatus } from '@/lib/db/schema';
import { formatMoney } from '@/lib/i18n/format';
import { Badge } from '../ui/badge';
import { Button } from '../ui/button';
import { DataTable, type ColumnDef } from '../ui/data-table';
import { Input, Select } from '../ui/input';
import { ChannelBadge, usePaymentLabel } from './order-card';

/** Every order in a date range: search by number, guest, email or phone; filter by status and channel. */
export function OrderHistory({ rows, from, to, showBranch }: { rows: OrderRow[]; from: string; to: string; showBranch: boolean }) {
  const t = useTranslations('admin.orders');
  const locale = useLocale();
  const router = useRouter();
  const id = useId();
  const payment = usePaymentLabel();
  const [status, setStatus] = useState<OrderStatus | ''>('');
  const [channel, setChannel] = useState<OrderChannel | ''>('');
  const [range, setRange] = useState({ from, to });

  const columns: ColumnDef<OrderRow, unknown>[] = [
    {
      accessorKey: 'number',
      header: t('history.columns.number'),
      cell: ({ row }) => (
        <Link href={`/admin/orders/${row.original.number}`} className="font-mono font-medium no-underline hover-capable:hover:underline" dir="ltr">
          {row.original.number}
        </Link>
      ),
    },
    { accessorKey: 'createdAt', header: t('history.columns.placed'), cell: ({ row }) => <span className="whitespace-nowrap">{row.original.placedLabel}</span> },
    {
      accessorKey: 'name',
      header: t('history.columns.guest'),
      cell: ({ row }) => (
        <span className="block max-w-56 truncate">
          <bdi>{row.original.name}</bdi>
        </span>
      ),
    },
    { accessorKey: 'channel', header: t('history.columns.channel'), cell: ({ row }) => <ChannelBadge channel={row.original.channel} table={null} />, enableSorting: false },
    ...(showBranch ? [{ accessorKey: 'branchName', header: t('history.columns.house') } as ColumnDef<OrderRow, unknown>] : []),
    { accessorKey: 'status', header: t('history.columns.status'), cell: ({ row }) => <Badge tone={STATUS_TONE[row.original.status]}>{t(`status.${row.original.status}`)}</Badge> },
    {
      id: 'payment',
      header: t('history.columns.payment'),
      enableSorting: false,
      cell: ({ row }) => {
        const p = payment(row.original);
        return <span className="whitespace-nowrap text-xs text-muted">{p.text}</span>;
      },
    },
    { accessorKey: 'total', header: t('history.columns.total'), meta: { align: 'end' }, cell: ({ row }) => formatMoney(row.original.total, locale) },
  ];

  const filtered = rows.filter((r) => (!status || r.status === status) && (!channel || r.channel === channel));

  return (
    <DataTable
      data={filtered}
      columns={columns}
      searchText={(r) => `${r.number} ${r.name} ${r.email} ${r.phone} ${r.promoCode ?? ''}`}
      searchLabel={t('history.search')}
      rowHref={(r) => `/admin/orders/${r.number}`}
      empty={{ title: t('history.empty') }}
      toolbar={
        <>
          <form
            className="flex flex-wrap items-center gap-2"
            onSubmit={(e) => {
              e.preventDefault();
              router.push(`/admin/orders?view=history&from=${range.from}&to=${range.to}`);
            }}
          >
            <label htmlFor={`${id}-from`} className="sr-only">
              {t('history.from')}
            </label>
            <Input id={`${id}-from`} type="date" value={range.from} max={range.to} onChange={(e) => setRange((r) => ({ ...r, from: e.target.value }))} className="w-auto" dir="ltr" />
            <span className="text-xs text-muted" aria-hidden="true">
              –
            </span>
            <label htmlFor={`${id}-to`} className="sr-only">
              {t('history.to')}
            </label>
            <Input id={`${id}-to`} type="date" value={range.to} min={range.from} onChange={(e) => setRange((r) => ({ ...r, to: e.target.value }))} className="w-auto" dir="ltr" />
            <Button type="submit" size="sm">
              {t('history.apply')}
            </Button>
          </form>
          <label htmlFor={`${id}-status`} className="sr-only">
            {t('history.columns.status')}
          </label>
          <Select id={`${id}-status`} value={status} onChange={(e) => setStatus(e.target.value as OrderStatus | '')} className="w-auto">
            <option value="">{t('history.allStatuses')}</option>
            {ORDER_STATUSES.map((s) => (
              <option key={s} value={s}>
                {t(`status.${s}`)}
              </option>
            ))}
          </Select>
          <label htmlFor={`${id}-channel`} className="sr-only">
            {t('history.columns.channel')}
          </label>
          <Select id={`${id}-channel`} value={channel} onChange={(e) => setChannel(e.target.value as OrderChannel | '')} className="w-auto">
            <option value="">{t('history.allChannels')}</option>
            {(['delivery', 'pickup', 'dine_in'] as const).map((c) => (
              <option key={c} value={c}>
                {t(`channels.${c}`)}
              </option>
            ))}
          </Select>
        </>
      }
    />
  );
}
