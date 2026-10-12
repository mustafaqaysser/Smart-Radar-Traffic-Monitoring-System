'use client';

import { useLocale, useTranslations } from 'next-intl';
import { useState } from 'react';
import { assignReservationTables } from '@/lib/actions/admin/reservations';
import type { DayTable } from '@/lib/server/reservations-staff';
import { formatNumber, formatWallTime } from '@/lib/i18n/format';
import { cn } from '@/lib/utils/cn';
import { Button } from '../ui/button';
import { useAdminAction } from '../use-action';

export interface TimelineBooking {
  id: string;
  code: string;
  name: string;
  time: string;
  partySize: number;
  status: string;
  tableIds: string[];
  startMin: number;
  endMin: number;
}

/**
 * Choose the tables for a booking. Tables another live booking holds during this one are marked with who has
 * them, and cannot be chosen; the seats chosen are totalled against the party.
 */
export function TablePicker({ tables, bookings, booking, onDone }: { tables: DayTable[]; bookings: TimelineBooking[]; booking: TimelineBooking; onDone?: () => void }) {
  const t = useTranslations('admin.reservations.tables');
  const ta = useTranslations('admin.reservations.areas');
  const locale = useLocale();
  const [chosen, setChosen] = useState<string[]>(booking.tableIds);
  const [pending, run] = useAdminAction();
  const busy = new Map<string, TimelineBooking>();
  for (const b of bookings) {
    if (b.id === booking.id || !['pending', 'confirmed', 'seated'].includes(b.status)) continue;
    if (b.startMin < booking.endMin && b.endMin > booking.startMin) for (const id of b.tableIds) busy.set(id, b);
  }
  const seats = tables.filter((tb) => chosen.includes(tb.id)).reduce((n, tb) => n + tb.maxSeats, 0);
  const areas = [...new Set(tables.map((tb) => tb.area))];
  const changed = chosen.length !== booking.tableIds.length || chosen.some((id) => !booking.tableIds.includes(id));

  return (
    <div className="flex flex-col gap-4">
      {areas.map((area) => (
        <fieldset key={area}>
          <legend className="mb-1.5 text-xs font-medium text-muted">{ta(area)}</legend>
          <div className="grid grid-cols-3 gap-1.5 sm:grid-cols-4">
            {tables
              .filter((tb) => tb.area === area)
              .map((tb) => {
                const holder = busy.get(tb.id);
                const on = chosen.includes(tb.id);
                return (
                  <label
                    key={tb.id}
                    className={cn(
                      'flex cursor-pointer flex-col rounded-hair border px-2 py-1.5 text-[0.8125rem] transition-colors has-[:focus-visible]:outline-2 has-[:focus-visible]:outline-ink',
                      on ? 'border-ink bg-ink text-bg' : holder ? 'cursor-not-allowed border-dashed border-line text-muted' : 'border-line hover-capable:hover:border-field',
                    )}
                    title={holder ? t('takenBy', { name: holder.name, time: formatWallTime(holder.time, locale) }) : undefined}
                  >
                    <input
                      type="checkbox"
                      className="sr-only"
                      checked={on}
                      disabled={Boolean(holder) && !on}
                      onChange={(e) => setChosen((prev) => (e.target.checked ? [...prev, tb.id] : prev.filter((x) => x !== tb.id)))}
                      aria-describedby={holder ? `${tb.id}-holder` : undefined}
                    />
                    <span className="font-medium">{tb.label}</span>
                    <span className={cn('text-[0.6875rem]', on ? 'text-bg/80' : 'text-muted')}>{t('seats', { n: formatNumber(tb.maxSeats, locale) })}</span>
                    {holder && !on ? (
                      <span id={`${tb.id}-holder`} className="truncate text-[0.6875rem]">
                        <bdi>{holder.name}</bdi>
                      </span>
                    ) : null}
                  </label>
                );
              })}
          </div>
        </fieldset>
      ))}
      <div className="flex flex-wrap items-center justify-between gap-2 border-t border-line pt-3">
        <p className={cn('text-[0.8125rem]', seats < booking.partySize ? 'text-danger' : 'text-muted')} aria-live="polite">
          {t('summary', { seats: formatNumber(seats, locale), party: formatNumber(booking.partySize, locale) })}
        </p>
        <Button variant="primary" size="sm" disabled={pending || !chosen.length || !changed} onClick={() => run(() => assignReservationTables({ id: booking.id, tableIds: chosen }), { success: t('saved'), onSuccess: () => onDone?.() })}>
          {t('save')}
        </Button>
      </div>
    </div>
  );
}
