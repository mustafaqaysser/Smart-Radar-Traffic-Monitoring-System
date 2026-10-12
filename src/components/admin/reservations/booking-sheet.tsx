'use client';

import Link from 'next/link';
import { ExternalLink, Mail, Phone } from 'lucide-react';
import { useLocale, useTranslations } from 'next-intl';
import { useId, useState } from 'react';
import { saveReservationNotes } from '@/lib/actions/admin/reservations';
import { RESERVATION_TONE } from '@/lib/admin/reservation-flow';
import type { DayTable, DayView } from '@/lib/server/reservations-staff';
import { formatMoney, formatNumber, formatWallTime, joinParts } from '@/lib/i18n/format';
import { Badge } from '../ui/badge';
import { Button } from '../ui/button';
import { Dialog, SheetContent } from '../ui/dialog';
import { Textarea } from '../ui/input';
import { DetailList } from '../ui/page-header';
import { useAdminAction } from '../use-action';
import { BookingActions } from './booking-actions';
import { TablePicker } from './table-picker';

type Booking = DayView['bookings'][number];

/** A booking's details beside the timeline: contact, notes, deposit, tables, and the next steps. */
export function BookingSheet({ booking, tables, bookings, canManage, onClose }: { booking: Booking; tables: DayTable[]; bookings: Booking[]; timeZone: string; canManage: boolean; onClose: () => void }) {
  const t = useTranslations('admin.reservations');
  const locale = useLocale();
  const id = useId();
  const [notes, setNotes] = useState(booking.staffNotes ?? '');
  const [pending, run] = useAdminAction();
  const tableLabels = booking.tableIds.map((tid) => tables.find((tb) => tb.id === tid)?.label ?? tid);
  const phone = booking.phone.replace(/[^\d+]/g, '');

  return (
    <Dialog open onOpenChange={(open) => !open && onClose()}>
      <SheetContent
        title={
          <span className="flex flex-wrap items-center gap-2">
            <bdi>{booking.name}</bdi>
            <Badge tone={RESERVATION_TONE[booking.status]}>{t(`status.${booking.status}`)}</Badge>
          </span>
        }
        description={t('sheet.summary', { time: formatWallTime(booking.time, locale), party: formatNumber(booking.partySize, locale), code: booking.code })}
      >
        <div className="flex flex-col gap-5">
          {canManage ? (
            <BookingActions booking={booking} />
          ) : null}

          <div className="flex flex-wrap gap-2">
            {booking.phone ? (
              <Button asChild size="sm">
                <a href={`tel:${phone}`} dir="ltr">
                  <Phone aria-hidden="true" />
                  {booking.phone}
                </a>
              </Button>
            ) : null}
            {booking.email ? (
              <Button asChild size="sm" variant="ghost">
                <a href={`mailto:${booking.email}`} dir="ltr">
                  <Mail aria-hidden="true" />
                  {booking.email}
                </a>
              </Button>
            ) : null}
          </div>

          <DetailList
            items={[
              { label: t('fields.tables'), value: tableLabels.join(locale === 'ar' ? '، ' : ', ') || '—' },
              { label: t('fields.area'), value: t(`areas.${booking.area}`) },
              ...(booking.occasion ? [{ label: t('fields.occasion'), value: t(`occasions.${booking.occasion}`) }] : []),
              { label: t('fields.source'), value: t(`sources.${booking.source}`) },
              ...(booking.depositAmount ? [{ label: t('fields.deposit'), value: joinParts([formatMoney(booking.depositAmount, locale), t(`deposit.${booking.depositStatus}`)], locale) }] : []),
              ...(booking.notes ? [{ label: t('fields.notes'), value: <bdi>{booking.notes}</bdi> }] : []),
              ...(booking.dietaryNotes ? [{ label: t('fields.dietary'), value: <bdi className="font-medium text-warning">{booking.dietaryNotes}</bdi> }] : []),
            ]}
          />

          <div>
            <label htmlFor={`${id}-notes`} className="mb-1.5 block text-[0.8125rem] font-medium">
              {t('fields.staffNotes')}
            </label>
            <Textarea id={`${id}-notes`} value={notes} onChange={(e) => setNotes(e.target.value)} rows={3} maxLength={1000} dir="auto" placeholder={t('sheet.notesPlaceholder')} readOnly={!canManage} />
            {canManage ? (
              <div className="mt-2 flex justify-end">
                <Button size="sm" disabled={pending || notes === (booking.staffNotes ?? '')} onClick={() => run(() => saveReservationNotes({ id: booking.id, staffNotes: notes }), { success: t('sheet.notesSaved') })}>
                  {t('sheet.saveNotes')}
                </Button>
              </div>
            ) : null}
          </div>

          {canManage && ['pending', 'confirmed', 'seated'].includes(booking.status) ? (
            <section aria-labelledby={`${id}-tables`}>
              <h3 id={`${id}-tables`} className="mb-2 text-[0.8125rem] font-medium">
                {t('sheet.moveTables')}
              </h3>
              <TablePicker tables={tables} bookings={bookings} booking={booking} />
            </section>
          ) : null}

          <Link href={`/admin/reservations/${booking.code}`} className="inline-flex items-center gap-1 text-[0.8125rem] text-link">
            {t('sheet.open')}
            <ExternalLink className="size-3.5" aria-hidden="true" />
          </Link>
        </div>
      </SheetContent>
    </Dialog>
  );
}
