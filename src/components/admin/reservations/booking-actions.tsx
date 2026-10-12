'use client';

import { useLocale, useTranslations } from 'next-intl';
import { useId, useState } from 'react';
import { setReservationStatus } from '@/lib/actions/admin/reservations';
import { STAFF_TRANSITIONS } from '@/lib/admin/reservation-flow';
import type { ReservationStatus } from '@/lib/db/schema';
import { formatMoney } from '@/lib/i18n/format';
import { cn } from '@/lib/utils/cn';
import { Button } from '../ui/button';
import { Checkbox } from '../ui/checkbox';
import { Confirm } from '../ui/confirm';
import { useAdminAction } from '../use-action';

export interface BookingActionTarget {
  id: string;
  code: string;
  status: ReservationStatus;
  name: string;
  email: string;
  depositAmount: number;
  depositStatus: 'none' | 'pending' | 'paid' | 'refunded';
}

/**
 * The next steps for a booking, in the order a host needs them: seat, finish, no-show, and — behind a
 * confirmation — cancel (with or without refunding a paid deposit and emailing the guest).
 */
export function BookingActions({ booking, size = 'sm', className }: { booking: BookingActionTarget; size?: 'sm' | 'md'; className?: string }) {
  const t = useTranslations('admin.reservations.actions');
  const ts = useTranslations('admin.reservations.status');
  const locale = useLocale();
  const id = useId();
  const [pending, run] = useAdminAction();
  const [refund, setRefund] = useState(true);
  const [notify, setNotify] = useState(Boolean(booking.email));
  const next = STAFF_TRANSITIONS[booking.status];
  const go = (status: ReservationStatus, extra: { refund?: boolean; notify?: boolean } = {}) =>
    run(() => setReservationStatus({ id: booking.id, next: status, refund: extra.refund ?? false, notify: extra.notify ?? Boolean(booking.email) }), { success: t('done', { code: booking.code, status: ts(status) }) });

  // Forward steps first; undo steps are quieter.
  const primary: ReservationStatus | null = booking.status === 'confirmed' ? 'seated' : booking.status === 'seated' ? 'completed' : booking.status === 'pending' ? 'confirmed' : null;
  const label = (status: ReservationStatus) => {
    if (status === 'confirmed') return booking.status === 'pending' ? t('confirmWithoutDeposit') : booking.status === 'cancelled' ? t('restore') : booking.status === 'seated' ? t('unseat') : t('arrivedNoSeat');
    if (status === 'seated') return booking.status === 'completed' ? t('reopen') : booking.status === 'no_show' ? t('lateArrival') : t('seat');
    return t(status);
  };

  return (
    <div className={cn('flex flex-wrap items-center gap-1.5', className)}>
      {next.map((status) =>
        status === 'cancelled' ? (
          <Confirm
            key={status}
            trigger={
              <Button variant="danger-outline" size={size} disabled={pending}>
                {t('cancelled')}
              </Button>
            }
            title={t('cancelTitle', { code: booking.code })}
            description={t('cancelBody', { name: booking.name })}
            confirmLabel={t('cancelConfirm')}
            onConfirm={async () => (await go('cancelled', { refund, notify })).ok}
          >
            <div className="flex flex-col gap-3 text-[0.875rem]">
              {booking.depositStatus === 'paid' ? (
                <label htmlFor={`${id}-refund`} className="flex items-center gap-2">
                  <Checkbox id={`${id}-refund`} checked={refund} onCheckedChange={(v) => setRefund(v === true)} />
                  {t('refund', { amount: formatMoney(booking.depositAmount, locale) })}
                </label>
              ) : null}
              {booking.email ? (
                <label htmlFor={`${id}-notify`} className="flex items-center gap-2">
                  <Checkbox id={`${id}-notify`} checked={notify} onCheckedChange={(v) => setNotify(v === true)} />
                  {t('notify')}
                </label>
              ) : null}
            </div>
          </Confirm>
        ) : (
          <Button key={status} variant={status === primary ? 'primary' : status === 'no_show' ? 'danger-outline' : 'outline'} size={size} disabled={pending} onClick={() => go(status)}>
            {label(status)}
          </Button>
        ),
      )}
    </div>
  );
}
