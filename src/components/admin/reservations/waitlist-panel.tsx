'use client';

import { useLocale, useTranslations } from 'next-intl';
import { useEffect, useId, useState } from 'react';
import { bookWaitlistEntry, cancelWaitlistEntry, runWaitlistOffers, staffSlots, type StaffSlot } from '@/lib/actions/admin/reservations';
import { formatNumber, formatWallTime } from '@/lib/i18n/format';
import { Badge } from '../ui/badge';
import { Button } from '../ui/button';
import { Checkbox } from '../ui/checkbox';
import { Confirm } from '../ui/confirm';
import { Dialog, DialogContent, DialogFooter } from '../ui/dialog';
import { Select } from '../ui/input';
import { EmptyState } from '../ui/page-header';
import { useAdminAction } from '../use-action';

export interface WaitlistRow {
  id: string;
  branchId: string;
  branchName: string;
  date: string;
  dateLabel: string;
  preferredTime: string;
  flexibleMinutes: number;
  partySize: number;
  name: string;
  email: string;
  phone: string;
  notes: string | null;
  status: 'waiting' | 'offered' | 'booked' | 'expired' | 'cancelled';
}

function BookDialog({ entry, onClose }: { entry: WaitlistRow; onClose: () => void }) {
  const t = useTranslations('admin.reservations.waitlist');
  const locale = useLocale();
  const id = useId();
  const [slots, setSlots] = useState<StaffSlot[] | null>(null);
  const [time, setTime] = useState(entry.preferredTime);
  const [override, setOverride] = useState(false);
  const [sendEmail, setSendEmail] = useState(Boolean(entry.email));
  const [pending, run] = useAdminAction();
  useEffect(() => {
    let live = true;
    void staffSlots({ branchId: entry.branchId, date: entry.date, partySize: entry.partySize, area: 'any', excludeId: null }).then((r) => {
      if (!live || !r.ok) return;
      setSlots(r.data);
      const best = r.data.filter((sl) => sl.available).sort((a, b) => Math.abs(toMin(a.time) - toMin(entry.preferredTime)) - Math.abs(toMin(b.time) - toMin(entry.preferredTime)))[0];
      if (best) setTime(best.time);
    });
    return () => {
      live = false;
    };
  }, [entry]);
  const choices = slots ?? [];
  return (
    <Dialog open onOpenChange={(open) => !open && onClose()}>
      <DialogContent title={t('bookTitle', { name: entry.name })} description={t('bookBody', { party: formatNumber(entry.partySize, locale), time: formatWallTime(entry.preferredTime, locale) })}>
        <div className="flex flex-col gap-4">
          <div>
            <label htmlFor={`${id}-time`} className="mb-1.5 block text-[0.8125rem] font-medium">
              {t('time')}
            </label>
            <Select id={`${id}-time`} value={time} onChange={(e) => setTime(e.target.value)} disabled={!slots}>
              {!choices.some((sl) => sl.time === time) ? <option value={time}>{formatWallTime(time, locale)}</option> : null}
              {choices.map((sl) => (
                <option key={sl.time} value={sl.time} disabled={!sl.available && !override}>
                  {formatWallTime(sl.time, locale)}
                  {sl.available ? '' : ` — ${t(`reasons.${sl.reason ?? 'full'}`)}`}
                </option>
              ))}
            </Select>
          </div>
          <label htmlFor={`${id}-override`} className="flex items-start gap-2 text-[0.875rem]">
            <Checkbox id={`${id}-override`} checked={override} onCheckedChange={(v) => setOverride(v === true)} className="mt-0.5" />
            <span>
              {t('override')}
              <span className="block text-xs text-muted">{t('overrideHint')}</span>
            </span>
          </label>
          {entry.email ? (
            <label htmlFor={`${id}-email`} className="flex items-center gap-2 text-[0.875rem]">
              <Checkbox id={`${id}-email`} checked={sendEmail} onCheckedChange={(v) => setSendEmail(v === true)} />
              {t('sendEmail')}
            </label>
          ) : null}
        </div>
        <DialogFooter>
          <Button onClick={onClose}>{t('close')}</Button>
          <Button variant="primary" disabled={pending} onClick={() => run(() => bookWaitlistEntry({ entryId: entry.id, time, tableIds: null, override, sendEmail }), { success: t('booked', { name: entry.name }), onSuccess: onClose })}>
            {t('book')}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function toMin(time: string) {
  const [h, m] = time.split(':').map(Number) as [number, number];
  return (h < 5 ? h + 24 : h) * 60 + m;
}

/** Parties waiting for a table: book them yourself, offer free tables now, or take them off the list. */
export function WaitlistPanel({ rows, canManage, offerTarget }: { rows: WaitlistRow[]; canManage: boolean; offerTarget: { branchId: string; date: string } | null }) {
  const t = useTranslations('admin.reservations.waitlist');
  const tu = useTranslations('common.units');
  const locale = useLocale();
  const [booking, setBooking] = useState<WaitlistRow | null>(null);
  const [pending, run] = useAdminAction();
  const dates = [...new Set(rows.map((r) => r.date))];
  return (
    <div className="flex flex-col gap-4">
      {canManage && offerTarget ? (
        <div className="flex flex-wrap items-center justify-between gap-3 rounded-soft border border-line bg-raised px-4 py-3">
          <p className="text-[0.8125rem] text-muted">{t('offerHint')}</p>
          <Button size="sm" disabled={pending} onClick={() => run(() => runWaitlistOffers(offerTarget), { onSuccess: (d) => d })}>
            {t('offerNow')}
          </Button>
        </div>
      ) : null}
      {rows.length ? (
        dates.map((date) => (
          <section key={date} aria-labelledby={`wl-${date}`} className="rounded-soft border border-line bg-raised">
            <h2 id={`wl-${date}`} className="border-b border-line px-5 py-3 text-[0.875rem] font-semibold">
              {rows.find((r) => r.date === date)?.dateLabel}
            </h2>
            <ul className="divide-y divide-line">
              {rows
                .filter((r) => r.date === date)
                .map((r) => (
                  <li key={r.id} className="flex flex-wrap items-center gap-x-4 gap-y-2 px-5 py-3">
                    <div className="min-w-48 flex-1">
                      <p className="font-medium">
                        <bdi>{r.name}</bdi>
                        <span className="ms-2 text-[0.8125rem] font-normal text-muted">{tu('guests', { count: r.partySize, n: formatNumber(r.partySize, locale) })}</span>
                      </p>
                      <p className="text-xs text-muted">
                        {t('prefers', { time: formatWallTime(r.preferredTime, locale), flex: r.flexibleMinutes >= 1440 ? t('anyTime') : t('within', { n: formatNumber(r.flexibleMinutes, locale) }) })}
                        {' · '}
                        <span dir="ltr">{r.phone}</span>
                        {r.branchName ? ` · ${r.branchName}` : ''}
                      </p>
                      {r.notes ? (
                        <p className="text-xs">
                          <bdi>{r.notes}</bdi>
                        </p>
                      ) : null}
                    </div>
                    <Badge tone={r.status === 'offered' ? 'sun' : 'outline'}>{t(`status.${r.status}`)}</Badge>
                    {canManage ? (
                      <div className="flex gap-1.5">
                        <Button size="sm" variant="primary" onClick={() => setBooking(r)}>
                          {t('book')}
                        </Button>
                        <Confirm trigger={<Button size="sm" variant="ghost">{t('remove')}</Button>} title={t('removeTitle', { name: r.name })} description={t('removeBody')} confirmLabel={t('remove')} onConfirm={async () => (await run(() => cancelWaitlistEntry(r.id), { success: t('removed') })).ok} />
                      </div>
                    ) : null}
                  </li>
                ))}
            </ul>
          </section>
        ))
      ) : (
        <div className="rounded-soft border border-line bg-raised">
          <EmptyState title={t('empty')} body={t('emptyBody')} />
        </div>
      )}
      {booking ? <BookDialog entry={booking} onClose={() => setBooking(null)} /> : null}
    </div>
  );
}
