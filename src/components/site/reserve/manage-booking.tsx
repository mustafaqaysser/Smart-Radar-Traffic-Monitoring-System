'use client';

import { useRouter } from 'next/navigation';
import { useLocale, useTranslations } from 'next-intl';
import { useEffect, useId, useState, useTransition } from 'react';
import { Icon } from '@/components/brand/icon';
import { Button } from '@/components/site/ui/button';
import { Dialog } from '@/components/site/ui/dialog';
import { Chip } from '@/components/site/ui/field';
import { cancelReservation, moveReservation } from '@/lib/actions/reservations';
import { formatNumber } from '@/lib/i18n/format';
import { plural } from '@/lib/i18n/plural';
import type { AreaChoice, FlowBranch, PublicSlot, SlotsResponse } from '@/lib/reserve/types';
import { cn } from '@/lib/utils/cn';
import { MonthCalendar } from './month-calendar';
import { SlotPicker } from './slot-picker';

interface ManageBookingProps {
  code: string;
  token: string;
  branch: FlowBranch;
  current: { date: string; time: string; party: number; area: AreaChoice };
  partyRange: { min: number; max: number };
  maxPartyOnline: number;
  /** Shown in the cancel dialog: what happens to a paid deposit. */
  depositNote: string | null;
}

/** Move or cancel a booking from its manage link (within the policy window). */
export function ManageBooking({ code, token, branch, current, partyRange, maxPartyOnline, depositNote }: ManageBookingProps) {
  const t = useTranslations('reserve');
  const tc = useTranslations('common');
  const tf = useTranslations('forms.errors');
  const tu = useTranslations('common.units');
  const locale = useLocale();
  const router = useRouter();
  const id = useId();
  const [date, setDate] = useState(branch.days[current.date] === 'open' ? current.date : null);
  const [party, setParty] = useState(current.party);
  const [area, setArea] = useState<AreaChoice>(current.area);
  const [slot, setSlot] = useState<PublicSlot | null>(null);
  const [refresh, setRefresh] = useState(0);
  const [slotsByKey, setSlotsByKey] = useState<Record<string, SlotsResponse | 'error'>>({});
  const [message, setMessage] = useState<{ tone: 'ok' | 'error'; text: string } | null>(null);
  const [confirming, setConfirming] = useState(false);
  const [saving, startSave] = useTransition();
  const [cancelling, startCancel] = useTransition();

  const key = date ? `${date}|${party}|${area}|${refresh}` : null;
  useEffect(() => {
    if (!key || !date || slotsByKey[key]) return undefined;
    const ctrl = new AbortController();
    const query = new URLSearchParams({ branch: branch.slug, date, party: String(party), area, code, token });
    fetch(`/api/reservations/slots?${query.toString()}`, { signal: ctrl.signal, cache: 'no-store' })
      .then((res) => (res.ok ? (res.json() as Promise<SlotsResponse>) : Promise.reject(new Error(String(res.status)))))
      .then((data) => setSlotsByKey((m) => ({ ...m, [key]: data })))
      .catch(() => {
        if (!ctrl.signal.aborted) setSlotsByKey((m) => ({ ...m, [key]: 'error' }));
      });
    return () => ctrl.abort();
  }, [key, date, party, area, branch.slug, code, token, slotsByKey]);
  const state = key ? slotsByKey[key] : undefined;
  const slots = state && state !== 'error' ? state : null;
  const unchanged = slot?.time === current.time && date === current.date && party === current.party && area === current.area;
  const errorText = (k: string) => (k === 'unavailable' || k === 'unchanged' ? t(`errors.${k}`) : k === 'window' ? t('manage.closedStatus') : tf.has(k) ? tf(k) : tf('unknown'));

  const save = () => {
    if (!date || !slot) return;
    setMessage(null);
    startSave(async () => {
      const res = await moveReservation({ code, token, date, time: slot.time, party, area });
      if (res.ok) {
        setMessage({ tone: 'ok', text: t('manage.saved') });
        setSlot(null);
        setRefresh((n) => n + 1);
        router.refresh();
      } else {
        setMessage({ tone: 'error', text: errorText(res.error) });
        if (res.error === 'unavailable') setRefresh((n) => n + 1);
      }
    });
  };

  const cancel = () => {
    startCancel(async () => {
      const res = await cancelReservation({ code, token });
      setConfirming(false);
      if (res.ok) {
        // The page re-renders as cancelled; keep focus on the status line so the change is announced.
        document.getElementById('booking-status')?.focus();
        router.refresh();
      } else setMessage({ tone: 'error', text: errorText(res.error) });
    });
  };

  const parties = Array.from({ length: partyRange.max - partyRange.min + 1 }, (_, i) => partyRange.min + i);

  return (
    <div className="flex flex-col gap-12">
      <section aria-labelledby={`${id}-change`} className="flex flex-col gap-8">
        <div className="flex flex-col gap-2">
          <h2 id={`${id}-change`} className="t-heading-lg">
            {t('manage.change')}
          </h2>
          <p className="t-small text-muted measure">{t('manage.changeHint')}</p>
        </div>

        <div className="max-w-md">
          <MonthCalendar
            first={branch.first}
            last={branch.last}
            today={branch.first}
            value={date}
            states={branch.days}
            notes={branch.notes}
            onSelect={(d) => {
              setDate(d);
              setSlot(null);
            }}
          />
        </div>

        <fieldset className="flex flex-col gap-3">
          <legend className="t-label mb-3 text-muted">{t('fields.party')}</legend>
          {partyRange.min === partyRange.max ? (
            <p className="t-small measure">
              {tu('guests', plural(party, locale))} — {t('manage.partyFixed')}
            </p>
          ) : (
            <>
              <div className="grid grid-cols-4 gap-2 sm:grid-cols-8">
                {parties.map((n) => (
                  <Chip
                    key={n}
                    pressed={party === n}
                    aria-label={tu('guests', plural(n, locale))}
                    className="tabular"
                    onClick={() => {
                      setParty(n);
                      setSlot(null);
                    }}
                  >
                    {formatNumber(n, locale)}
                  </Chip>
                ))}
              </div>
              {partyRange.max < maxPartyOnline ? <p className="t-small text-muted">{t('manage.partyLimit', { n: formatNumber(partyRange.max, locale) })}</p> : null}
            </>
          )}
        </fieldset>

        <fieldset className="flex flex-col gap-3">
          <legend className="t-label mb-3 text-muted">{t('summary.seating')}</legend>
          <div className="flex flex-wrap gap-2">
            {(['any', ...branch.areas] as AreaChoice[]).map((a) => (
              <Chip
                key={a}
                pressed={area === a}
                onClick={() => {
                  setArea(a);
                  setSlot(null);
                }}
              >
                {t(`areas.${a}`)}
              </Chip>
            ))}
          </div>
        </fieldset>

        {date ? (
          <SlotPicker
            data={slots}
            error={state === 'error'}
            periods={branch.periods}
            timeZone={branch.timeZone}
            value={slot?.time ?? (date === current.date && party === current.party && area === current.area ? current.time : null)}
            pending={null}
            onPick={(s) => setSlot(s)}
            onRetry={() => setRefresh((n) => n + 1)}
          />
        ) : (
          <p className="t-small text-muted">{t('date.pick')}</p>
        )}
        {slots && !slots.slots.some((s) => s.available) ? <p className="t-body">{t('time.none')}</p> : null}

        <div className="flex flex-col gap-3">
          {message ? (
            <p role={message.tone === 'error' ? 'alert' : 'status'} className={cn('t-small flex items-start gap-2', message.tone === 'error' ? 'text-danger' : 'text-ink')}>
              {message.tone === 'ok' ? <Icon name="check" size={18} className="mt-0.5 shrink-0" /> : null}
              {message.text}
            </p>
          ) : null}
          <Button size="lg" icon="arrow" disabled={!slot || unchanged || saving} onClick={save} className="w-full sm:w-auto sm:self-start">
            {saving ? tc('actions.saving') : t('manage.save')}
          </Button>
          {slot && unchanged ? <p className="t-small text-muted">{t('manage.unchanged')}</p> : null}
        </div>
      </section>

      <section aria-labelledby={`${id}-cancel`} className="flex flex-col gap-4 border-t border-line pt-8">
        <h2 id={`${id}-cancel`} className="t-heading-md">
          {t('manage.cancel')}
        </h2>
        <p className="t-small text-muted measure">{t('manage.cancelBody')}</p>
        <Button variant="secondary" onClick={() => setConfirming(true)} className="self-start">
          {t('manage.cancel')}
        </Button>
      </section>

      <Dialog
        open={confirming}
        onClose={() => setConfirming(false)}
        title={t('manage.cancelConfirm')}
        closeLabel={tc('a11y.close')}
        footer={
          <div className="flex flex-wrap justify-end gap-3">
            <Button variant="secondary" onClick={() => setConfirming(false)} disabled={cancelling}>
              {t('manage.cancelKeep')}
            </Button>
            <Button onClick={cancel} disabled={cancelling}>
              {cancelling ? tc('actions.saving') : t('manage.cancelYes')}
            </Button>
          </div>
        }
      >
        <div className="flex flex-col gap-3">
          <p className="t-body">{t('manage.cancelBody')}</p>
          {depositNote ? <p className="t-small text-muted">{depositNote}</p> : null}
        </div>
      </Dialog>
    </div>
  );
}
