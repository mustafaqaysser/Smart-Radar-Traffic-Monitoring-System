'use client';

import { BellOff, BellRing, Inbox } from 'lucide-react';
import { useLocale, useTranslations } from 'next-intl';
import { useEffect, useRef, useState } from 'react';
import { setAdminSound } from '@/lib/actions/admin/shell';
import { setOrderingSwitches } from '@/lib/actions/admin/orders';
import type { BoardData } from '@/lib/admin/orders';
import { BOARD_COLUMNS } from '@/lib/admin/order-flow';
import { formatNumber } from '@/lib/i18n/format';
import { cn } from '@/lib/utils/cn';
import { useLive } from '../shell/live';
import { audioReady, chime, unlockAudio } from '../sound';
import { Button } from '../ui/button';
import { Switch } from '../ui/switch';
import { useAdminAction } from '../use-action';
import { OrderCardView } from './order-card';

const FRESH_MS = 90_000;

/**
 * The live orders board. New orders chime (once sound is allowed in this browser), glow for a minute and a
 * half, and the tab title counts what is waiting; every change elsewhere refreshes the columns.
 */
export function OrdersBoard({ data, canManage, soundPreferred, showBranch, timeZone }: { data: BoardData; canManage: boolean; soundPreferred: boolean; showBranch: boolean; timeZone: string }) {
  const t = useTranslations('admin.orders.board');
  const locale = useLocale();
  const live = useLive();
  const [sound, setSound] = useState(soundPreferred);
  const [locked, setLocked] = useState(false);
  const [fresh, setFresh] = useState<Map<string, number>>(new Map());
  const seen = useRef(new Set(data.active.filter((o) => o.status === 'placed').map((o) => o.id)));
  const [, run] = useAdminAction();

  // A remembered "sound on" still needs one click or key press after each page load (browser autoplay rules).
  useEffect(() => {
    if (!sound) return;
    let cancelled = false;
    const unlock = () => {
      void unlockAudio().then((ok) => !cancelled && setLocked(!ok));
    };
    if (!audioReady()) {
      const timer = window.setTimeout(() => !cancelled && setLocked(!audioReady()), 0);
      window.addEventListener('pointerdown', unlock, { once: true });
      window.addEventListener('keydown', unlock, { once: true });
      return () => {
        cancelled = true;
        window.clearTimeout(timer);
        window.removeEventListener('pointerdown', unlock);
        window.removeEventListener('keydown', unlock);
      };
    }
    return () => {
      cancelled = true;
    };
  }, [sound]);

  // Newly placed orders: chime and glow.
  const placed = live?.orders?.placed;
  useEffect(() => {
    if (!placed) return;
    const arrived = placed.filter((id) => !seen.current.has(id));
    if (!arrived.length) return;
    for (const id of arrived) seen.current.add(id);
    if (sound) chime(arrived.length > 1 ? 2 : 1);
    const at = Date.now();
    const timer = window.setTimeout(() => setFresh((prev) => new Map([...prev, ...arrived.map((id) => [id, at] as const)])), 0);
    return () => window.clearTimeout(timer);
  }, [placed, sound]);

  // Glow fades after a while.
  useEffect(() => {
    if (!fresh.size) return;
    const timer = window.setInterval(() => {
      setFresh((prev) => {
        const next = new Map([...prev].filter(([, at]) => Date.now() - at < FRESH_MS));
        return next.size === prev.size ? prev : next;
      });
    }, 5000);
    return () => window.clearInterval(timer);
  }, [fresh.size]);

  // The tab title shows how many orders are waiting, so a background tab still says so.
  const waiting = data.active.filter((o) => o.status === 'placed').length;
  useEffect(() => {
    const base = document.title.replace(/^\(\S+\)\s/, '');
    document.title = waiting ? `(${formatNumber(waiting, locale)}) ${base}` : base;
    return () => {
      document.title = document.title.replace(/^\(\S+\)\s/, '');
    };
  }, [waiting, locale]);

  const toggleSound = async () => {
    const next = !sound;
    if (next) {
      const ok = await unlockAudio();
      setLocked(!ok);
      if (ok) chime();
    }
    setSound(next);
    void run(() => setAdminSound(next));
  };

  return (
    <div className="flex flex-col gap-4">
      <div className="flex flex-wrap items-center gap-x-6 gap-y-3 rounded-soft border border-line bg-raised px-4 py-3">
        <Button variant={sound ? 'secondary' : 'outline'} size="sm" onClick={toggleSound} aria-pressed={sound}>
          {sound ? <BellRing aria-hidden="true" /> : <BellOff aria-hidden="true" />}
          {sound ? t('soundOn') : t('soundOff')}
        </Button>
        {sound && locked ? <p className="text-xs text-warning">{t('soundLocked')}</p> : null}
        {canManage
          ? data.branches.map((b) => (
              <div key={b.id} className="flex flex-wrap items-center gap-x-5 gap-y-2 text-[0.8125rem]">
                {data.branches.length > 1 ? <span className="font-medium">{b.name}</span> : null}
                <span className="flex items-center gap-2">
                  <label className="flex items-center gap-2">
                    <Switch checked={!b.orderingPaused} onCheckedChange={(on) => run(() => setOrderingSwitches({ branchId: b.id, orderingPaused: !on }), { success: on ? t('resumed', { name: b.name }) : t('paused', { name: b.name }) })} />
                    {t('ordersOpen')}
                  </label>
                  {b.orderingPaused ? <span className="font-medium text-danger">{t('ordersPaused')}</span> : null}
                </span>
                <label className="flex items-center gap-2">
                  <Switch checked={b.busyMode} onCheckedChange={(on) => run(() => setOrderingSwitches({ branchId: b.id, busyMode: on }), { success: on ? t('busyOn', { n: formatNumber(b.busyExtraMinutes, locale) }) : t('busyOff') })} />
                  {t('busy', { n: formatNumber(b.busyExtraMinutes, locale) })}
                </label>
              </div>
            ))
          : null}
        <p className="ms-auto text-xs text-muted">
          {t('tally', { completed: formatNumber(data.doneToday.completed, locale), declined: formatNumber(data.doneToday.rejected + data.doneToday.cancelled, locale) })}
          {data.awaitingPayment ? ` · ${t('awaitingPayment', { n: formatNumber(data.awaitingPayment, locale) })}` : ''}
        </p>
      </div>

      <div className="grid gap-4 lg:grid-cols-3">
        {BOARD_COLUMNS.map((col) => {
          const cards = data.active.filter((o) => col.statuses.includes(o.status));
          return (
            <section key={col.key} aria-labelledby={`col-${col.key}`} className={cn('flex min-w-0 flex-col gap-3 rounded-soft bg-surface/50 p-2.5', col.key === 'new' && cards.length && 'bg-accent/5')}>
              <h2 id={`col-${col.key}`} className="flex items-center justify-between px-1 text-[0.8125rem] font-semibold">
                {t(`columns.${col.key}`)}
                <span className="rounded-pill bg-raised px-2 text-xs font-medium tabular">{formatNumber(cards.length, locale)}</span>
              </h2>
              {cards.length ? (
                cards.map((o) => <OrderCardView key={o.id} order={o} canManage={canManage} fresh={fresh.has(o.id)} showBranch={showBranch} timeZone={timeZone} />)
              ) : (
                <div className="flex flex-col items-center gap-1.5 rounded-soft border border-dashed border-line px-4 py-8 text-center text-[0.8125rem] text-muted">
                  <Inbox className="size-5" aria-hidden="true" />
                  {t(`empty.${col.key}`)}
                </div>
              )}
            </section>
          );
        })}
      </div>
    </div>
  );
}
