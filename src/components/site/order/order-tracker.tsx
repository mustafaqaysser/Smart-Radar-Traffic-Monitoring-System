'use client';

import { useRouter } from 'next/navigation';
import { useLocale, useTranslations } from 'next-intl';
import { useEffect, useRef, useState } from 'react';
import { Icon } from '@/components/brand/icon';
import { SplitWords } from '@/components/motion/split-words';
import { Button } from '@/components/site/ui/button';
import { cart } from '@/lib/cart/store';
import { formatClock } from '@/lib/i18n/format';
import { LAST_ORDER_KEY } from '@/lib/order/types';
import type { TrackingSnapshot } from '@/lib/server/orders';
import { cn } from '@/lib/utils/cn';
import { useReorder } from './use-reorder';

type Channel = 'delivery' | 'pickup' | 'dine_in';
type Step = 'placed' | 'accepted' | 'preparing' | 'ready' | 'out_for_delivery' | 'completed';

function stepsFor(channel: Channel): Step[] {
  return channel === 'delivery' ? ['placed', 'accepted', 'preparing', 'out_for_delivery', 'completed'] : ['placed', 'accepted', 'preparing', 'ready', 'completed'];
}

interface OrderTrackerProps {
  number: string;
  token: string;
  channel: Channel;
  timeZone: string;
  initial: TrackingSnapshot;
  canReorder: boolean;
}

/**
 * The order's progress, live: the page listens to the kitchen (Server-Sent Events) and moves the sun along
 * the steps as the order is accepted, cooked, and sent or made ready.
 */
export function OrderTracker({ number, token, channel, timeZone, initial, canReorder }: OrderTrackerProps) {
  const t = useTranslations('order.track');
  const locale = useLocale();
  const router = useRouter();
  const [snapshot, setSnapshot] = useState(initial);
  const [reordering, reorder] = useReorder(number, token);
  const startedAs = useRef(initial.status);

  useEffect(() => {
    if (['completed', 'rejected', 'cancelled'].includes(startedAs.current)) return undefined;
    let previous = startedAs.current;
    const source = new EventSource(`/api/orders/${encodeURIComponent(number)}/stream?token=${encodeURIComponent(token)}`);
    source.addEventListener('status', (e) => {
      const next = JSON.parse((e as MessageEvent<string>).data) as TrackingSnapshot;
      setSnapshot(next);
      if (['completed', 'rejected', 'cancelled'].includes(next.status)) source.close();
      // Paid elsewhere (another tab, a late webhook): refresh the rest of the page to drop the payment form.
      if (previous === 'pending_payment' && next.status !== 'pending_payment') router.refresh();
      previous = next.status;
    });
    return () => source.close();
  }, [number, token, router]);

  // The basket empties once the order placed from it is on its way.
  useEffect(() => {
    if (snapshot.status === 'pending_payment') return;
    try {
      if (window.sessionStorage.getItem(LAST_ORDER_KEY) === number) {
        cart.clear();
        window.sessionStorage.removeItem(LAST_ORDER_KEY);
      }
    } catch {
      // No session storage: nothing to tidy.
    }
  }, [snapshot.status, number]);

  const steps = stepsFor(channel);
  const status = snapshot.status;
  const failed = status === 'rejected' || status === 'cancelled';
  const index = status === 'ready' && channel === 'delivery' ? steps.indexOf('preparing') : steps.indexOf(status as Step);
  const reached = (s: Step) => snapshot.events.find((e) => e.status === s)?.at ?? null;
  const titleKey = status === 'ready' && channel === 'dine_in' ? 'ready_dine_in' : status;
  const eta = snapshot.promisedAt ? formatClock(new Date(snapshot.promisedAt), locale, timeZone) : null;

  return (
    <div className="flex flex-col gap-8">
      <h1 className="t-display-lg" aria-live="polite">
        <SplitWords key={titleKey} text={t(`titles.${titleKey}`)} />
      </h1>
      {!failed && status !== 'completed' && eta ? (
        <p className="t-body-lg flex items-center gap-3">
          <Icon name="clock" size={22} />
          {channel === 'delivery' ? t('deliveryEta', { time: eta }) : t('pickupEta', { time: eta })}
        </p>
      ) : null}

      {!failed && status !== 'pending_payment' ? (
        <ol className="flex flex-col gap-0 border-s border-line ps-6 sm:flex-row sm:border-s-0 sm:border-t sm:ps-0 sm:pt-6">
          {steps.map((s, i) => {
            const done = i < index || status === 'completed';
            const current = i === index && status !== 'completed';
            const at = reached(s);
            const label = s === 'completed' ? t(`steps.completed_${channel}`) : t(`steps.${s}`);
            return (
              <li key={s} className="relative flex flex-1 flex-col gap-1 pb-6 sm:pb-0 sm:pe-4" aria-current={current ? 'step' : undefined}>
                <span
                  aria-hidden="true"
                  className={cn(
                    'absolute -start-[1.95rem] top-1 size-3.5 rounded-full border sm:start-0 sm:-top-[2.05rem]',
                    done && 'border-ink bg-ink',
                    current && 'border-sun bg-sun shadow-[0_0_0_6px_color-mix(in_oklab,var(--c-sun)_28%,transparent)] motion-safe:animate-pulse',
                    !done && !current && 'border-line bg-bg',
                  )}
                />
                <span className={cn('t-heading-sm', !done && !current && 'text-muted')}>{label}</span>
                {at && (done || current) ? <span className="t-small tabular text-muted">{formatClock(new Date(at), locale, timeZone)}</span> : null}
              </li>
            );
          })}
        </ol>
      ) : null}

      {!failed && status !== 'completed' ? <p className="t-small text-muted">{t('live')}</p> : null}

      {canReorder && (status === 'completed' || failed) ? (
        <Button variant="secondary" leadingIcon="refresh" onClick={reorder} disabled={reordering} className="self-start">
          {t('reorder')}
        </Button>
      ) : null}
    </div>
  );
}
