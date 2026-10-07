'use client';

import { useLocale } from 'next-intl';
import { useSyncExternalStore } from 'react';
import restaurantConfig from '@config';
import { formatDateTime, formatRelativeMinutes } from '@/lib/i18n/format';

const listeners = new Set<() => void>();
let tick = 0;
let timer: ReturnType<typeof setInterval> | null = null;

function subscribe(listener: () => void) {
  listeners.add(listener);
  timer ??= setInterval(() => {
    tick = Date.now();
    listeners.forEach((l) => l());
  }, 30_000);
  return () => {
    listeners.delete(listener);
    if (!listeners.size && timer) {
      clearInterval(timer);
      timer = null;
    }
  };
}

/** A shared clock that ticks every 30 s while something on screen shows a relative time. */
export function useMinuteClock(): number {
  return useSyncExternalStore(subscribe, () => tick || (tick = Date.now()), () => 0);
}

/** "5 minutes ago", kept fresh; the exact time is in the tooltip and the datetime attribute. */
export function RelativeTime({ iso, className, timeZone = restaurantConfig.defaultTimeZone }: { iso: string; className?: string; timeZone?: string }) {
  const locale = useLocale();
  const now = useMinuteClock();
  const at = new Date(iso);
  const minutes = now ? (at.getTime() - now) / 60_000 : null;
  return (
    <time dateTime={iso} className={className} title={formatDateTime(at, locale, timeZone)} suppressHydrationWarning>
      {minutes === null ? '' : formatRelativeMinutes(Math.min(0, minutes), locale)}
    </time>
  );
}
