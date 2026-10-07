'use client';

import { useSyncExternalStore } from 'react';
import { formatNumber } from '@/lib/i18n/format';

const listeners = new Set<() => void>();
let now = 0;
let timer: ReturnType<typeof setInterval> | null = null;

function subscribe(listener: () => void) {
  listeners.add(listener);
  timer ??= setInterval(() => {
    now = Date.now();
    listeners.forEach((l) => l());
  }, 1000);
  return () => {
    listeners.delete(listener);
    if (!listeners.size && timer) {
      clearInterval(timer);
      timer = null;
    }
  };
}

/** A shared one-second clock for timers (0 during server rendering, so timers render on the client only). */
export function useSecondClock(): number {
  return useSyncExternalStore(subscribe, () => now || (now = Date.now()), () => 0);
}

/** Elapsed time as m:ss under an hour and h:mm:ss above, in the locale's digits. */
export function formatElapsed(ms: number, locale: string): string {
  const total = Math.max(0, Math.floor(ms / 1000));
  const h = Math.floor(total / 3600);
  const m = Math.floor((total % 3600) / 60);
  const sec = total % 60;
  const two = (n: number) => formatNumber(n, locale, { minimumIntegerDigits: 2 });
  return h ? `${formatNumber(h, locale)}:${two(m)}:${two(sec)}` : `${formatNumber(m, locale)}:${two(sec)}`;
}
