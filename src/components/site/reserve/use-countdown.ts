'use client';

import { useEffect, useRef, useState } from 'react';
import { formatNumber } from '@/lib/i18n/format';

/**
 * Seconds left until `expiresAt` (ISO), ticking once a second; null when there is nothing to count down.
 * `onExpire` runs once when the countdown reaches zero.
 */
export function useCountdown(expiresAt: string | null, onExpire?: () => void): number | null {
  const [tick, setTick] = useState<{ at: string; seconds: number } | null>(null);
  const expireRef = useRef(onExpire);
  useEffect(() => {
    expireRef.current = onExpire;
  });

  useEffect(() => {
    if (!expiresAt) return undefined;
    const deadline = Date.parse(expiresAt);
    let fired = false;
    const update = () => {
      const seconds = Math.max(0, Math.round((deadline - Date.now()) / 1000));
      setTick({ at: expiresAt, seconds });
      if (seconds === 0 && !fired) {
        fired = true;
        window.clearInterval(interval);
        expireRef.current?.();
      }
    };
    const interval = window.setInterval(update, 1000);
    const first = window.setTimeout(update, 0);
    return () => {
      window.clearInterval(interval);
      window.clearTimeout(first);
    };
  }, [expiresAt]);

  return expiresAt && tick?.at === expiresAt ? tick.seconds : null;
}

/** 412 → "6:52" in the locale's digits. */
export function formatCountdown(seconds: number, locale: string): string {
  return `${formatNumber(Math.floor(seconds / 60), locale)}:${formatNumber(seconds % 60, locale, { minimumIntegerDigits: 2 })}`;
}
