'use client';

import { useLocale, useTranslations } from 'next-intl';
import { Icon } from '@/components/brand/icon';
import { Button } from '@/components/site/ui/button';
import { formatClock, formatWallTime } from '@/lib/i18n/format';
import type { PublicSlot, SlotsResponse } from '@/lib/reserve/types';
import { cn } from '@/lib/utils/cn';

interface SlotPickerProps {
  data: SlotsResponse | null;
  error: boolean;
  periods: { key: string; name: string }[];
  timeZone: string;
  value: string | null;
  /** The time currently being held (its chip shows progress). */
  pending: string | null;
  onPick: (slot: PublicSlot) => void;
  onRetry: () => void;
}

/**
 * Seating times grouped by service. Full times stay visible (struck through) so the evening's shape is clear;
 * the time nearest maghrib carries a small setting sun.
 */
export function SlotPicker({ data, error, periods, timeZone, value, pending, onPick, onRetry }: SlotPickerProps) {
  const t = useTranslations('reserve.time');
  const tc = useTranslations('common');
  const locale = useLocale();

  if (error) {
    return (
      <div role="alert" className="flex flex-col items-start gap-4 border-t border-line pt-5">
        <p className="t-body">{t('error')}</p>
        <Button variant="secondary" size="sm" leadingIcon="refresh" onClick={onRetry}>
          {tc('actions.retry')}
        </Button>
      </div>
    );
  }

  if (!data) {
    return (
      <div aria-busy="true" className="flex flex-col gap-3">
        <p className="sr-only" role="status">
          {t('loading')}
        </p>
        <div className="grid grid-cols-3 gap-2 sm:grid-cols-4 md:grid-cols-5" aria-hidden="true">
          {Array.from({ length: 10 }, (_, i) => (
            <span key={i} className="h-11 animate-pulse rounded-pill bg-surface" />
          ))}
        </div>
      </div>
    );
  }

  const sunsetMinutes = data.sunset?.minutes ?? null;
  const nearestSunset =
    sunsetMinutes === null
      ? null
      : data.slots.reduce<PublicSlot | null>((best, s) => {
          const d = Math.abs(s.minutes - sunsetMinutes);
          return d <= 20 && (!best || d < Math.abs(best.minutes - sunsetMinutes)) ? s : best;
        }, null);
  const groups = periods.map((p) => ({ ...p, slots: data.slots.filter((s) => s.periodKey === p.key) })).filter((g) => g.slots.length > 0);

  return (
    <div className="flex flex-col gap-7">
      {data.sunset ? (
        <p className="t-instrument flex items-center gap-2 text-muted">
          <Icon name="sun" size={16} />
          {t('maghrib', { time: formatClock(new Date(data.sunset.at), locale, timeZone) })}
          <span aria-hidden="true">·</span>
          <span>{t('zoneNote')}</span>
        </p>
      ) : null}
      {groups.map((g) => (
        <fieldset key={g.key} className="flex flex-col gap-3">
          <legend className="t-label mb-3 text-muted">{g.name}</legend>
          <div className="grid grid-cols-3 gap-2 sm:grid-cols-4 md:grid-cols-5">
            {g.slots.map((slot) => {
              const label = formatWallTime(slot.time, locale);
              const selected = value === slot.time;
              const busy = pending === slot.time;
              const sunset = nearestSunset?.time === slot.time;
              const unavailable = !slot.available;
              return (
                <button
                  key={slot.time}
                  type="button"
                  data-slot={slot.time}
                  aria-pressed={selected}
                  aria-disabled={unavailable || undefined}
                  aria-busy={busy || undefined}
                  aria-label={unavailable ? t(slot.reason === 'past' ? 'passedLabel' : 'fullLabel', { time: label }) : sunset ? `${label}${locale === 'ar' ? '، ' : ', '}${t('sunset')}` : undefined}
                  disabled={Boolean(pending) && !busy}
                  onClick={() => {
                    if (!unavailable) onPick(slot);
                  }}
                  className={cn(
                    'relative inline-flex min-h-11 items-center justify-center rounded-pill border px-3 text-[0.9375rem] tabular transition-[background-color,color,border-color] duration-[var(--dur-quick)] disabled:opacity-60',
                    selected ? 'border-ink bg-ink text-bg' : 'border-line',
                    !selected && !unavailable && 'hover-capable:hover:border-ink',
                    unavailable && 'cursor-not-allowed border-dashed text-muted line-through decoration-1',
                    busy && 'animate-pulse',
                  )}
                >
                  {sunset ? (
                    <svg aria-hidden="true" viewBox="0 0 24 12" className={cn('absolute -top-2 h-3 w-6', selected ? 'text-accent' : 'text-sun')} fill="currentColor">
                      <path d="M2 12a10 10 0 0 1 20 0z" />
                    </svg>
                  ) : null}
                  <span>{label}</span>
                </button>
              );
            })}
          </div>
        </fieldset>
      ))}
    </div>
  );
}
