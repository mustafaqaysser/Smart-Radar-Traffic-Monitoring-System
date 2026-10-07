import type { CSSProperties, ReactNode } from 'react';
import { atmosphereCssVars, computeAtmosphere } from '@/lib/brand/atmosphere';
import { sunTimes } from '@/lib/time/sun';
import { toDateString } from '@/lib/time/zoned';
import { cn } from '@/lib/utils/cn';
import { SunArc } from './sun-arc';

/** A booking's details as a ruled list (confirmation, manage and offer pages). */
export function BookingRows({ rows, className }: { rows: { label: string; value: ReactNode; strong?: boolean }[]; className?: string }) {
  return (
    <dl className={cn('flex flex-col border-t border-ink', className)}>
      {rows.map((row) => (
        <div key={row.label} className="flex items-baseline justify-between gap-6 border-b border-line py-4">
          <dt className="t-label text-muted">{row.label}</dt>
          <dd className={cn('text-end', row.strong ? 't-heading-sm tabular' : 't-body')}>{row.value}</dd>
        </div>
      ))}
    </dl>
  );
}

/**
 * The hour of the booking in its own light: the card takes the palette of that hour and shows the sun's path
 * over the house that day.
 */
export function HourCard({
  at,
  branch,
  locale,
  dateLabel,
  timeLabel,
  lightLabel,
  sunLabels,
  className,
}: {
  at: Date;
  branch: { lat: number; lng: number; timeZone: string };
  locale: string;
  dateLabel: string;
  timeLabel: string;
  lightLabel: (phase: string) => string;
  sunLabels: { sunrise: string; sunset: string };
  className?: string;
}) {
  const atmosphere = computeAtmosphere(at, branch);
  const times = sunTimes(toDateString(at, branch.timeZone), branch.lat, branch.lng);
  return (
    <section data-phase={atmosphere.phase} style={atmosphereCssVars(atmosphere) as CSSProperties} className={cn('flex flex-col gap-6 bg-bg p-7 text-ink cast-shade md:p-9', className)}>
      <div className="flex flex-col gap-2">
        <p className="t-label text-muted">{dateLabel}</p>
        <p className="t-display-md tabular">
          <bdi>{timeLabel}</bdi>
        </p>
        <p className="t-body text-muted">{lightLabel(atmosphere.phase)}</p>
      </div>
      <SunArc sunrise={times.sunrise} sunset={times.sunset} at={at} lat={branch.lat} lng={branch.lng} timeZone={branch.timeZone} locale={locale} labels={sunLabels} />
    </section>
  );
}
