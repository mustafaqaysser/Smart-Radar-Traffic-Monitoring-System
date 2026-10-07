import { formatClock } from '@/lib/i18n/format';
import { sunPosition } from '@/lib/time/sun';

const r2 = (n: number) => Math.round(n * 100) / 100;
const GNOMON = 30;

/**
 * The sun's path over the house on the day of the booking, with the sun where it will be at the table's hour
 * — or, after maghrib, a lamp below the horizon. A dial, so it reads the same way in both languages.
 */
export function SunArc({ sunrise, sunset, at, lat, lng, timeZone, locale, labels }: { sunrise: Date | null; sunset: Date | null; at: Date; lat: number; lng: number; timeZone: string; locale: string; labels: { sunrise: string; sunset: string } }) {
  const w = 320;
  const h = 176;
  const cx = w / 2;
  const cy = 150;
  const radius = 128;
  const fraction = sunrise && sunset ? (at.getTime() - sunrise.getTime()) / (sunset.getTime() - sunrise.getTime()) : -1;
  const daylight = fraction >= 0 && fraction <= 1;
  const angle = Math.PI * (1 - Math.min(1, Math.max(0, fraction)));
  const sx = r2(cx + radius * Math.cos(angle));
  const sy = r2(cy - radius * Math.sin(angle));
  const left = `${cx - radius} ${cy}`;
  const right = `${cx + radius} ${cy}`;
  const dir = locale === 'ar' ? 'rtl' : 'ltr';
  // Shade length from the sun's real altitude at that hour (long at the ends of the day, short at noon).
  const altitude = Math.max(4, sunPosition(at, lat, lng).altitude) * (Math.PI / 180);
  const length = Math.min(radius - 12, GNOMON / Math.tan(altitude));
  const shadow = sx > cx ? -length : length;
  return (
    <figure className="flex flex-col gap-3" aria-hidden="true">
      <svg viewBox={`0 0 ${w} ${h + 8}`} className="w-full max-w-[22rem]" fill="none">
        <path d={`M${left} A${radius} ${radius} 0 0 1 ${right}`} stroke="var(--c-line)" strokeWidth="1" strokeDasharray="2 6" />
        {daylight ? <path d={`M${left} A${radius} ${radius} 0 0 1 ${sx} ${sy}`} stroke="currentColor" strokeWidth="1.5" /> : null}
        <line x1="8" y1={cy} x2={w - 8} y2={cy} stroke="currentColor" strokeWidth="1" />
        {Array.from({ length: 13 }, (_, i) => {
          const a = Math.PI * (i / 12);
          const x1 = r2(cx + (radius + 6) * Math.cos(a));
          const y1 = r2(cy - (radius + 6) * Math.sin(a));
          const x2 = r2(cx + (radius + (i % 3 === 0 ? 14 : 10)) * Math.cos(a));
          const y2 = r2(cy - (radius + (i % 3 === 0 ? 14 : 10)) * Math.sin(a));
          return <line key={i} x1={x1} y1={y1} x2={x2} y2={y2} stroke="var(--c-muted)" strokeWidth="0.75" />;
        })}
        {daylight ? (
          <>
            {/* The gnomon at the centre and the shade it casts at that hour, away from the sun. */}
            <line x1={cx} y1={cy} x2={r2(cx + shadow)} y2={cy} stroke="var(--c-shade)" strokeWidth="6" />
            <line x1={cx} y1={cy} x2={cx} y2={cy - GNOMON} stroke="currentColor" strokeWidth="1.5" />
            <circle cx={sx} cy={sy} r="11" fill="var(--c-sun)" />
          </>
        ) : (
          <g transform={`translate(${cx} ${cy + 2})`}>
            <path d="M-7 0h14M-5 0v-12h10V0M-3 -12l3-6 3 6" stroke="currentColor" strokeWidth="1.25" />
            <circle cx="0" cy="-6" r="2" fill="var(--c-sun)" />
          </g>
        )}
      </svg>
      {/* East stays on the left in both languages, like the dial above. */}
      <figcaption dir="ltr" className="t-instrument flex max-w-[22rem] justify-between text-muted">
        <span dir={dir}>
          {labels.sunrise} {sunrise ? <bdi>{formatClock(sunrise, locale, timeZone)}</bdi> : '—'}
        </span>
        <span dir={dir}>
          {labels.sunset} {sunset ? <bdi>{formatClock(sunset, locale, timeZone)}</bdi> : '—'}
        </span>
      </figcaption>
    </figure>
  );
}
