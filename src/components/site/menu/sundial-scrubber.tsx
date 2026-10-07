'use client';

import { useLocale, useTranslations } from 'next-intl';
import { useId, useRef } from 'react';
import { formatNumber, formatWallTime } from '@/lib/i18n/format';
import { formatTime } from '@/lib/time/zoned';
import { cn } from '@/lib/utils/cn';

/** Half-hour steps from 06:00 to 02:00 the next morning. */
export const SCRUB_START = 6 * 60;
export const SCRUB_STEPS = 40;
export const SCRUB_STEP = 30;

export function stepToMinutes(step: number): number {
  return SCRUB_START + step * SCRUB_STEP;
}

export function minutesToStep(minutes: number): number {
  const m = minutes < SCRUB_START ? minutes + 1440 : minutes;
  return Math.max(0, Math.min(SCRUB_STEPS, Math.round((m - SCRUB_START) / SCRUB_STEP)));
}

interface SundialScrubberProps {
  value: number;
  nowStep: number;
  onChange: (step: number) => void;
  /** What is served at the chosen hour (already translated). */
  readout: string;
  className?: string;
}

const R = 150;
const CX = 180;
const CY = 172;

/**
 * The sundial scrubber: a half dial from 06:00 to 02:00. The sun handle can be dragged along the arc, and the
 * same control is a native range input underneath, so it works with a keyboard and assistive technology.
 * Time flows in the reading direction (mirrored in Arabic).
 */
export function SundialScrubber({ value, nowStep, onChange, readout, className }: SundialScrubberProps) {
  const t = useTranslations('menu.scrubber');
  const locale = useLocale();
  const id = useId();
  const svg = useRef<SVGSVGElement>(null);
  const dragging = useRef(false);
  const rtl = locale === 'ar';

  const angleFor = (step: number) => {
    const f = step / SCRUB_STEPS;
    // LTR: from the left horizon (180°) over the top to the right (360°). RTL mirrors.
    const deg = rtl ? 360 - f * 180 : 180 + f * 180;
    return (deg * Math.PI) / 180;
  };
  const point = (step: number, r = R) => {
    const a = angleFor(step);
    // Rounded so server and browser (whose Math.cos may differ in the last digit) render identical markup.
    return { x: Math.round((CX + Math.cos(a) * r) * 100) / 100, y: Math.round((CY + Math.sin(a) * r) * 100) / 100 };
  };

  const fromPointer = (clientX: number, clientY: number) => {
    const el = svg.current;
    if (!el) return;
    const box = el.getBoundingClientRect();
    const x = ((clientX - box.left) / box.width) * 360 - CX;
    const y = ((clientY - box.top) / box.height) * 200 - CY;
    let deg = (Math.atan2(y, x) * 180) / Math.PI; // -180..180, top half is negative
    if (deg > 0) deg = x < 0 ? -180 : 0; // below the horizon: clamp to the nearest end
    const f = (deg + 180) / 180; // 0 at left, 1 at right
    const step = Math.round((rtl ? 1 - f : f) * SCRUB_STEPS);
    onChange(Math.max(0, Math.min(SCRUB_STEPS, step)));
  };

  const handle = point(value);
  const now = point(nowStep, R + 14);
  const time = formatWallTime(formatTime(stepToMinutes(value) % 1440), locale);

  return (
    <div className={cn('sundial relative select-none', className)}>
      <label htmlFor={id} className="sr-only">
        {t('label')}
      </label>
      <svg
        ref={svg}
        viewBox="0 0 360 200"
        className="w-full touch-none overflow-visible"
        aria-hidden="true"
        onPointerDown={(e) => {
          dragging.current = true;
          (e.currentTarget as SVGSVGElement).setPointerCapture(e.pointerId);
          fromPointer(e.clientX, e.clientY);
        }}
        onPointerMove={(e) => dragging.current && fromPointer(e.clientX, e.clientY)}
        onPointerUp={() => (dragging.current = false)}
        onPointerCancel={() => (dragging.current = false)}
      >
        <path d={`M${CX - R} ${CY} A${R} ${R} 0 0 1 ${CX + R} ${CY}`} fill="none" stroke="var(--c-line)" strokeWidth="1.5" />
        <line x1={CX - R - 16} y1={CY} x2={CX + R + 16} y2={CY} stroke="var(--c-ink)" strokeWidth="1.5" />
        {Array.from({ length: SCRUB_STEPS + 1 }, (_, s) => {
          const major = (stepToMinutes(s) % 180) === 0;
          const a = point(s, R - (major ? 12 : 6));
          const b = point(s, R);
          const label = point(s, R - 28);
          const hour = Math.floor((stepToMinutes(s) % 1440) / 60) || 24;
          return (
            <g key={s}>
              <line x1={a.x} y1={a.y} x2={b.x} y2={b.y} stroke="var(--c-muted)" strokeWidth={major ? 1.5 : 1} />
              {major ? (
                <text x={label.x} y={label.y - 8} textAnchor="middle" dominantBaseline="middle" fontSize="12" fill="var(--c-muted)" className="t-instrument">
                  {formatNumber(hour, locale)}
                </text>
              ) : null}
            </g>
          );
        })}
        {/* The gnomon's shadow points away from the chosen sun. */}
        <line x1={CX} y1={CY} x2={CX - (handle.x - CX) * 0.42} y2={CY + Math.abs(handle.y - CY) * 0.06} stroke="var(--c-shade)" strokeWidth="6" strokeLinecap="butt" />
        <line x1={CX} y1={CY} x2={CX} y2={CY - 34} stroke="var(--c-ink)" strokeWidth="3" />
        <circle cx={now.x} cy={now.y} r="3" fill="var(--c-accent)" />
        <circle cx={handle.x} cy={handle.y} r="13" fill="var(--c-sun)" stroke="var(--c-ink)" strokeWidth="1.5" className="cursor-grab" />
      </svg>
      <input
        id={id}
        type="range"
        min={0}
        max={SCRUB_STEPS}
        step={1}
        value={value}
        onChange={(e) => onChange(Number(e.target.value))}
        aria-valuetext={`${time} — ${readout}`}
        className="sundial-range pointer-events-none absolute inset-x-0 top-0 h-11 w-full opacity-0"
        dir={rtl ? 'rtl' : 'ltr'}
      />
      <div className="mt-2 flex flex-col items-center gap-1 text-center">
        <p className="t-heading-md tabular" aria-hidden="true">
          <bdi>{time}</bdi>
        </p>
        <p className="t-small text-muted" aria-live="polite">
          {readout}
        </p>
        {value !== nowStep ? (
          <button type="button" onClick={() => onChange(nowStep)} className="t-label mt-2 min-h-11 underline decoration-line underline-offset-4">
            {t('back')}
          </button>
        ) : (
          <p className="t-label mt-2 flex min-h-11 items-center gap-2 text-accent">
            <span className="size-2 rounded-full bg-accent" aria-hidden="true" />
            {t('now')}
          </p>
        )}
      </div>
    </div>
  );
}
