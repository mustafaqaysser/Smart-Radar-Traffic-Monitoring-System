'use client';

import { useLocale, useTranslations } from 'next-intl';
import { useEffect, useId, useRef, useState, type KeyboardEvent } from 'react';
import { Icon } from '@/components/brand/icon';
import { formatDateString, formatNumber, formatWeekday } from '@/lib/i18n/format';
import type { DayState } from '@/lib/reserve/types';
import { addDays, weekdayOf } from '@/lib/time/zoned';
import { cn } from '@/lib/utils/cn';

/** Weeks start on Sunday, as they do in Saudi Arabia. */
const WEEK_START = 0;

const pad = (n: number) => String(n).padStart(2, '0');
const monthOf = (date: string) => date.slice(0, 7);

function shiftMonth(month: string, delta: number): string {
  const [y, m] = month.split('-').map(Number) as [number, number];
  const total = y * 12 + (m - 1) + delta;
  return `${Math.floor(total / 12)}-${pad((total % 12) + 1)}`;
}

function daysInMonth(month: string): number {
  const [y, m] = month.split('-').map(Number) as [number, number];
  return new Date(Date.UTC(y, m, 0)).getUTCDate();
}

/** The same day of the month in another month (clamped to that month's length). */
function sameDayIn(date: string, deltaMonths: number): string {
  const target = shiftMonth(monthOf(date), deltaMonths);
  return `${target}-${pad(Math.min(Number(date.slice(8, 10)), daysInMonth(target)))}`;
}

interface MonthCalendarProps {
  first: string;
  last: string;
  today: string;
  value: string | null;
  states: Record<string, DayState>;
  notes?: Record<string, string>;
  onSelect: (date: string) => void;
}

/**
 * An accessible month grid (WAI-ARIA grid pattern): arrow keys move by day and week (mirrored in Arabic),
 * Home/End jump within the week, Page Up/Down change month; closed days are announced and skipped.
 */
export function MonthCalendar({ first, last, today, value, states, notes, onSelect }: MonthCalendarProps) {
  const t = useTranslations('reserve.date');
  const locale = useLocale();
  const rtl = locale === 'ar';
  const headingId = useId();
  const [month, setMonth] = useState(() => monthOf(value ?? first));
  const [focusDate, setFocusDate] = useState(value ?? first);
  const grid = useRef<HTMLTableElement>(null);
  const wantFocus = useRef(false);

  useEffect(() => {
    if (!wantFocus.current) return;
    wantFocus.current = false;
    grid.current?.querySelector<HTMLButtonElement>(`[data-date="${focusDate}"]`)?.focus();
  }, [focusDate, month]);

  const clamp = (d: string) => (d < first ? first : d > last ? last : d);

  const moveFocus = (d: string) => {
    const next = clamp(d);
    wantFocus.current = true;
    setFocusDate(next);
    if (monthOf(next) !== month) setMonth(monthOf(next));
  };

  const changeMonth = (delta: number) => {
    const next = shiftMonth(month, delta);
    setMonth(next);
    setFocusDate(clamp(sameDayIn(focusDate, delta)));
  };

  const onKey = (e: KeyboardEvent<HTMLButtonElement>, date: string) => {
    const offset = (weekdayOf(date) - WEEK_START + 7) % 7;
    const keys: Record<string, string> = {
      [rtl ? 'ArrowLeft' : 'ArrowRight']: addDays(date, 1),
      [rtl ? 'ArrowRight' : 'ArrowLeft']: addDays(date, -1),
      ArrowDown: addDays(date, 7),
      ArrowUp: addDays(date, -7),
      Home: addDays(date, -offset),
      End: addDays(date, 6 - offset),
      PageDown: sameDayIn(date, 1),
      PageUp: sameDayIn(date, -1),
    };
    const target = keys[e.key];
    if (!target) return;
    e.preventDefault();
    moveFocus(target);
  };

  const lead = (weekdayOf(`${month}-01`) - WEEK_START + 7) % 7;
  const cells: (string | null)[] = [...Array.from({ length: lead }, () => null), ...Array.from({ length: daysInMonth(month) }, (_, i) => `${month}-${pad(i + 1)}`)];
  while (cells.length % 7) cells.push(null);
  const weeks = Array.from({ length: cells.length / 7 }, (_, w) => cells.slice(w * 7, w * 7 + 7));
  const weekdays = Array.from({ length: 7 }, (_, i) => (WEEK_START + i) % 7);
  const separator = rtl ? '، ' : ', ';
  const focusInMonth = monthOf(focusDate) === month ? focusDate : clamp(`${month}-01`);

  return (
    <div className="flex flex-col gap-4">
      <div className="flex items-center justify-between gap-4">
        <h3 id={headingId} className="t-heading-sm" aria-live="polite">
          {formatDateString(`${month}-01`, locale, { month: 'long', year: 'numeric' })}
        </h3>
        <div className="flex gap-1">
          <button type="button" onClick={() => changeMonth(-1)} disabled={month <= monthOf(first)} className="inline-flex size-11 items-center justify-center border border-line transition-colors hover-capable:hover:border-ink disabled:opacity-35" aria-label={t('previousMonth')}>
            <Icon name="chevron" size={18} className="rotate-180" />
          </button>
          <button type="button" onClick={() => changeMonth(1)} disabled={month >= monthOf(last)} className="inline-flex size-11 items-center justify-center border border-line transition-colors hover-capable:hover:border-ink disabled:opacity-35" aria-label={t('nextMonth')}>
            <Icon name="chevron" size={18} />
          </button>
        </div>
      </div>

      <table ref={grid} role="grid" aria-labelledby={headingId} className="w-full table-fixed border-collapse">
        <thead>
          <tr>
            {weekdays.map((wd) => (
              <th key={wd} scope="col" className="t-label pb-3 text-center font-normal text-muted">
                <abbr title={formatWeekday(wd, locale, 'long')} className="no-underline">
                  {/* Arabic has no short weekday names; the calendar convention is the initial letter. */}
                  {formatWeekday(wd, locale, rtl ? 'narrow' : 'short')}
                </abbr>
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {weeks.map((week, w) => (
            <tr key={w}>
              {week.map((date, i) => {
                if (!date) return <td key={`e${i}`} />;
                const inRange = date >= first && date <= last;
                const day = formatNumber(Number(date.slice(8, 10)), locale);
                if (!inRange) {
                  return (
                    <td key={date} className="p-0.5 text-center">
                      <span className="t-body tabular mx-auto flex aspect-square max-w-14 items-center justify-center text-muted/45" aria-hidden="true">
                        {day}
                      </span>
                    </td>
                  );
                }
                const state = states[date] ?? 'closed';
                const selected = value === date;
                const open = state === 'open';
                const label = [
                  formatDateString(date, locale, { weekday: 'long', day: 'numeric', month: 'long' }),
                  date === today ? t('today') : null,
                  state === 'closed' ? t('closed') : state === 'blocked' ? t('blocked') : null,
                  notes?.[date] ?? null,
                ]
                  .filter(Boolean)
                  .join(separator);
                return (
                  <td key={date} className="p-0.5 text-center">
                    <button
                      type="button"
                      data-date={date}
                      tabIndex={date === focusInMonth ? 0 : -1}
                      aria-label={label}
                      aria-pressed={selected}
                      aria-disabled={!open || undefined}
                      title={notes?.[date]}
                      onKeyDown={(e) => onKey(e, date)}
                      onClick={() => {
                        setFocusDate(date);
                        if (open) onSelect(date);
                      }}
                      className={cn(
                        't-body tabular relative mx-auto flex aspect-square w-full max-w-14 items-center justify-center transition-[background-color,color] duration-[var(--dur-quick)]',
                        open && !selected && 'hover-capable:hover:bg-surface',
                        selected && 'bg-ink text-bg',
                        state === 'closed' && 'cursor-not-allowed bg-[repeating-linear-gradient(135deg,transparent_0_6px,var(--c-line)_6px_7px)] text-muted/70',
                        state === 'blocked' && 'cursor-not-allowed text-muted',
                      )}
                    >
                      <span aria-hidden="true">{day}</span>
                      {date === today ? <span aria-hidden="true" className={cn('absolute bottom-1.5 h-px w-4', selected ? 'bg-bg' : 'bg-accent')} /> : null}
                      {notes?.[date] && open ? <span aria-hidden="true" className={cn('absolute top-1.5 end-1.5 size-1.5 rounded-full', selected ? 'bg-bg' : 'bg-sun')} /> : null}
                    </button>
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
