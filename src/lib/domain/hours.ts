/**
 * Opening-hours engine. Pure functions over plain data, always in the branch's IANA time zone.
 *
 * A day's ranges are expressed in minutes from that day's local midnight; `end` may exceed 1440 when a
 * service runs past midnight (e.g. 19:00–01:30 → 1140–1530). Such minutes belong to the *service day* that
 * started them, which is how guests and staff think about a late dinner.
 */
import { addDays, getZonedParts, localToUtc, parseTime, toDateString, weekdayOf } from '../time/zoned';

export interface WeeklyRange {
  weekday: number; // 0 = Sunday
  opens: string; // 'HH:mm'
  closes: string; // 'HH:mm' — earlier than opens means after midnight
}

export interface SpecialDay {
  date: string; // 'YYYY-MM-DD'
  closed: boolean;
  ranges?: { opens: string; closes: string }[] | null;
  label?: string;
  reservationsBlocked?: boolean;
}

export interface HoursInput {
  timeZone: string;
  weekly: WeeklyRange[];
  specials?: SpecialDay[];
  /** Seasonal replacement hours applying to specific dates (e.g. Ramadan). */
  seasonal?: { startDate: string; endDate: string; ranges: { opens: string; closes: string }[] }[];
}

export interface DayRange {
  start: number;
  end: number;
}

export interface DaySchedule {
  date: string;
  weekday: number;
  closed: boolean;
  ranges: DayRange[];
  special: SpecialDay | null;
  seasonal: boolean;
}

function toRange(opens: string, closes: string): DayRange {
  const start = parseTime(opens);
  let end = parseTime(closes);
  if (end <= start) end += 1440;
  return { start, end };
}

/** The schedule for one service day, applying specials > seasonal > weekly. */
export function scheduleForDate(input: HoursInput, date: string): DaySchedule {
  const weekday = weekdayOf(date);
  const special = input.specials?.find((s) => s.date === date) ?? null;
  if (special) {
    if (special.closed) return { date, weekday, closed: true, ranges: [], special, seasonal: false };
    if (special.ranges && special.ranges.length) {
      return { date, weekday, closed: false, ranges: special.ranges.map((r) => toRange(r.opens, r.closes)).sort((a, b) => a.start - b.start), special, seasonal: false };
    }
  }
  const season = input.seasonal?.find((s) => s.startDate <= date && date <= s.endDate);
  if (season) {
    const ranges = season.ranges.map((r) => toRange(r.opens, r.closes)).sort((a, b) => a.start - b.start);
    return { date, weekday, closed: ranges.length === 0, ranges, special, seasonal: true };
  }
  const ranges = input.weekly
    .filter((w) => w.weekday === weekday)
    .map((w) => toRange(w.opens, w.closes))
    .sort((a, b) => a.start - b.start);
  return { date, weekday, closed: ranges.length === 0, ranges, special, seasonal: false };
}

export interface OpenState {
  open: boolean;
  /** When the current opening ends (if open). */
  closesAt: Date | null;
  /** When the next opening starts (if closed), within the next 14 days. */
  opensAt: Date | null;
  /** Service day that the current/next opening belongs to. */
  serviceDate: string | null;
}

interface Interval {
  start: Date;
  end: Date;
  serviceDate: string;
}

function intervalsForDay(input: HoursInput, date: string): Interval[] {
  const day = scheduleForDate(input, date);
  const out: Interval[] = [];
  for (const r of day.ranges) {
    const start = localToUtc(date, r.start, input.timeZone, true);
    const end = localToUtc(date, r.end, input.timeZone, true);
    if (start && end && end > start) out.push({ start, end, serviceDate: date });
  }
  return out;
}

/** Open/closed state at an instant, considering yesterday's after-midnight service. */
export function openState(input: HoursInput, now: Date): OpenState {
  const today = toDateString(now, input.timeZone);
  const candidates = [addDays(today, -1), today].flatMap((d) => intervalsForDay(input, d));
  const current = candidates.find((i) => i.start <= now && now < i.end);
  if (current) {
    // Merge with an immediately following interval (e.g. 23:00–24:00 then 00:00–02:00).
    let closesAt = current.end;
    const later = [today, addDays(today, 1)].flatMap((d) => intervalsForDay(input, d));
    for (const next of later.sort((a, b) => a.start.getTime() - b.start.getTime())) {
      if (next.start.getTime() === closesAt.getTime()) closesAt = next.end;
    }
    return { open: true, closesAt, opensAt: null, serviceDate: current.serviceDate };
  }
  for (let i = 0; i < 15; i++) {
    const next = intervalsForDay(input, addDays(today, i))
      .filter((iv) => iv.start > now)
      .sort((a, b) => a.start.getTime() - b.start.getTime())[0];
    if (next) return { open: false, closesAt: null, opensAt: next.start, serviceDate: next.serviceDate };
  }
  return { open: false, closesAt: null, opensAt: null, serviceDate: null };
}

/** Seven consecutive service days starting at `fromDate`, for display. */
export function weekAhead(input: HoursInput, fromDate: string, days = 7): DaySchedule[] {
  return Array.from({ length: days }, (_, i) => scheduleForDate(input, addDays(fromDate, i)));
}

/** True when the instant falls within any opening of its (or the previous) service day. */
export function isWithinHours(input: HoursInput, instant: Date): boolean {
  return openState(input, instant).open;
}

/** Local minutes-from-midnight of an instant within the given service date (can exceed 1440). */
export function serviceMinutes(instant: Date, serviceDate: string, timeZone: string): number {
  const p = getZonedParts(instant, timeZone);
  const local = toDateString(instant, timeZone);
  const base = p.hour * 60 + p.minute;
  return local === serviceDate ? base : base + 1440;
}
