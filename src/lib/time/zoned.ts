/**
 * Time-zone arithmetic on top of `Intl`, with no dependencies.
 *
 * Every scheduling decision in the platform happens in the *branch's* IANA time zone. Instants are stored as
 * UTC; wall-clock values ("2026-10-02", "19:30") are interpreted in a named zone with these helpers.
 *
 * DST rules:
 *  - A wall-clock time inside a spring-forward gap does not exist. `zonedTimeToUtc` returns `null` for it in
 *    strict mode, or shifts it forward by the gap in lenient mode.
 *  - A wall-clock time inside a fall-back overlap exists twice. `zonedTimeToUtc` returns the earlier instant by
 *    default, or the later one with `{ prefer: 'later' }`.
 */

export interface ZonedParts {
  year: number;
  month: number; // 1–12
  day: number; // 1–31
  hour: number; // 0–23
  minute: number;
  second: number;
  /** 0 = Sunday … 6 = Saturday */
  weekday: number;
}

const formatterCache = new Map<string, Intl.DateTimeFormat>();

function partsFormatter(timeZone: string): Intl.DateTimeFormat {
  let f = formatterCache.get(timeZone);
  if (!f) {
    f = new Intl.DateTimeFormat('en-US', {
      timeZone,
      hourCycle: 'h23',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      weekday: 'short',
    });
    formatterCache.set(timeZone, f);
  }
  return f;
}

const WEEKDAYS: Record<string, number> = { Sun: 0, Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6 };

/** Wall-clock parts of an instant in a time zone. */
export function getZonedParts(date: Date, timeZone: string): ZonedParts {
  const out: Partial<Record<Intl.DateTimeFormatPartTypes, string>> = {};
  for (const part of partsFormatter(timeZone).formatToParts(date)) out[part.type] = part.value;
  return {
    year: Number(out.year),
    month: Number(out.month),
    day: Number(out.day),
    hour: Number(out.hour),
    minute: Number(out.minute),
    second: Number(out.second),
    weekday: WEEKDAYS[out.weekday ?? 'Sun'] ?? 0,
  };
}

/** Offset of the zone from UTC at an instant, in minutes (e.g. +180 for Asia/Riyadh). */
export function getOffsetMinutes(date: Date, timeZone: string): number {
  const p = getZonedParts(date, timeZone);
  const asUtc = Date.UTC(p.year, p.month - 1, p.day, p.hour, p.minute, p.second);
  const actual = Math.floor(date.getTime() / 1000) * 1000;
  return Math.round((asUtc - actual) / 60000);
}

export interface WallClock {
  year: number;
  month: number;
  day: number;
  hour: number;
  minute: number;
}

/**
 * Converts a wall-clock time in `timeZone` to a UTC instant.
 * Returns `null` for non-existent times (spring-forward gap) unless `lenient` is set.
 */
export function zonedTimeToUtc(
  wall: WallClock,
  timeZone: string,
  options: { prefer?: 'earlier' | 'later'; lenient?: boolean } = {},
): Date | null {
  const target = Date.UTC(wall.year, wall.month - 1, wall.day, wall.hour, wall.minute);
  // Candidate offsets: the zone's offsets a day before and after cover any single transition.
  const offsets = new Set<number>();
  for (const probe of [target - 36e5 * 26, target, target + 36e5 * 26]) {
    offsets.add(getOffsetMinutes(new Date(probe), timeZone));
  }
  const matches: number[] = [];
  for (const offset of offsets) {
    const instant = target - offset * 60000;
    const p = getZonedParts(new Date(instant), timeZone);
    if (p.year === wall.year && p.month === wall.month && p.day === wall.day && p.hour === wall.hour && p.minute === wall.minute) {
      matches.push(instant);
    }
  }
  if (matches.length > 0) {
    matches.sort((a, b) => a - b);
    const chosen = options.prefer === 'later' ? matches[matches.length - 1] : matches[0];
    return new Date(chosen as number);
  }
  if (!options.lenient) return null;
  // In a gap: use the offset in force *before* the transition, which lands just after the gap.
  const before = getOffsetMinutes(new Date(target - 36e5 * 26), timeZone);
  return new Date(target - before * 60000);
}

/** 'YYYY-MM-DD' of an instant in a time zone. */
export function toDateString(date: Date, timeZone: string): string {
  const p = getZonedParts(date, timeZone);
  return `${p.year}-${pad(p.month)}-${pad(p.day)}`;
}

/** Minutes since local midnight of an instant in a time zone. */
export function toLocalMinutes(date: Date, timeZone: string): number {
  const p = getZonedParts(date, timeZone);
  return p.hour * 60 + p.minute;
}

export function parseDateString(value: string): { year: number; month: number; day: number } {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(value);
  if (!match) throw new Error(`Invalid date string: ${value}`);
  return { year: Number(match[1]), month: Number(match[2]), day: Number(match[3]) };
}

/** Adds calendar days to a 'YYYY-MM-DD' string (zone-independent). */
export function addDays(dateString: string, days: number): string {
  const { year, month, day } = parseDateString(dateString);
  const d = new Date(Date.UTC(year, month - 1, day + days));
  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}`;
}

/** Weekday (0 = Sunday) of a 'YYYY-MM-DD' calendar date. */
export function weekdayOf(dateString: string): number {
  const { year, month, day } = parseDateString(dateString);
  return new Date(Date.UTC(year, month - 1, day)).getUTCDay();
}

/** Whole days from `a` to `b` (both 'YYYY-MM-DD'). */
export function daysBetween(a: string, b: string): number {
  const pa = parseDateString(a);
  const pb = parseDateString(b);
  return Math.round((Date.UTC(pb.year, pb.month - 1, pb.day) - Date.UTC(pa.year, pa.month - 1, pa.day)) / 864e5);
}

/** 'HH:mm' → minutes since midnight. Accepts '24:00' as 1440. */
export function parseTime(value: string): number {
  const match = /^(\d{1,2}):(\d{2})$/.exec(value);
  if (!match) throw new Error(`Invalid time: ${value}`);
  const h = Number(match[1]);
  const m = Number(match[2]);
  if (h > 24 || m > 59 || (h === 24 && m !== 0)) throw new Error(`Invalid time: ${value}`);
  return h * 60 + m;
}

/** Minutes since midnight → 'HH:mm' (wraps past 24h). */
export function formatTime(minutes: number): string {
  const m = ((minutes % 1440) + 1440) % 1440;
  return `${pad(Math.floor(m / 60))}:${pad(m % 60)}`;
}

/**
 * Converts a local date + minutes-from-midnight (may exceed 1440 for after-midnight times belonging to the
 * previous service day) to a UTC instant in `timeZone`.
 */
export function localToUtc(dateString: string, minutes: number, timeZone: string, lenient = false): Date | null {
  const dayOffset = Math.floor(minutes / 1440);
  const inDay = minutes - dayOffset * 1440;
  const { year, month, day } = parseDateString(dayOffset ? addDays(dateString, dayOffset) : dateString);
  return zonedTimeToUtc({ year, month, day, hour: Math.floor(inDay / 60), minute: inDay % 60 }, timeZone, { lenient });
}

function pad(n: number): string {
  return String(n).padStart(2, '0');
}
