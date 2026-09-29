import { describe, expect, it } from 'vitest';
import {
  addDays,
  daysBetween,
  formatTime,
  getOffsetMinutes,
  getZonedParts,
  localToUtc,
  parseTime,
  toDateString,
  weekdayOf,
  zonedTimeToUtc,
} from '@/lib/time/zoned';

describe('zoned time', () => {
  it('reads wall-clock parts in a zone', () => {
    const d = new Date('2026-10-01T16:30:00Z');
    expect(getZonedParts(d, 'Asia/Riyadh')).toMatchObject({ year: 2026, month: 10, day: 1, hour: 19, minute: 30, weekday: 4 });
    expect(getZonedParts(d, 'America/New_York')).toMatchObject({ hour: 12, minute: 30 });
  });

  it('computes offsets, including DST', () => {
    expect(getOffsetMinutes(new Date('2026-01-15T12:00:00Z'), 'Asia/Riyadh')).toBe(180);
    expect(getOffsetMinutes(new Date('2026-01-15T12:00:00Z'), 'Europe/London')).toBe(0);
    expect(getOffsetMinutes(new Date('2026-07-15T12:00:00Z'), 'Europe/London')).toBe(60);
  });

  it('converts wall-clock to UTC in a fixed-offset zone', () => {
    expect(zonedTimeToUtc({ year: 2026, month: 10, day: 2, hour: 19, minute: 30 }, 'Asia/Riyadh')?.toISOString()).toBe('2026-10-02T16:30:00.000Z');
  });

  it('returns null for a time inside a spring-forward gap, or shifts it when lenient', () => {
    // Europe/London springs forward at 01:00 → 02:00 on 2026-03-29.
    const wall = { year: 2026, month: 3, day: 29, hour: 1, minute: 30 };
    expect(zonedTimeToUtc(wall, 'Europe/London')).toBeNull();
    expect(zonedTimeToUtc(wall, 'Europe/London', { lenient: true })?.toISOString()).toBe('2026-03-29T01:30:00.000Z');
  });

  it('resolves an ambiguous fall-back time to the earlier or later instant', () => {
    // Europe/London falls back at 02:00 → 01:00 on 2026-10-25; 01:30 happens twice.
    const wall = { year: 2026, month: 10, day: 25, hour: 1, minute: 30 };
    expect(zonedTimeToUtc(wall, 'Europe/London')?.toISOString()).toBe('2026-10-25T00:30:00.000Z');
    expect(zonedTimeToUtc(wall, 'Europe/London', { prefer: 'later' })?.toISOString()).toBe('2026-10-25T01:30:00.000Z');
  });

  it('handles after-midnight minutes as the next calendar day', () => {
    expect(localToUtc('2026-10-02', 24 * 60 + 30, 'Asia/Riyadh')?.toISOString()).toBe('2026-10-02T21:30:00.000Z');
  });

  it('does calendar arithmetic without zones', () => {
    expect(addDays('2026-12-31', 1)).toBe('2027-01-01');
    expect(addDays('2028-03-01', -1)).toBe('2028-02-29');
    expect(daysBetween('2026-09-29', '2026-10-29')).toBe(30);
    expect(weekdayOf('2026-10-02')).toBe(5);
    expect(toDateString(new Date('2026-10-01T22:30:00Z'), 'Asia/Riyadh')).toBe('2026-10-02');
  });

  it('parses and formats clock times', () => {
    expect(parseTime('07:05')).toBe(425);
    expect(parseTime('24:00')).toBe(1440);
    expect(() => parseTime('25:00')).toThrow();
    expect(formatTime(1470)).toBe('00:30');
  });
});
