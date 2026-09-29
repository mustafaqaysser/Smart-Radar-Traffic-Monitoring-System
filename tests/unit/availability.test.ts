import { describe, expect, it } from 'vitest';
import { assignTables, checkSlot, computeAvailability, turnTimeFor, type AvailabilityContext, type TableInfo } from '@/lib/domain/availability';
import { openState, scheduleForDate, weekAhead, type HoursInput } from '@/lib/domain/hours';

const table = (id: string, area: string, min: number, max: number, group: string | null = null, sortOrder = 0): TableInfo => ({
  id,
  area,
  minSeats: min,
  maxSeats: max,
  combineGroup: group,
  reservable: true,
  isActive: true,
  sortOrder,
});

const TABLES: TableInfo[] = [
  table('c1', 'courtyard', 1, 2, 'c', 1),
  table('c2', 'courtyard', 2, 4, 'c', 2),
  table('c3', 'courtyard', 2, 4, 'c', 3),
  table('l1', 'liwan', 4, 6, null, 4),
  table('p1', 'private', 6, 10, null, 5),
];

const RULES = {
  slotIntervalMinutes: 30,
  turnTimes: { '1': 90, '3': 105, '5': 120, '7': 150 },
  maxCoversPerSlot: 12,
  lastSeatingBeforeCloseMinutes: 60,
};

function ctx(overrides: Partial<AvailabilityContext> = {}): AvailabilityContext {
  return {
    timeZone: 'Asia/Riyadh',
    rules: RULES,
    periods: [{ key: 'dinner', weekdays: [0, 1, 2, 3, 4, 5, 6], start: '19:00', end: '23:30' }],
    tables: TABLES,
    bookings: [],
    override: null,
    ...overrides,
  };
}

const NOW = new Date('2026-10-01T06:00:00Z'); // 09:00 in Riyadh on Thursday 1 Oct

describe('turn times', () => {
  it('uses the largest party-size key not above the party', () => {
    expect(turnTimeFor(2, RULES.turnTimes)).toBe(90);
    expect(turnTimeFor(4, RULES.turnTimes)).toBe(105);
    expect(turnTimeFor(6, RULES.turnTimes)).toBe(120);
    expect(turnTimeFor(12, RULES.turnTimes)).toBe(150);
  });
});

describe('table assignment', () => {
  it('prefers the tightest single table', () => {
    expect(assignTables(2, TABLES)).toEqual(['c1']);
    expect(assignTables(3, TABLES)).toEqual(['c2']);
    expect(assignTables(5, TABLES)).toEqual(['l1']);
  });
  it('combines tables within one group when no single table fits', () => {
    const free = TABLES.filter((t) => t.id !== 'l1' && t.id !== 'p1');
    expect(assignTables(8, free)?.sort()).toEqual(['c2', 'c3']);
    expect(assignTables(9, free)?.sort()).toEqual(['c1', 'c2', 'c3']);
    expect(assignTables(11, free)).toBeNull();
  });
});

describe('availability', () => {
  it('generates slots across the service period at the slot interval', () => {
    const slots = computeAvailability(ctx(), { date: '2026-10-02', partySize: 2, now: NOW });
    expect(slots.map((s) => s.time)).toEqual(['19:00', '19:30', '20:00', '20:30', '21:00', '21:30', '22:00', '22:30', '23:00', '23:30']);
    expect(slots.every((s) => s.available)).toBe(true);
    expect(slots[0]?.startsAt.toISOString()).toBe('2026-10-02T16:00:00.000Z');
    expect(slots[0]?.endsAt.toISOString()).toBe('2026-10-02T17:30:00.000Z');
  });

  it('marks past slots and respects minimum lead time', () => {
    const now = new Date('2026-10-02T16:10:00Z'); // 19:10 local
    const slots = computeAvailability(ctx(), { date: '2026-10-02', partySize: 2, now, minLeadMinutes: 30 });
    expect(slots.find((s) => s.time === '19:30')?.reason).toBe('past');
    expect(slots.find((s) => s.time === '20:00')?.available).toBe(true);
  });

  it('blocks a table for the full turn time of an overlapping booking', () => {
    const booking = { id: 'r1', startsAt: new Date('2026-10-02T16:00:00Z'), endsAt: new Date('2026-10-02T17:30:00Z'), partySize: 2, tableIds: ['c1'] };
    const only = ctx({ tables: [TABLES[0] as TableInfo], bookings: [booking] });
    const slots = computeAvailability(only, { date: '2026-10-02', partySize: 2, now: NOW });
    expect(slots.find((s) => s.time === '19:00')?.reason).toBe('full');
    expect(slots.find((s) => s.time === '20:00')?.reason).toBe('full'); // 20:00 < 20:30 end
    expect(slots.find((s) => s.time === '20:30')?.available).toBe(true);
  });

  it('enforces pacing (max covers starting per slot)', () => {
    const bookings = [
      { id: 'a', startsAt: new Date('2026-10-02T17:00:00Z'), endsAt: new Date('2026-10-02T18:45:00Z'), partySize: 6, tableIds: ['l1'] },
      { id: 'b', startsAt: new Date('2026-10-02T17:00:00Z'), endsAt: new Date('2026-10-02T18:45:00Z'), partySize: 4, tableIds: ['c2'] },
    ];
    const slots = computeAvailability(ctx({ bookings }), { date: '2026-10-02', partySize: 4, now: NOW });
    expect(slots.find((s) => s.time === '20:00')?.reason).toBe('pacing'); // 10 + 4 > 12
    expect(slots.find((s) => s.time === '20:30')?.available).toBe(true);
  });

  it('respects area preferences and reports available areas', () => {
    const slots = computeAvailability(ctx(), { date: '2026-10-02', partySize: 5, area: 'liwan', now: NOW });
    expect(slots[0]?.tableIds).toEqual(['l1']);
    expect(slots[0]?.areas.sort()).toEqual(['courtyard', 'liwan']); // the private room seats 6–10
    const courtyardOnly = computeAvailability(ctx({ tables: TABLES.filter((t) => t.area === 'courtyard') }), { date: '2026-10-02', partySize: 11, now: NOW });
    expect(courtyardOnly.every((s) => s.reason === 'full')).toBe(true);
  });

  it('treats unexpired holds like bookings, and ignores excluded ids (modify flow)', () => {
    const hold = { id: 'hold-1', startsAt: new Date('2026-10-02T16:00:00Z'), endsAt: new Date('2026-10-02T17:30:00Z'), partySize: 2, tableIds: ['c1'] };
    const only = ctx({ tables: [TABLES[0] as TableInfo], bookings: [hold] });
    expect(checkSlot(only, { date: '2026-10-02', time: '19:00', partySize: 2, now: NOW })?.available).toBe(false);
    expect(checkSlot(only, { date: '2026-10-02', time: '19:00', partySize: 2, now: NOW, excludeIds: ['hold-1'] })?.available).toBe(true);
  });

  it('closes on holidays and blackout dates, and reshapes on special hours', () => {
    expect(computeAvailability(ctx({ override: { closed: true, ranges: null, reservationsBlocked: false } }), { date: '2026-10-02', partySize: 2, now: NOW })).toEqual([]);
    expect(computeAvailability(ctx({ override: { closed: false, ranges: null, reservationsBlocked: true } }), { date: '2026-10-02', partySize: 2, now: NOW })).toEqual([]);
    const early = computeAvailability(ctx({ override: { closed: false, ranges: [{ start: 18 * 60, end: 21 * 60 }], reservationsBlocked: false } }), {
      date: '2026-10-02',
      partySize: 2,
      now: NOW,
    });
    expect(early.map((s) => s.time)).toEqual(['19:00', '19:30', '20:00']);
  });

  it('crosses midnight: late seatings belong to the service day and land on the next calendar date', () => {
    const late = ctx({ periods: [{ key: 'late', weekdays: [5], start: '23:00', end: '01:00' }] });
    const slots = computeAvailability(late, { date: '2026-10-02', partySize: 2, now: NOW });
    expect(slots.map((s) => s.time)).toEqual(['23:00', '23:30', '00:00', '00:30', '01:00']);
    const afterMidnight = slots.find((s) => s.time === '00:30');
    expect(afterMidnight?.minutes).toBe(1470);
    expect(afterMidnight?.startsAt.toISOString()).toBe('2026-10-02T21:30:00.000Z'); // 00:30 on 3 Oct, Riyadh
    // Service-day weekday rules: the same period does not run on Thursday (weekday 4).
    expect(computeAvailability(late, { date: '2026-10-01', partySize: 2, now: NOW })).toEqual([]);
  });

  it('skips non-existent times in a DST spring-forward gap', () => {
    const london = ctx({ timeZone: 'Europe/London', periods: [{ key: 'late', weekdays: [6], start: '23:30', end: '02:30' }] });
    // Clocks jump 01:00 → 02:00 on Sunday 29 March 2026; service day is Saturday 28 March.
    const slots = computeAvailability(london, { date: '2026-03-28', partySize: 2, now: new Date('2026-03-01T00:00:00Z') });
    expect(slots.map((s) => s.time)).toEqual(['23:30', '00:00', '00:30', '02:00', '02:30']);
    const [a, b] = [slots.find((s) => s.time === '00:30'), slots.find((s) => s.time === '02:00')];
    expect((b!.startsAt.getTime() - a!.startsAt.getTime()) / 60000).toBe(30);
  });

  it('offers a repeated DST fall-back time only once', () => {
    const london = ctx({ timeZone: 'Europe/London', periods: [{ key: 'late', weekdays: [6], start: '23:30', end: '02:00' }] });
    // Clocks go back 02:00 → 01:00 on Sunday 25 October 2026.
    const slots = computeAvailability(london, { date: '2026-10-24', partySize: 2, now: new Date('2026-10-01T00:00:00Z') });
    expect(slots.filter((s) => s.time === '01:30')).toHaveLength(1);
    expect(slots.map((s) => s.time)).toEqual(['23:30', '00:00', '00:30', '01:00', '01:30', '02:00']);
  });
});

describe('opening hours', () => {
  const hours: HoursInput = {
    timeZone: 'Asia/Riyadh',
    weekly: [0, 1, 2, 3, 4, 5, 6].flatMap((weekday) => [
      { weekday, opens: '07:00', closes: '16:00' },
      { weekday, opens: '19:00', closes: '01:00' },
    ]),
    specials: [{ date: '2026-12-02', closed: true, label: 'Private event' }, { date: '2026-10-05', closed: false, ranges: [{ opens: '12:00', closes: '18:00' }] }],
    seasonal: [{ startDate: '2027-02-08', endDate: '2027-03-09', ranges: [{ opens: '17:30', closes: '03:00' }] }],
  };

  it('is open during the day and closed in the afternoon gap', () => {
    expect(openState(hours, new Date('2026-10-01T06:00:00Z')).open).toBe(true); // 09:00
    const gap = openState(hours, new Date('2026-10-01T14:00:00Z')); // 17:00
    expect(gap.open).toBe(false);
    expect(gap.opensAt?.toISOString()).toBe('2026-10-01T16:00:00.000Z'); // 19:00
  });

  it('stays open past midnight as part of the previous service day', () => {
    const late = openState(hours, new Date('2026-10-01T21:30:00Z')); // 00:30 on 2 Oct
    expect(late.open).toBe(true);
    expect(late.serviceDate).toBe('2026-10-01');
    expect(late.closesAt?.toISOString()).toBe('2026-10-01T22:00:00.000Z');
  });

  it('applies holiday closures, special hours and seasonal (Ramadan) hours', () => {
    expect(scheduleForDate(hours, '2026-12-02').closed).toBe(true);
    expect(scheduleForDate(hours, '2026-10-05').ranges).toEqual([{ start: 720, end: 1080 }]);
    const ramadan = scheduleForDate(hours, '2027-02-20');
    expect(ramadan.seasonal).toBe(true);
    expect(ramadan.ranges).toEqual([{ start: 1050, end: 1620 }]);
    expect(weekAhead(hours, '2026-10-01')).toHaveLength(7);
  });
});
