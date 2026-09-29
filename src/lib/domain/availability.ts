/**
 * Reservation availability engine.
 *
 * Real availability is computed from the branch's tables, turn times and pacing:
 *  1. Service periods valid for the service date produce candidate seating times at the slot interval
 *     (holidays close or reshape them; blackout dates block online booking).
 *  2. Each time is converted to an instant in the branch's IANA zone (DST gaps are skipped, repeated
 *     wall-clock times are offered once).
 *  3. Pacing: covers already *starting* in that slot + this party must not exceed the slot's cover cap.
 *  4. Tables: a free table (or a combination of free tables in the same combine group) must fit the party
 *     for the whole turn time, which depends on party size.
 *
 * Existing reservations and unexpired holds both occupy tables. All functions are pure.
 */
import { parseTime, formatTime, localToUtc, weekdayOf } from '../time/zoned';
import type { DayRange } from './hours';

export interface TableInfo {
  id: string;
  area: string;
  minSeats: number;
  maxSeats: number;
  combineGroup: string | null;
  reservable: boolean;
  isActive: boolean;
  sortOrder: number;
}

export interface ServicePeriodInfo {
  key: string;
  weekdays: number[];
  start: string; // first seating 'HH:mm'
  end: string; // last seating 'HH:mm' (may be after midnight)
  maxCoversPerSlot?: number | null;
}

export interface BookingInfo {
  id: string;
  startsAt: Date;
  endsAt: Date;
  partySize: number;
  tableIds: string[];
}

export interface ReservationRules {
  slotIntervalMinutes: number;
  turnTimes: Record<string, number>;
  maxCoversPerSlot: number;
  /** Seating must start at least this long before the venue closes. */
  lastSeatingBeforeCloseMinutes: number;
}

export interface DayOverride {
  closed: boolean;
  /** When set, seatings must fall inside these ranges (minutes from local midnight). */
  ranges: DayRange[] | null;
  reservationsBlocked: boolean;
}

export interface AvailabilityQuery {
  date: string;
  partySize: number;
  area?: string; // 'any' or a table area
  now: Date;
  /** Minimum notice for online bookings. */
  minLeadMinutes?: number;
  /** Ignore this reservation/hold (when modifying a booking or confirming a hold). */
  excludeIds?: string[];
}

export interface AvailabilityContext {
  timeZone: string;
  rules: ReservationRules;
  periods: ServicePeriodInfo[];
  tables: TableInfo[];
  bookings: BookingInfo[];
  override?: DayOverride | null;
}

export type SlotReason = 'past' | 'pacing' | 'full';

export interface Slot {
  /** 'HH:mm' wall-clock label (after-midnight seatings show their clock time). */
  time: string;
  /** Minutes from the service date's midnight (≥ 1440 after midnight). */
  minutes: number;
  periodKey: string;
  startsAt: Date;
  endsAt: Date;
  available: boolean;
  reason?: SlotReason;
  /** Tables assigned for the requested area ('any' → best fit anywhere). */
  tableIds: string[];
  /** Areas that could seat this party at this time. */
  areas: string[];
}

/** Turn time for a party: the entry with the largest party-size key not above the party. */
export function turnTimeFor(partySize: number, turnTimes: Record<string, number>): number {
  const keys = Object.keys(turnTimes)
    .map(Number)
    .filter((k) => Number.isFinite(k))
    .sort((a, b) => a - b);
  let turn = turnTimes[String(keys[0] ?? 2)] ?? 90;
  for (const k of keys) if (k <= partySize) turn = turnTimes[String(k)] ?? turn;
  return turn;
}

function overlaps(aStart: Date, aEnd: Date, bStart: Date, bEnd: Date): boolean {
  return aStart < bEnd && aEnd > bStart;
}

/**
 * Best table assignment for a party among free tables: the tightest single table, else the smallest
 * combination (2–4 tables) within one combine group.
 */
export function assignTables(partySize: number, freeTables: TableInfo[]): string[] | null {
  const singles = freeTables
    .filter((t) => t.minSeats <= partySize && partySize <= t.maxSeats)
    .sort((a, b) => a.maxSeats - b.maxSeats || a.sortOrder - b.sortOrder);
  if (singles[0]) return [singles[0].id];

  const groups = new Map<string, TableInfo[]>();
  for (const t of freeTables) {
    if (!t.combineGroup) continue;
    const list = groups.get(t.combineGroup) ?? [];
    list.push(t);
    groups.set(t.combineGroup, list);
  }
  let best: { ids: string[]; capacity: number } | null = null;
  for (const list of groups.values()) {
    const sorted = [...list].sort((a, b) => a.sortOrder - b.sortOrder);
    const n = sorted.length;
    // Enumerate subsets of size 2..4 (tables per group are few).
    const visit = (start: number, chosen: TableInfo[]) => {
      if (chosen.length >= 2) {
        const capacity = chosen.reduce((s, t) => s + t.maxSeats, 0);
        const minSeats = Math.min(...chosen.map((t) => t.minSeats));
        if (capacity >= partySize && partySize >= minSeats) {
          if (!best || capacity < best.capacity || (capacity === best.capacity && chosen.length < best.ids.length)) {
            best = { ids: chosen.map((t) => t.id), capacity };
          }
        }
      }
      if (chosen.length === 4) return;
      for (let i = start; i < n; i++) visit(i + 1, [...chosen, sorted[i] as TableInfo]);
    };
    visit(0, []);
  }
  return best ? (best as { ids: string[] }).ids : null;
}

function periodMinutes(period: ServicePeriodInfo): { start: number; end: number } {
  const start = parseTime(period.start);
  let end = parseTime(period.end);
  if (end < start) end += 1440;
  return { start, end };
}

export function computeAvailability(ctx: AvailabilityContext, query: AvailabilityQuery): Slot[] {
  const { timeZone, rules } = ctx;
  if (ctx.override?.closed || ctx.override?.reservationsBlocked) return [];
  const weekday = weekdayOf(query.date);
  const exclude = new Set(query.excludeIds ?? []);
  const bookings = ctx.bookings.filter((b) => !exclude.has(b.id));
  const usableTables = ctx.tables.filter((t) => t.isActive && t.reservable);
  const lead = query.minLeadMinutes ?? 0;
  const turn = turnTimeFor(query.partySize, rules.turnTimes);
  const seen = new Set<number>();
  const slots: Slot[] = [];

  const periods = ctx.periods.filter((p) => p.weekdays.includes(weekday));
  for (const period of periods) {
    const { start, end } = periodMinutes(period);
    for (let m = start; m <= end; m += rules.slotIntervalMinutes) {
      if (ctx.override?.ranges) {
        const inside = ctx.override.ranges.some((r) => m >= r.start && m <= r.end - rules.lastSeatingBeforeCloseMinutes);
        if (!inside) continue;
      }
      const startsAt = localToUtc(query.date, m, timeZone);
      if (!startsAt) continue; // DST gap: the time does not exist
      if (seen.has(startsAt.getTime())) continue; // repeated wall-clock hour or overlapping periods
      seen.add(startsAt.getTime());
      const endsAt = new Date(startsAt.getTime() + turn * 60000);
      const base = { time: formatTime(m), minutes: m, periodKey: period.key, startsAt, endsAt };

      if (startsAt.getTime() < query.now.getTime() + lead * 60000) {
        slots.push({ ...base, available: false, reason: 'past', tableIds: [], areas: [] });
        continue;
      }

      const slotEnd = new Date(startsAt.getTime() + rules.slotIntervalMinutes * 60000);
      const coversStarting = bookings
        .filter((b) => b.startsAt >= startsAt && b.startsAt < slotEnd)
        .reduce((sum, b) => sum + b.partySize, 0);
      const cap = period.maxCoversPerSlot ?? rules.maxCoversPerSlot;
      if (coversStarting + query.partySize > cap) {
        slots.push({ ...base, available: false, reason: 'pacing', tableIds: [], areas: [] });
        continue;
      }

      const occupied = new Set(bookings.filter((b) => overlaps(startsAt, endsAt, b.startsAt, b.endsAt)).flatMap((b) => b.tableIds));
      const free = usableTables.filter((t) => !occupied.has(t.id));
      const areaNames = [...new Set(usableTables.map((t) => t.area))];
      const areas = areaNames.filter((area) => assignTables(query.partySize, free.filter((t) => t.area === area)) !== null);
      const wanted = query.area && query.area !== 'any' ? free.filter((t) => t.area === query.area) : free;
      const tableIds = assignTables(query.partySize, wanted);
      if (!tableIds) {
        slots.push({ ...base, available: false, reason: 'full', tableIds: [], areas });
        continue;
      }
      slots.push({ ...base, available: true, tableIds, areas });
    }
  }
  return slots.sort((a, b) => a.startsAt.getTime() - b.startsAt.getTime());
}

/** Re-validates one specific time (used inside the booking transaction). */
export function checkSlot(ctx: AvailabilityContext, query: AvailabilityQuery & { time: string }): Slot | null {
  const minutes = parseTime(query.time);
  const slots = computeAvailability(ctx, query);
  return slots.find((s) => s.minutes === minutes || s.minutes === minutes + 1440) ?? null;
}
