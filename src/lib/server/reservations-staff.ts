import 'server-only';
import { and, asc, eq, gte, inArray, lt, lte, ne, or } from 'drizzle-orm';
import { db } from '@/lib/db/client';
import * as s from '@/lib/db/schema';
import { assignTables, checkSlot, turnTimeFor, type BookingInfo } from '@/lib/domain/availability';
import { serviceMinutes } from '@/lib/domain/hours';
import type { BranchDTO } from '@/lib/queries/types';
import type { AreaChoice, Occasion } from '@/lib/reserve/types';
import { linkToken } from '@/lib/server/tokens';
import { localToUtc, parseTime } from '@/lib/time/zoned';
import { CODE_PREFIX, createCode, createId, sha256 } from '@/lib/utils/id';
import { STAFF_TRANSITIONS } from '@/lib/admin/reservation-flow';
import { audit } from './audit';
import {
  availabilityContext,
  emailBookingCancelled,
  emailBookingConfirmed,
  emailBookingUpdated,
  markWaitlistBooked,
  processWaitlist,
  releaseHolds,
  waitlistSessionKey,
  type Reservation,
  type WaitlistEntry,
} from './reservations';

type Actor = { id: string; email: string };
const MINUTE = 60_000;

/** Seatings typed by staff before five in the morning belong to the previous evening's service. */
export function serviceMinutesFor(time: string): number {
  const m = parseTime(time);
  return m < 5 * 60 ? m + 1440 : m;
}

/** Tables shared with another live booking in the window (empty when they are free). */
function clashes(bookings: BookingInfo[], tableIds: string[], startsAt: Date, endsAt: Date, excludeId?: string): string[] {
  const wanted = new Set(tableIds);
  return bookings.filter((b) => b.id !== excludeId && b.startsAt < endsAt && b.endsAt > startsAt && b.tableIds.some((t) => wanted.has(t))).map((b) => b.id);
}

export interface StaffBookingInput {
  branch: BranchDTO;
  date: string;
  time: string;
  partySize: number;
  area: AreaChoice;
  /** Chosen tables, or null to let the engine assign. */
  tableIds: string[] | null;
  /** Ignore pacing and service periods (the team knows the room). Tables must still be free. */
  override: boolean;
  seatNow: boolean;
  name: string;
  email: string;
  phone: string;
  occasion: Occasion | null;
  notes: string | null;
  dietaryNotes: string | null;
  staffNotes: string | null;
  locale: string;
  source: 'phone' | 'walk_in' | 'admin';
  userId: string | null;
  sendEmail: boolean;
  actor: Actor;
  now: Date;
}

export type StaffBookResult = { ok: true; reservation: Reservation } | { ok: false; error: 'unavailable' | 'tableTaken' | 'noTables' };

/**
 * A booking taken by the team (phone, walk-in, or from the waitlist). Runs in one write transaction against the
 * same table map as online bookings, so it can never double-book a table. No deposit is asked for.
 */
export async function staffBook(input: StaffBookingInput): Promise<StaffBookResult> {
  const result = await db.transaction(async (tx) => {
    const ctx = await availabilityContext(tx, input.branch, input.date, input.now);
    const minutes = serviceMinutesFor(input.time);
    let startsAt: Date;
    let endsAt: Date;
    let tableIds: string[];
    if (!input.override && !input.tableIds && !input.seatNow) {
      const slot = checkSlot(ctx, { date: input.date, partySize: input.partySize, area: input.area, now: input.now, minLeadMinutes: 0, time: input.time });
      if (!slot || !slot.available) return { ok: false as const, error: 'unavailable' as const };
      ({ startsAt, endsAt, tableIds } = slot);
    } else {
      startsAt = localToUtc(input.date, minutes, input.branch.timeZone, true) as Date;
      endsAt = new Date(startsAt.getTime() + turnTimeFor(input.partySize, ctx.rules.turnTimes) * MINUTE);
      if (input.tableIds?.length) {
        if (clashes(ctx.bookings, input.tableIds, startsAt, endsAt).length) return { ok: false as const, error: 'tableTaken' as const };
        tableIds = input.tableIds;
      } else {
        const occupied = new Set(ctx.bookings.filter((b) => b.startsAt < endsAt && b.endsAt > startsAt).flatMap((b) => b.tableIds));
        const free = ctx.tables.filter((t) => t.isActive && !occupied.has(t.id) && (input.area === 'any' || t.area === input.area));
        const picked = assignTables(input.partySize, free);
        if (!picked) return { ok: false as const, error: 'noTables' as const };
        tableIds = picked;
      }
    }
    const id = createId();
    const rows = await tx
      .insert(s.reservations)
      .values({
        id,
        code: createCode(CODE_PREFIX.reservation),
        tokenHash: await sha256(linkToken('reservation', id)),
        branchId: input.branch.id,
        userId: input.userId,
        date: input.date,
        time: input.time,
        startsAt,
        endsAt,
        partySize: input.partySize,
        area: input.area,
        occasion: input.occasion,
        tableIds,
        status: input.seatNow ? 'seated' : 'confirmed',
        seatedAt: input.seatNow ? input.now : null,
        name: input.name,
        email: input.email,
        phone: input.phone,
        notes: input.notes,
        dietaryNotes: input.dietaryNotes,
        staffNotes: input.staffNotes,
        locale: input.locale,
        source: input.source,
      })
      .returning();
    return { ok: true as const, reservation: rows[0] as Reservation };
  });
  if (!result.ok) return result;
  if (input.sendEmail && input.email) await emailBookingConfirmed(result.reservation, input.branch);
  await audit({ actor: input.actor, action: 'reservation.create', entity: 'reservation', entityId: result.reservation.id, summary: `${result.reservation.code} · ${input.source} · ${input.date} ${input.time} · ${input.partySize}` });
  return result;
}

export type StatusResult = { ok: true; reservation: Reservation } | { ok: false; error: 'transition' | 'conflict' | 'tableTaken' };

/**
 * Moves a booking through the evening: seated, finished (which frees its tables at once), no-show, cancelled
 * (optionally refunding a paid deposit and emailing the guest) — or back again when it was a mistake.
 */
export async function staffSetStatus(r: Reservation, next: s.ReservationStatus, options: { refund: boolean; notify: boolean; actor: Actor; branch: BranchDTO; now: Date }): Promise<StatusResult> {
  if (!STAFF_TRANSITIONS[r.status].includes(next)) return { ok: false, error: 'transition' };
  const now = options.now;
  if (next === 'confirmed' && r.status === 'cancelled') {
    // Restoring needs the tables (or others) to be free again.
    const restored = await db.transaction(async (tx) => {
      const ctx = await availabilityContext(tx, options.branch, r.date, now);
      let tableIds = r.tableIds;
      if (clashes(ctx.bookings, tableIds, r.startsAt, r.endsAt, r.id).length) {
        const occupied = new Set(ctx.bookings.filter((b) => b.id !== r.id && b.startsAt < r.endsAt && b.endsAt > r.startsAt).flatMap((b) => b.tableIds));
        const picked = assignTables(r.partySize, ctx.tables.filter((t) => t.isActive && !occupied.has(t.id)));
        if (!picked) return null;
        tableIds = picked;
      }
      const rows = await tx.update(s.reservations).set({ status: 'confirmed', tableIds, cancelledAt: null, updatedAt: now }).where(and(eq(s.reservations.id, r.id), eq(s.reservations.status, 'cancelled'))).returning();
      return rows[0] ?? null;
    });
    if (!restored) return { ok: false, error: 'tableTaken' };
    if (options.notify) await emailBookingConfirmed(restored, options.branch);
    await audit({ actor: options.actor, action: 'reservation.restore', entity: 'reservation', entityId: r.id, summary: r.code });
    return { ok: true, reservation: restored };
  }

  const refund = next === 'cancelled' && options.refund && r.depositStatus === 'paid' && Boolean(r.paymentId);
  const set: Partial<typeof s.reservations.$inferInsert> = { status: next, updatedAt: now };
  if (next === 'seated') Object.assign(set, { seatedAt: r.seatedAt ?? now, completedAt: null });
  if (next === 'completed') Object.assign(set, { completedAt: now, ...(r.endsAt > now ? { endsAt: now } : {}) });
  if (next === 'cancelled') Object.assign(set, { cancelledAt: now, ...(refund ? { depositStatus: 'refunded' as const } : {}) });
  if (next === 'confirmed' && r.status === 'pending') Object.assign(set, { depositStatus: r.depositStatus === 'pending' ? ('none' as const) : r.depositStatus });
  if (next === 'confirmed' && r.status === 'seated') Object.assign(set, { seatedAt: null });
  const rows = await db.update(s.reservations).set(set).where(and(eq(s.reservations.id, r.id), eq(s.reservations.status, r.status))).returning();
  const updated = rows[0];
  if (!updated) return { ok: false, error: 'conflict' };
  if (refund && r.paymentId) {
    const { refundPayment } = await import('./payments');
    await refundPayment(r.paymentId);
  }
  if (options.notify && next === 'cancelled') await emailBookingCancelled(r, options.branch, refund);
  if (options.notify && next === 'confirmed' && r.status === 'pending') await emailBookingConfirmed(updated, options.branch);
  await audit({ actor: options.actor, action: `reservation.${next}`, entity: 'reservation', entityId: r.id, summary: `${r.code}: ${r.status} → ${next}${refund ? ' (deposit refunded)' : ''}` });
  // Tables freed early may go to the waitlist.
  if (next === 'cancelled' || next === 'no_show' || next === 'completed') await processWaitlist(r.branchId, r.date, now);
  return { ok: true, reservation: updated };
}

export interface StaffEditInput {
  date: string;
  time: string;
  partySize: number;
  area: AreaChoice;
  tableIds: string[] | null;
  override: boolean;
  name: string;
  email: string;
  phone: string;
  occasion: Occasion | null;
  notes: string | null;
  dietaryNotes: string | null;
  staffNotes: string | null;
  notify: boolean;
}

/** Changes a booking's details; a new time, party or tables is re-checked against the table map. */
export async function staffEdit(r: Reservation, input: StaffEditInput, ctx: { branch: BranchDTO; actor: Actor; now: Date }): Promise<StaffBookResult> {
  const moved = input.date !== r.date || input.time !== r.time || input.partySize !== r.partySize || input.area !== r.area;
  const retabled = input.tableIds !== null && (input.tableIds.length !== r.tableIds.length || input.tableIds.some((t) => !r.tableIds.includes(t)));
  const result = await db.transaction(async (tx) => {
    let schedule = { startsAt: r.startsAt, endsAt: r.endsAt, tableIds: r.tableIds };
    if (moved || retabled) {
      const avail = await availabilityContext(tx, ctx.branch, input.date, ctx.now);
      const startsAt = localToUtc(input.date, serviceMinutesFor(input.time), ctx.branch.timeZone, true) as Date;
      const endsAt = new Date(startsAt.getTime() + turnTimeFor(input.partySize, avail.rules.turnTimes) * MINUTE);
      if (input.tableIds?.length) {
        if (clashes(avail.bookings, input.tableIds, startsAt, endsAt, r.id).length) return { ok: false as const, error: 'tableTaken' as const };
        schedule = { startsAt, endsAt, tableIds: input.tableIds };
      } else if (input.override) {
        const occupied = new Set(avail.bookings.filter((b) => b.id !== r.id && b.startsAt < endsAt && b.endsAt > startsAt).flatMap((b) => b.tableIds));
        const picked = assignTables(input.partySize, avail.tables.filter((t) => t.isActive && !occupied.has(t.id) && (input.area === 'any' || t.area === input.area)));
        if (!picked) return { ok: false as const, error: 'noTables' as const };
        schedule = { startsAt, endsAt, tableIds: picked };
      } else {
        const slot = checkSlot(avail, { date: input.date, partySize: input.partySize, area: input.area, now: ctx.now, minLeadMinutes: 0, time: input.time, excludeIds: [r.id] });
        if (!slot || !slot.available) return { ok: false as const, error: 'unavailable' as const };
        schedule = { startsAt: slot.startsAt, endsAt: slot.endsAt, tableIds: slot.tableIds };
      }
    }
    const rows = await tx
      .update(s.reservations)
      .set({
        date: input.date,
        time: input.time,
        partySize: input.partySize,
        area: input.area,
        ...schedule,
        name: input.name,
        email: input.email,
        phone: input.phone,
        occasion: input.occasion,
        notes: input.notes,
        dietaryNotes: input.dietaryNotes,
        staffNotes: input.staffNotes,
        ...(moved ? { reminderSentAt: null } : {}),
        updatedAt: ctx.now,
      })
      .where(eq(s.reservations.id, r.id))
      .returning();
    return { ok: true as const, reservation: rows[0] as Reservation };
  });
  if (!result.ok) return result;
  if (moved && input.notify) await emailBookingUpdated(result.reservation, ctx.branch);
  await audit({ actor: ctx.actor, action: 'reservation.update', entity: 'reservation', entityId: r.id, summary: moved ? `${r.code}: ${r.date} ${r.time} (${r.partySize}) → ${input.date} ${input.time} (${input.partySize})` : r.code });
  if (moved) await processWaitlist(r.branchId, r.date, ctx.now);
  return result;
}

/** Books a waitlisted party at the time the team chose, and closes their waitlist entry. */
export async function staffBookFromWaitlist(entry: WaitlistEntry, input: { branch: BranchDTO; time: string; tableIds: string[] | null; override: boolean; sendEmail: boolean; actor: Actor; now: Date }): Promise<StaffBookResult> {
  // A held offer would block the very table the team is about to give them.
  await releaseHolds(waitlistSessionKey(entry.id));
  const result = await staffBook({
    branch: input.branch,
    date: entry.date,
    time: input.time,
    partySize: entry.partySize,
    area: 'any',
    tableIds: input.tableIds,
    override: input.override,
    seatNow: false,
    name: entry.name,
    email: entry.email,
    phone: entry.phone,
    occasion: null,
    notes: entry.notes,
    dietaryNotes: null,
    staffNotes: null,
    locale: entry.locale,
    source: 'admin',
    userId: null,
    sendEmail: input.sendEmail,
    actor: input.actor,
    now: input.now,
  });
  if (result.ok) await markWaitlistBooked(entry.id, result.reservation.id);
  return result;
}

export async function staffCancelWaitlist(entry: WaitlistEntry, actor: Actor): Promise<void> {
  await releaseHolds(waitlistSessionKey(entry.id));
  await db.update(s.waitlistEntries).set({ status: 'cancelled', updatedAt: new Date() }).where(eq(s.waitlistEntries.id, entry.id));
  await audit({ actor, action: 'waitlist.cancel', entity: 'waitlist', entityId: entry.id, summary: `${entry.name} · ${entry.date}` });
}

// ————————————————————————————————————————— reads —————————————————————————————————————————

export interface DayTable {
  id: string;
  label: string;
  area: s.TableArea;
  minSeats: number;
  maxSeats: number;
  reservable: boolean;
}

export interface DayBooking {
  id: string;
  code: string;
  time: string;
  startsAt: string;
  endsAt: string;
  partySize: number;
  status: s.ReservationStatus;
  name: string;
  phone: string;
  email: string;
  tableIds: string[];
  area: string;
  occasion: string | null;
  notes: string | null;
  dietaryNotes: string | null;
  staffNotes: string | null;
  source: Reservation['source'];
  depositAmount: number;
  depositStatus: Reservation['depositStatus'];
  userId: string | null;
}

export interface DayView {
  tables: DayTable[];
  /** Each booking with its minutes from the service day's midnight (for the timeline). */
  bookings: (DayBooking & { startMin: number; endMin: number })[];
  holds: { id: string; startMin: number; endMin: number; tableIds: string[]; partySize: number }[];
  waitlist: WaitlistEntry[];
  /** Minutes from local midnight where the timeline starts and ends. */
  span: { start: number; end: number };
}

function toDayBooking(r: Reservation): DayBooking {
  return {
    id: r.id,
    code: r.code,
    time: r.time,
    startsAt: r.startsAt.toISOString(),
    endsAt: r.endsAt.toISOString(),
    partySize: r.partySize,
    status: r.status,
    name: r.name,
    phone: r.phone,
    email: r.email,
    tableIds: r.tableIds,
    area: r.area,
    occasion: r.occasion,
    notes: r.notes,
    dietaryNotes: r.dietaryNotes,
    staffNotes: r.staffNotes,
    source: r.source,
    depositAmount: r.depositAmount,
    depositStatus: r.depositStatus,
    userId: r.userId,
  };
}

/** One house's service day: its tables, every booking (cancelled ones last), live holds and the waitlist. */
export async function dayView(branch: BranchDTO, date: string, now = new Date()): Promise<DayView> {
  const [tables, bookings, holds, waitlist] = await Promise.all([
    db.select().from(s.diningTables).where(and(eq(s.diningTables.branchId, branch.id), eq(s.diningTables.isActive, true))).orderBy(asc(s.diningTables.sortOrder)),
    db.select().from(s.reservations).where(and(eq(s.reservations.branchId, branch.id), eq(s.reservations.date, date))).orderBy(asc(s.reservations.startsAt)),
    db.select().from(s.reservationHolds).where(and(eq(s.reservationHolds.branchId, branch.id), eq(s.reservationHolds.date, date))),
    db.select().from(s.waitlistEntries).where(and(eq(s.waitlistEntries.branchId, branch.id), eq(s.waitlistEntries.date, date))).orderBy(asc(s.waitlistEntries.createdAt)),
  ]);
  const weekday = new Date(`${date}T12:00:00Z`).getUTCDay();
  const periods = branch.periods.filter((p) => p.weekdays.includes(weekday));
  let start = Math.min(...periods.map((p) => parseTime(p.start)), ...bookings.map((b) => serviceMinutesFor(b.time)));
  let end = Math.max(
    ...periods.map((p) => {
      const st = parseTime(p.start);
      const e = parseTime(p.end);
      return (e < st ? e + 1440 : e) + 120;
    }),
    ...bookings.map((b) => serviceMinutesFor(b.time) + Math.round((b.endsAt.getTime() - b.startsAt.getTime()) / MINUTE)),
  );
  if (!Number.isFinite(start) || !Number.isFinite(end)) {
    start = 12 * 60;
    end = 24 * 60;
  }
  const span = (startsAt: Date, endsAt: Date) => {
    const startMin = serviceMinutes(startsAt, date, branch.timeZone);
    return { startMin, endMin: startMin + Math.max(15, Math.round((endsAt.getTime() - startsAt.getTime()) / MINUTE)) };
  };
  return {
    tables: tables.map((t) => ({ id: t.id, label: t.label, area: t.area, minSeats: t.minSeats, maxSeats: t.maxSeats, reservable: t.reservable })),
    bookings: bookings.map((b) => ({ ...toDayBooking(b), ...span(b.startsAt, b.endsAt) })),
    holds: holds.filter((h) => h.expiresAt > now).map((h) => ({ id: h.id, ...span(h.startsAt, h.endsAt), tableIds: h.tableIds, partySize: h.partySize })),
    waitlist,
    span: { start: Math.floor(start / 60) * 60, end: Math.ceil(end / 60) * 60 },
  };
}

/** Bookings and covers per day for a month (cancelled bookings excluded). */
export async function monthCounts(branchIds: string[], first: string, last: string): Promise<Record<string, { bookings: number; covers: number }>> {
  const rows = await db
    .select({ date: s.reservations.date, partySize: s.reservations.partySize })
    .from(s.reservations)
    .where(and(inArray(s.reservations.branchId, branchIds), gte(s.reservations.date, first), lte(s.reservations.date, last), ne(s.reservations.status, 'cancelled')));
  const out: Record<string, { bookings: number; covers: number }> = {};
  for (const r of rows) {
    const day = (out[r.date] ??= { bookings: 0, covers: 0 });
    day.bookings += 1;
    day.covers += r.partySize;
  }
  return out;
}

/** Bookings across houses for a date range (the list view). */
export async function rangeBookings(branchIds: string[], from: string, to: string): Promise<(DayBooking & { branchId: string; date: string })[]> {
  const rows = await db
    .select()
    .from(s.reservations)
    .where(and(inArray(s.reservations.branchId, branchIds), gte(s.reservations.date, from), lte(s.reservations.date, to)))
    .orderBy(asc(s.reservations.startsAt))
    .limit(3000);
  return rows.map((r) => ({ ...toDayBooking(r), branchId: r.branchId, date: r.date }));
}

/** Upcoming waitlist across houses (today onwards). */
export async function openWaitlist(branchIds: string[], from: string): Promise<WaitlistEntry[]> {
  return db
    .select()
    .from(s.waitlistEntries)
    .where(and(inArray(s.waitlistEntries.branchId, branchIds), gte(s.waitlistEntries.date, from), inArray(s.waitlistEntries.status, ['waiting', 'offered'])))
    .orderBy(asc(s.waitlistEntries.date), asc(s.waitlistEntries.createdAt));
}

/** A guest's visit record by email/phone: how often they came, and how often they did not. */
export async function guestRecord(r: Pick<Reservation, 'id' | 'email' | 'phone'>): Promise<{ visits: number; noShows: number; cancellations: number; lastVisit: string | null }> {
  const same = or(r.email ? eq(s.reservations.email, r.email) : undefined, r.phone ? eq(s.reservations.phone, r.phone) : undefined);
  if (!same) return { visits: 0, noShows: 0, cancellations: 0, lastVisit: null };
  const rows = await db
    .select({ status: s.reservations.status, date: s.reservations.date })
    .from(s.reservations)
    .where(and(ne(s.reservations.id, r.id), lt(s.reservations.startsAt, new Date()), same))
    .orderBy(asc(s.reservations.startsAt));
  const visits = rows.filter((x) => x.status === 'completed' || x.status === 'seated');
  return {
    visits: visits.length,
    noShows: rows.filter((x) => x.status === 'no_show').length,
    cancellations: rows.filter((x) => x.status === 'cancelled').length,
    lastVisit: visits.at(-1)?.date ?? null,
  };
}
