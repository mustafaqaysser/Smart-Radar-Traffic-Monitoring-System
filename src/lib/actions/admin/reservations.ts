'use server';

import { eq } from 'drizzle-orm';
import { z } from 'zod';
import { getAdminScope } from '@/lib/admin/context';
import { db } from '@/lib/db/client';
import * as s from '@/lib/db/schema';
import { computeAvailability } from '@/lib/domain/availability';
import { getBranch } from '@/lib/queries/branches';
import { AREA_CHOICES, OCCASIONS } from '@/lib/reserve/types';
import { availabilityContext, processWaitlist } from '@/lib/server/reservations';
import { STAFF_TRANSITIONS } from '@/lib/admin/reservation-flow';
import { staffBook, staffBookFromWaitlist, staffCancelWaitlist, staffEdit, staffSetStatus } from '@/lib/server/reservations-staff';
import { normalizePhone } from '@/lib/services/phone';
import { fail, ok, type ActionResult } from '../result';
import { actorOf, staffAction } from './guard';

const DATE = /^\d{4}-\d{2}-\d{2}$/;
const TIME = /^([01]\d|2[0-3]):[0-5]\d$/;

const guest = {
  name: z.string().trim().min(2, 'tooShort').max(80, 'tooLong'),
  email: z.union([z.literal(''), z.email('email').max(254)]),
  phone: z.string().trim().max(32),
  occasion: z.enum(OCCASIONS).nullable(),
  notes: z.string().trim().max(500).nullable(),
  dietaryNotes: z.string().trim().max(500).nullable(),
  staffNotes: z.string().trim().max(1000).nullable(),
};

/** A phone number typed by the team: normalised when it parses, kept as typed otherwise (walk-ins may give none). */
function cleanPhone(raw: string): string {
  return raw ? (normalizePhone(raw) ?? raw) : '';
}

async function bookingInScope(id: string, userId: Parameters<typeof getAdminScope>[0]) {
  const scope = await getAdminScope(userId);
  const r = await db.query.reservations.findFirst({ where: eq(s.reservations.id, id) });
  if (!r || !scope.branchIds.includes(r.branchId)) return null;
  const branch = await getBranch(r.branchId);
  return branch ? { r, branch } : null;
}

const createSchema = z.object({
  branchId: z.string().min(1).max(64),
  date: z.string().regex(DATE),
  time: z.string().regex(TIME),
  partySize: z.number().int().min(1).max(60),
  area: z.enum(AREA_CHOICES),
  tableIds: z.array(z.string().max(64)).max(8).nullable(),
  override: z.boolean(),
  seatNow: z.boolean(),
  source: z.enum(['phone', 'walk_in', 'admin']),
  locale: z.enum(['ar', 'en']),
  sendEmail: z.boolean(),
  userId: z.string().max(64).nullable(),
  ...guest,
});

/** A booking taken by phone, at the door (seated at once) or on a guest's behalf. */
export async function createReservation(input: z.input<typeof createSchema>): Promise<ActionResult<{ code: string }>> {
  return staffAction('reservations:manage', createSchema, input, async (d, user) => {
    const scope = await getAdminScope(user);
    const branch = scope.branches.find((b) => b.id === d.branchId);
    if (!branch) return fail('notFound');
    const result = await staffBook({ ...d, branch, phone: cleanPhone(d.phone), email: d.email.toLowerCase(), actor: actorOf(user), now: new Date() });
    if (!result.ok) return fail(result.error === 'tableTaken' ? 'tableTaken' : 'capacity');
    return ok({ code: result.reservation.code });
  });
}

const statusSchema = z.object({ id: z.string().min(1).max(64), next: z.enum(s.RESERVATION_STATUSES), refund: z.boolean().default(false), notify: z.boolean().default(true) });

export async function setReservationStatus(input: z.input<typeof statusSchema>): Promise<ActionResult<{ status: s.ReservationStatus }>> {
  return staffAction('reservations:manage', statusSchema, input, async (d, user) => {
    const found = await bookingInScope(d.id, user);
    if (!found) return fail('notFound');
    if (!STAFF_TRANSITIONS[found.r.status].includes(d.next)) return fail('transition');
    const result = await staffSetStatus(found.r, d.next, { refund: d.refund, notify: d.notify, actor: actorOf(user), branch: found.branch, now: new Date() });
    if (!result.ok) return fail(result.error);
    return ok({ status: result.reservation.status });
  });
}

const editSchema = z.object({
  id: z.string().min(1).max(64),
  date: z.string().regex(DATE),
  time: z.string().regex(TIME),
  partySize: z.number().int().min(1).max(60),
  area: z.enum(AREA_CHOICES),
  tableIds: z.array(z.string().max(64)).max(8).nullable(),
  override: z.boolean(),
  notify: z.boolean(),
  ...guest,
});

export async function updateReservation(input: z.input<typeof editSchema>): Promise<ActionResult<{ code: string }>> {
  return staffAction('reservations:manage', editSchema, input, async (d, user) => {
    const found = await bookingInScope(d.id, user);
    if (!found) return fail('notFound');
    if (found.r.status === 'cancelled' || found.r.status === 'completed') return fail('transition');
    const result = await staffEdit(found.r, { ...d, phone: cleanPhone(d.phone), email: d.email.toLowerCase() }, { branch: found.branch, actor: actorOf(user), now: new Date() });
    if (!result.ok) return fail(result.error === 'tableTaken' ? 'tableTaken' : 'capacity');
    return ok({ code: result.reservation.code });
  });
}

const tablesSchema = z.object({ id: z.string().min(1).max(64), tableIds: z.array(z.string().max(64)).min(1).max(8) });

/** Moves a booking to other tables (same time), refusing tables another booking holds then. */
export async function assignReservationTables(input: z.input<typeof tablesSchema>): Promise<ActionResult<null>> {
  return staffAction('reservations:manage', tablesSchema, input, async (d, user) => {
    const found = await bookingInScope(d.id, user);
    if (!found) return fail('notFound');
    const { r } = found;
    const result = await staffEdit(
      r,
      { date: r.date, time: r.time, partySize: r.partySize, area: r.area === 'any' ? 'any' : (r.area as (typeof AREA_CHOICES)[number]), tableIds: d.tableIds, override: true, notify: false, name: r.name, email: r.email, phone: r.phone, occasion: (r.occasion as (typeof OCCASIONS)[number] | null) ?? null, notes: r.notes, dietaryNotes: r.dietaryNotes, staffNotes: r.staffNotes },
      { branch: found.branch, actor: actorOf(user), now: new Date() },
    );
    if (!result.ok) return fail(result.error === 'tableTaken' ? 'tableTaken' : 'capacity');
    return ok(null);
  });
}

const notesSchema = z.object({ id: z.string().min(1).max(64), staffNotes: z.string().trim().max(1000) });

export async function saveReservationNotes(input: z.input<typeof notesSchema>): Promise<ActionResult<null>> {
  return staffAction('reservations:manage', notesSchema, input, async (d, user) => {
    const found = await bookingInScope(d.id, user);
    if (!found) return fail('notFound');
    await db.update(s.reservations).set({ staffNotes: d.staffNotes || null, updatedAt: new Date() }).where(eq(s.reservations.id, d.id));
    return ok(null);
  });
}

const slotsSchema = z.object({ branchId: z.string().min(1).max(64), date: z.string().regex(DATE), partySize: z.number().int().min(1).max(60), area: z.enum(AREA_CHOICES), excludeId: z.string().max(64).nullable() });

export interface StaffSlot {
  time: string;
  available: boolean;
  reason: 'past' | 'pacing' | 'full' | null;
  tableIds: string[];
}

/** Times for the booking form, with why a time is not offered (staff may still override pacing). */
export async function staffSlots(input: z.input<typeof slotsSchema>): Promise<ActionResult<StaffSlot[]>> {
  return staffAction(
    ['reservations:view', 'reservations:manage'],
    slotsSchema,
    input,
    async (d, user) => {
      const scope = await getAdminScope(user);
      const branch = scope.branches.find((b) => b.id === d.branchId);
      if (!branch) return fail('notFound');
      const ctx = await availabilityContext(db, branch, d.date, new Date());
      const slots = computeAvailability(ctx, { date: d.date, partySize: d.partySize, area: d.area, now: new Date(), minLeadMinutes: 0, excludeIds: d.excludeId ? [d.excludeId] : [] });
      return ok(slots.map((sl) => ({ time: sl.time, available: sl.available, reason: sl.reason ?? null, tableIds: sl.tableIds })));
    },
    { refresh: false },
  );
}

const waitlistBookSchema = z.object({ entryId: z.string().min(1).max(64), time: z.string().regex(TIME), tableIds: z.array(z.string().max(64)).max(8).nullable(), override: z.boolean(), sendEmail: z.boolean() });

export async function bookWaitlistEntry(input: z.input<typeof waitlistBookSchema>): Promise<ActionResult<{ code: string }>> {
  return staffAction('reservations:manage', waitlistBookSchema, input, async (d, user) => {
    const scope = await getAdminScope(user);
    const entry = await db.query.waitlistEntries.findFirst({ where: eq(s.waitlistEntries.id, d.entryId) });
    if (!entry || !scope.branchIds.includes(entry.branchId)) return fail('notFound');
    if (entry.status !== 'waiting' && entry.status !== 'offered') return fail('transition');
    const branch = scope.branches.find((b) => b.id === entry.branchId);
    if (!branch) return fail('notFound');
    const result = await staffBookFromWaitlist(entry, { branch, time: d.time, tableIds: d.tableIds, override: d.override, sendEmail: d.sendEmail, actor: actorOf(user), now: new Date() });
    if (!result.ok) return fail(result.error === 'tableTaken' ? 'tableTaken' : 'capacity');
    return ok({ code: result.reservation.code });
  });
}

export async function cancelWaitlistEntry(entryId: string): Promise<ActionResult<null>> {
  return staffAction('reservations:manage', z.string().min(1).max(64), entryId, async (id, user) => {
    const scope = await getAdminScope(user);
    const entry = await db.query.waitlistEntries.findFirst({ where: eq(s.waitlistEntries.id, id) });
    if (!entry || !scope.branchIds.includes(entry.branchId)) return fail('notFound');
    await staffCancelWaitlist(entry, actorOf(user));
    return ok(null);
  });
}

const offersSchema = z.object({ branchId: z.string().min(1).max(64), date: z.string().regex(DATE) });

/** Offers any free tables to the waitlist now (the hourly job does the same). */
export async function runWaitlistOffers(input: z.input<typeof offersSchema>): Promise<ActionResult<{ offered: number }>> {
  return staffAction('reservations:manage', offersSchema, input, async (d, user) => {
    const scope = await getAdminScope(user);
    if (!scope.branchIds.includes(d.branchId)) return fail('notFound');
    return ok({ offered: await processWaitlist(d.branchId, d.date, new Date()) });
  });
}
