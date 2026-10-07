import 'server-only';
import { and, asc, eq, gt, inArray, isNull, lt, lte, ne, or } from 'drizzle-orm';
import { getTranslations } from 'next-intl/server';
import restaurantConfig from '@config';
import { db } from '@/lib/db/client';
import * as s from '@/lib/db/schema';
import { checkSlot, computeAvailability, type AvailabilityContext, type BookingInfo } from '@/lib/domain/availability';
import { buildIcs, googleCalendarUrl } from '@/lib/domain/calendar';
import { scheduleForDate } from '@/lib/domain/hours';
import { formatClock, formatDate, formatMoney, formatNumber, formatWallTime } from '@/lib/i18n/format';
import { tr } from '@/lib/i18n/localized';
import { plural } from '@/lib/i18n/plural';
import { getBranch, getSeasonalModes, hoursFor } from '@/lib/queries/branches';
import type { BranchDTO } from '@/lib/queries/types';
import { isAreaChoice, OCCASIONS, type AreaChoice, type Occasion, type PublicSlot } from '@/lib/reserve/types';

export { AREA_CHOICES, OCCASIONS, isAreaChoice, type AreaChoice, type Occasion } from '@/lib/reserve/types';
import { audit, notifyStaff } from '@/lib/server/audit';
import { mail } from '@/lib/server/mail';
import { linkToken, verifyLinkToken } from '@/lib/server/tokens';
import { googleDirectionsUrl, whatsappUrl } from '@/lib/services/maps';
import { absoluteUrl } from '@/lib/site/url';
import { localToUtc, parseTime, toDateString } from '@/lib/time/zoned';
import { CODE_PREFIX, createCode, createId, sha256 } from '@/lib/utils/id';

type Tx = Parameters<Parameters<typeof db.transaction>[0]>[0];
type Executor = typeof db | Tx;
export type Reservation = typeof s.reservations.$inferSelect;
export type WaitlistEntry = typeof s.waitlistEntries.$inferSelect;

const MINUTE = 60_000;
const HOUR = 60 * MINUTE;
const ACTIVE: s.ReservationStatus[] = ['pending', 'confirmed', 'seated'];
const rules = restaurantConfig.reservations;

/** Online bookings need this much notice. */
export const MIN_LEAD_MINUTES = 30;
/** An unpaid deposit releases its table after this long. */
export const DEPOSIT_WINDOW_MINUTES = 30;
/** A table offered to the waitlist is held this long (the waitlist email promises an hour). */
const WAITLIST_HOLD_MINUTES = 60;
// ————————————————————————————————————————— availability —————————————————————————————————————————

/**
 * Everything the availability engine needs for one branch and service date. Reads run inside the caller's
 * transaction when one is passed, so holds and bookings are checked and written atomically.
 */
export async function availabilityContext(exec: Executor, branch: BranchDTO, date: string, now: Date): Promise<AvailabilityContext> {
  const dayStart = localToUtc(date, 0, branch.timeZone, true) ?? new Date(`${date}T00:00:00Z`);
  const windowStart = new Date(dayStart.getTime() - 12 * HOUR);
  const windowEnd = new Date(dayStart.getTime() + 40 * HOUR);
  const staleDeposit = new Date(now.getTime() - DEPOSIT_WINDOW_MINUTES * MINUTE);
  const modes = await getSeasonalModes();
  const tables = await exec.select().from(s.diningTables).where(eq(s.diningTables.branchId, branch.id));
  const reservations = await exec
    .select({ id: s.reservations.id, startsAt: s.reservations.startsAt, endsAt: s.reservations.endsAt, partySize: s.reservations.partySize, tableIds: s.reservations.tableIds })
    .from(s.reservations)
    .where(
      and(
        eq(s.reservations.branchId, branch.id),
        inArray(s.reservations.status, ACTIVE),
        lt(s.reservations.startsAt, windowEnd),
        gt(s.reservations.endsAt, windowStart),
        // A deposit left unpaid past its window no longer keeps the table.
        or(ne(s.reservations.depositStatus, 'pending'), gt(s.reservations.createdAt, staleDeposit)),
      ),
    );
  const holds = await exec
    .select({ id: s.reservationHolds.id, startsAt: s.reservationHolds.startsAt, endsAt: s.reservationHolds.endsAt, partySize: s.reservationHolds.partySize, tableIds: s.reservationHolds.tableIds })
    .from(s.reservationHolds)
    .where(and(eq(s.reservationHolds.branchId, branch.id), gt(s.reservationHolds.expiresAt, now), lt(s.reservationHolds.startsAt, windowEnd), gt(s.reservationHolds.endsAt, windowStart)));
  const schedule = scheduleForDate(hoursFor(branch, modes), date);
  const special = branch.specials.find((sp) => sp.date === date);
  const bookings: BookingInfo[] = [...reservations, ...holds];
  const reshaped = Boolean(schedule.special?.ranges?.length) || schedule.seasonal;
  return {
    timeZone: branch.timeZone,
    rules: {
      slotIntervalMinutes: branch.reservationSettings.slotIntervalMinutes,
      turnTimes: branch.reservationSettings.turnTimes,
      maxCoversPerSlot: branch.reservationSettings.maxCoversPerSlot,
      lastSeatingBeforeCloseMinutes: branch.reservationSettings.lastSeatingBeforeCloseMinutes,
    },
    periods: branch.periods.map((p) => ({ key: p.key, weekdays: p.weekdays, start: p.start, end: p.end, maxCoversPerSlot: p.maxCoversPerSlot })),
    tables: tables.map((t) => ({ id: t.id, area: t.area, minSeats: t.minSeats, maxSeats: t.maxSeats, combineGroup: t.combineGroup, reservable: t.reservable, isActive: t.isActive, sortOrder: t.sortOrder })),
    bookings,
    override: {
      closed: schedule.closed,
      // Holiday and seasonal hours (e.g. Ramadan) reshape the seatings; ordinary days follow the service periods.
      ranges: reshaped ? schedule.ranges : null,
      reservationsBlocked: Boolean(special?.reservationsBlocked),
    },
  };
}

/** First and last bookable service dates in the branch's time zone. */
export function bookingWindow(branch: BranchDTO, now: Date): { first: string; last: string } {
  const first = toDateString(now, branch.timeZone);
  const last = toDateString(new Date(now.getTime() + rules.bookingWindowDays * 24 * HOUR), branch.timeZone);
  return { first, last };
}

/** Bookable times for a party on a day. Table assignments never leave the server. */
export async function findSlots(branch: BranchDTO, date: string, partySize: number, area: string, now: Date, excludeIds: string[] = []): Promise<PublicSlot[]> {
  const ctx = await availabilityContext(db, branch, date, now);
  return computeAvailability(ctx, { date, partySize, area, now, minLeadMinutes: MIN_LEAD_MINUTES, excludeIds }).map((slot) => ({
    time: slot.time,
    minutes: slot.minutes,
    periodKey: slot.periodKey,
    available: slot.available,
    reason: slot.reason ?? null,
    areas: slot.areas,
    startsAt: slot.startsAt.toISOString(),
  }));
}

// ————————————————————————————————————————— holds & booking —————————————————————————————————————————

export interface HoldInput {
  branch: BranchDTO;
  date: string;
  time: string;
  partySize: number;
  area: AreaChoice;
  sessionKey: string;
  now: Date;
  minutes?: number;
  minLeadMinutes?: number;
}

/**
 * Holds a table for a few minutes while the guest finishes. The session's own hold is ignored while checking
 * and replaced in the same write transaction, so changing the time or the area never blocks the guest against
 * themselves — and if the new choice is not possible, the table they already hold stays theirs.
 */
export async function holdSlot(input: HoldInput) {
  return db.transaction(async (tx) => {
    await tx.delete(s.reservationHolds).where(lt(s.reservationHolds.expiresAt, input.now));
    const own = await tx.select({ id: s.reservationHolds.id }).from(s.reservationHolds).where(eq(s.reservationHolds.sessionKey, input.sessionKey));
    const ctx = await availabilityContext(tx, input.branch, input.date, input.now);
    const slot = checkSlot(ctx, {
      date: input.date,
      partySize: input.partySize,
      area: input.area,
      now: input.now,
      minLeadMinutes: input.minLeadMinutes ?? MIN_LEAD_MINUTES,
      time: input.time,
      excludeIds: own.map((h) => h.id),
    });
    // Not possible: the guest keeps the table they already hold.
    if (!slot || !slot.available) return null;
    const hold = {
      id: createId(),
      branchId: input.branch.id,
      date: input.date,
      time: slot.time,
      startsAt: slot.startsAt,
      endsAt: slot.endsAt,
      partySize: input.partySize,
      tableIds: slot.tableIds,
      area: input.area,
      sessionKey: input.sessionKey,
      expiresAt: new Date(input.now.getTime() + (input.minutes ?? rules.holdMinutes) * MINUTE),
    };
    await tx.delete(s.reservationHolds).where(eq(s.reservationHolds.sessionKey, input.sessionKey));
    await tx.insert(s.reservationHolds).values(hold);
    return { ...hold, areas: slot.areas };
  });
}

export async function releaseHolds(sessionKey: string): Promise<void> {
  await db.delete(s.reservationHolds).where(eq(s.reservationHolds.sessionKey, sessionKey));
}

export async function holdFor(sessionKey: string, holdId: string, now: Date) {
  const hold = await db.query.reservationHolds.findFirst({ where: and(eq(s.reservationHolds.id, holdId), eq(s.reservationHolds.sessionKey, sessionKey)) });
  return hold && hold.expiresAt > now ? hold : null;
}

/** Deposit in minor units for a party (0 when no deposit applies). */
export function depositFor(partySize: number): number {
  return rules.deposit.perGuest > 0 && partySize >= rules.deposit.minParty ? partySize * rules.deposit.perGuest * 100 : 0;
}

export interface BookingDetails {
  name: string;
  email: string;
  phone: string;
  occasion: Occasion | null;
  notes: string | null;
  dietaryNotes: string | null;
  locale: string;
  userId: string | null;
  source: 'web' | 'concierge';
}

export type BookResult =
  | { ok: true; reservation: Reservation; branch: BranchDTO; deposit: number }
  | { ok: false; error: 'holdExpired' | 'unavailable' };

/**
 * Turns a hold into a reservation in one write transaction: the hold must still be the guest's and unexpired,
 * and the slot is re-checked without it, so two guests can never be given the same table.
 */
export async function bookFromHold(input: { holdId: string; sessionKey: string; details: BookingDetails; now: Date }): Promise<BookResult> {
  const hold = await holdFor(input.sessionKey, input.holdId, input.now);
  if (!hold) return { ok: false, error: 'holdExpired' };
  const branch = await getBranch(hold.branchId);
  if (!branch || !branch.reservationsEnabled) return { ok: false, error: 'unavailable' };
  const deposit = depositFor(hold.partySize);
  const area = isAreaChoice(hold.area) ? hold.area : 'any';
  const reservation = await db.transaction(async (tx) => {
    const ctx = await availabilityContext(tx, branch, hold.date, input.now);
    const slot = checkSlot(ctx, { date: hold.date, partySize: hold.partySize, area, now: input.now, minLeadMinutes: 0, time: hold.time, excludeIds: [hold.id] });
    if (!slot || !slot.available) return null;
    const id = createId();
    const rows = await tx
      .insert(s.reservations)
      .values({
        id,
        code: createCode(CODE_PREFIX.reservation),
        tokenHash: await sha256(linkToken('reservation', id)),
        branchId: branch.id,
        userId: input.details.userId,
        date: hold.date,
        time: slot.time,
        startsAt: slot.startsAt,
        endsAt: slot.endsAt,
        partySize: hold.partySize,
        area,
        occasion: input.details.occasion,
        tableIds: slot.tableIds,
        status: deposit > 0 ? 'pending' : 'confirmed',
        name: input.details.name,
        email: input.details.email,
        phone: input.details.phone,
        notes: input.details.notes,
        dietaryNotes: input.details.dietaryNotes,
        locale: input.details.locale,
        source: input.details.source,
        depositAmount: deposit,
        depositStatus: deposit > 0 ? 'pending' : 'none',
      })
      .returning();
    await tx.delete(s.reservationHolds).where(eq(s.reservationHolds.id, hold.id));
    return rows[0] ?? null;
  });
  if (!reservation) return { ok: false, error: 'unavailable' };
  if (deposit === 0) await announceBooking(reservation, branch);
  return { ok: true, reservation, branch, deposit };
}

/** Deposit settled through the payment provider: the booking is confirmed and announced. */
export async function confirmDepositPaid(reservationId: string, paymentId: string): Promise<void> {
  const now = new Date();
  const updated = await db
    .update(s.reservations)
    .set({ status: 'confirmed', depositStatus: 'paid', paymentId, updatedAt: now })
    .where(and(eq(s.reservations.id, reservationId), eq(s.reservations.status, 'pending')))
    .returning();
  const confirmed = updated[0];
  if (confirmed) {
    const branch = await getBranch(confirmed.branchId);
    if (branch) await announceBooking(confirmed, branch);
    return;
  }
  // Paid after the deposit window lapsed: confirm it if the table is still free, otherwise return the money.
  const r = await db.query.reservations.findFirst({ where: eq(s.reservations.id, reservationId) });
  if (!r || r.status !== 'cancelled' || r.depositStatus !== 'pending') return;
  const branch = await getBranch(r.branchId);
  const revived = branch
    ? await db.transaction(async (tx) => {
        const ctx = await availabilityContext(tx, branch, r.date, now);
        const slot = checkSlot(ctx, { date: r.date, partySize: r.partySize, area: r.area, now, minLeadMinutes: 0, time: r.time, excludeIds: [r.id] });
        if (!slot?.available) return null;
        const rows = await tx
          .update(s.reservations)
          .set({ status: 'confirmed', depositStatus: 'paid', paymentId, tableIds: slot.tableIds, cancelledAt: null, updatedAt: now })
          .where(and(eq(s.reservations.id, r.id), eq(s.reservations.status, 'cancelled')))
          .returning();
        return rows[0] ?? null;
      })
    : null;
  if (revived && branch) {
    await announceBooking(revived, branch);
    return;
  }
  const { refundPayment } = await import('./payments');
  await refundPayment(paymentId);
  await db.update(s.reservations).set({ depositStatus: 'refunded', paymentId, updatedAt: now }).where(eq(s.reservations.id, r.id));
}

/** Whether a pending booking's deposit can still be paid (its table is still held). */
export function depositOpen(r: Reservation, now: Date): boolean {
  return r.status === 'pending' && r.depositStatus === 'pending' && now.getTime() - r.createdAt.getTime() < DEPOSIT_WINDOW_MINUTES * MINUTE;
}

/** Cancels web bookings whose deposit was never paid, so their tables return to the pool. */
export async function expireUnpaidDeposits(now = new Date()): Promise<number> {
  const stale = new Date(now.getTime() - DEPOSIT_WINDOW_MINUTES * MINUTE);
  const expired = await db
    .update(s.reservations)
    .set({ status: 'cancelled', cancelledAt: now, updatedAt: now })
    .where(and(eq(s.reservations.status, 'pending'), eq(s.reservations.depositStatus, 'pending'), inArray(s.reservations.source, ['web', 'concierge']), lt(s.reservations.createdAt, stale)))
    .returning({ branchId: s.reservations.branchId, date: s.reservations.date });
  for (const key of new Set(expired.map((e) => `${e.branchId}|${e.date}`))) {
    const [branchId, date] = key.split('|') as [string, string];
    await processWaitlist(branchId, date, now);
  }
  return expired.length;
}

// ————————————————————————————————————————— links, labels & emails —————————————————————————————————————————

export function manageUrl(r: Pick<Reservation, 'id' | 'code' | 'locale'>): string {
  return absoluteUrl(`/${r.locale}/reserve/manage/${r.code}?token=${linkToken('reservation', r.id)}`);
}

/** The confirmation page without the locale prefix (for localised links). */
export function bookingPath(r: Pick<Reservation, 'id' | 'code'>): string {
  return `/reserve/confirmed/${r.code}?token=${linkToken('reservation', r.id)}`;
}

export function confirmationPath(r: Pick<Reservation, 'id' | 'code'>, locale: string): string {
  return `/${locale}${bookingPath(r)}`;
}

export function icsPath(r: Pick<Reservation, 'id' | 'code'>): string {
  return `/api/reservations/${r.code}/ics?token=${linkToken('reservation', r.id)}`;
}

/** Finds a reservation by its public code and the token from its link. */
export async function reservationByCode(code: string, token: string | null | undefined): Promise<Reservation | null> {
  if (!/^ZL-[A-Z0-9]{4,10}$/.test(code)) return null;
  const r = await db.query.reservations.findFirst({ where: eq(s.reservations.code, code) });
  return r && verifyLinkToken('reservation', r.id, token) ? r : null;
}

/** Labels for emails and pages, in the booking's own language and the house's time zone. */
export async function describeReservation(r: Reservation, branch: BranchDTO, locale = r.locale) {
  const t = await getTranslations({ locale, namespace: 'reserve' });
  const tu = await getTranslations({ locale, namespace: 'common.units' });
  return {
    branchName: tr(branch.name, locale),
    branchAddress: tr(branch.address, locale),
    dateLabel: formatDate(r.startsAt, locale, branch.timeZone, { weekday: 'long', day: 'numeric', month: 'long', year: 'numeric' }),
    timeLabel: formatClock(r.startsAt, locale, branch.timeZone),
    partyLabel: tu('guests', plural(r.partySize, locale)),
    areaLabel: t(`areas.${r.area}`),
    occasionLabel: r.occasion && (OCCASIONS as readonly string[]).includes(r.occasion) ? t(`occasions.${r.occasion}`) : null,
    depositLabel: r.depositAmount > 0 ? formatMoney(r.depositAmount, locale) : null,
    cutoffLabel: tu('hours', plural(rules.modifyCutoffHours, locale)),
  };
}

function calendarEvent(r: Reservation, branch: BranchDTO, labels: { branchAddress: string; partyLabel: string }) {
  return {
    uid: `reservation-${r.id}@${new URL(absoluteUrl('/')).hostname}`,
    title: `${tr(restaurantConfig.name, r.locale)} · ${tr(branch.shortName, r.locale)}`,
    start: r.startsAt,
    end: r.endsAt,
    location: labels.branchAddress,
    description: `${labels.partyLabel} · ${r.code}\n${manageUrl(r)}`,
    url: manageUrl(r),
    sequence: Math.floor(r.updatedAt.getTime() / 1000),
    alarmMinutes: 120,
  };
}

export function reservationIcs(r: Reservation, branch: BranchDTO, labels: { branchAddress: string; partyLabel: string }): string {
  return buildIcs(calendarEvent(r, branch, labels));
}

export function reservationCalendarUrl(r: Reservation, branch: BranchDTO, labels: { branchAddress: string; partyLabel: string }): string {
  const event = calendarEvent(r, branch, labels);
  return googleCalendarUrl({ title: event.title, start: r.startsAt, end: r.endsAt, location: labels.branchAddress, details: `${labels.partyLabel} · ${r.code}` });
}

async function emailProps(r: Reservation, branch: BranchDTO) {
  const labels = await describeReservation(r, branch);
  const t = await getTranslations({ locale: r.locale, namespace: 'reserve.confirmed' });
  return {
    ics: reservationIcs(r, branch, labels),
    props: {
      locale: r.locale,
      name: r.name,
      code: r.code,
      ...labels,
      manageUrl: manageUrl(r),
      calendarUrl: reservationCalendarUrl(r, branch, labels),
      whatsappUrl: whatsappUrl('', t('shareText', { house: labels.branchName, date: labels.dateLabel, time: labels.timeLabel, guests: labels.partyLabel })),
      mapUrl: googleDirectionsUrl({ lat: branch.lat, lng: branch.lng }),
    },
  };
}

function icsAttachment(r: Reservation, ics: string) {
  return { filename: `zill-${r.code}.ics`, content: Buffer.from(ics, 'utf8').toString('base64'), contentType: 'text/calendar; charset=utf-8; method=PUBLISH' };
}

/** Confirmation email (with the .ics) and a note for the hosts. */
async function announceBooking(r: Reservation, branch: BranchDTO): Promise<void> {
  const { props, ics } = await emailProps(r, branch);
  await mail(r.email, { name: 'reservation-confirmed', props }, { attachments: [icsAttachment(r, ics)], meta: { reservation: r.code } });
  await notifyStaff({
    role: 'host',
    branchId: r.branchId,
    kind: 'reservation',
    title: { ar: `حجزٌ جديد لـ${formatNumber(r.partySize, 'ar')}`, en: `New booking for ${r.partySize}` },
    body: { ar: `${r.name} · ${formatWallTime(r.time, 'ar')}`, en: `${r.name} · ${formatWallTime(r.time, 'en')}` },
    href: `/admin/reservations?date=${r.date}&branch=${r.branchId}`,
  });
}

// ————————————————————————————————————————— guest changes —————————————————————————————————————————

/** Guests may change or cancel online until the cut-off before their booking. */
export function changeable(r: Reservation, now: Date): boolean {
  return (r.status === 'confirmed' || r.status === 'pending') && r.startsAt.getTime() - now.getTime() >= rules.modifyCutoffHours * HOUR;
}

/** Party sizes a guest may move to online: a deposit fixes the party; otherwise stay below the deposit size. */
export function partyRangeForChange(r: Reservation): { min: number; max: number } {
  if (r.depositAmount > 0) return { min: r.partySize, max: r.partySize };
  const ceiling = rules.deposit.perGuest > 0 ? rules.deposit.minParty - 1 : rules.maxPartyOnline;
  return { min: 1, max: Math.max(r.partySize, Math.min(rules.maxPartyOnline, ceiling)) };
}

/** Guest cancellation through the manage link; a paid deposit is refunded when outside the cut-off. */
export async function cancelByGuest(r: Reservation, now: Date): Promise<{ refunded: boolean }> {
  const refund = r.depositStatus === 'paid' && r.startsAt.getTime() - now.getTime() >= rules.modifyCutoffHours * HOUR;
  const updated = await db
    .update(s.reservations)
    .set({ status: 'cancelled', cancelledAt: now, depositStatus: refund ? 'refunded' : r.depositStatus, updatedAt: now })
    .where(and(eq(s.reservations.id, r.id), inArray(s.reservations.status, ['pending', 'confirmed'])))
    .returning();
  if (!updated[0]) return { refunded: false };
  if (refund && r.paymentId) {
    const { refundPayment } = await import('./payments');
    await refundPayment(r.paymentId);
  }
  const branch = await getBranch(r.branchId);
  if (branch) {
    const labels = await describeReservation(r, branch);
    const tm = await getTranslations({ locale: r.locale, namespace: 'reserve.manage' });
    await mail(r.email, {
      name: 'reservation-cancelled',
      props: {
        locale: r.locale,
        name: r.name,
        code: r.code,
        branchName: labels.branchName,
        dateLabel: labels.dateLabel,
        timeLabel: labels.timeLabel,
        rebookUrl: absoluteUrl(`/${r.locale}/reserve?branch=${branch.slug}`),
        refundLabel: refund ? formatMoney(r.depositAmount, r.locale) : null,
        keptLabel: r.depositStatus === 'paid' && !refund ? tm('noRefund', { hours: labels.cutoffLabel }) : null,
      },
    });
    await notifyStaff({
      role: 'host',
      branchId: r.branchId,
      kind: 'reservation',
      title: { ar: `أُلغي الحجز ${r.code}`, en: `Booking ${r.code} cancelled` },
      body: { ar: `${r.name} · ${formatWallTime(r.time, 'ar')}`, en: `${r.name} · ${formatWallTime(r.time, 'en')}` },
      href: `/admin/reservations?date=${r.date}&branch=${r.branchId}`,
    });
  }
  await audit({ actor: null, action: 'reservation.cancel_by_guest', entity: 'reservation', entityId: r.id, summary: r.code });
  await processWaitlist(r.branchId, r.date, now);
  return { refunded: refund };
}

/** Moves a booking to a new slot; the old tables are released in the same transaction. */
export async function moveByGuest(r: Reservation, input: { date: string; time: string; partySize: number; area: AreaChoice; now: Date }): Promise<Reservation | null> {
  const branch = await getBranch(r.branchId);
  if (!branch) return null;
  const moved = await db.transaction(async (tx) => {
    const ctx = await availabilityContext(tx, branch, input.date, input.now);
    const slot = checkSlot(ctx, { date: input.date, partySize: input.partySize, area: input.area, now: input.now, minLeadMinutes: MIN_LEAD_MINUTES, time: input.time, excludeIds: [r.id] });
    if (!slot || !slot.available) return null;
    const rows = await tx
      .update(s.reservations)
      .set({ date: input.date, time: slot.time, startsAt: slot.startsAt, endsAt: slot.endsAt, partySize: input.partySize, area: input.area, tableIds: slot.tableIds, reminderSentAt: null, updatedAt: input.now })
      .where(and(eq(s.reservations.id, r.id), inArray(s.reservations.status, ['pending', 'confirmed'])))
      .returning();
    return rows[0] ?? null;
  });
  if (!moved) return null;
  const { props, ics } = await emailProps(moved, branch);
  await mail(moved.email, { name: 'reservation-updated', props }, { attachments: [icsAttachment(moved, ics)], meta: { reservation: moved.code } });
  await notifyStaff({
    role: 'host',
    branchId: r.branchId,
    kind: 'reservation',
    title: { ar: `نُقل الحجز ${r.code}`, en: `Booking ${r.code} moved` },
    body: { ar: `${moved.date} · ${formatWallTime(moved.time, 'ar')}`, en: `${moved.date} · ${formatWallTime(moved.time, 'en')}` },
    href: `/admin/reservations?date=${moved.date}&branch=${r.branchId}`,
  });
  await audit({ actor: null, action: 'reservation.move_by_guest', entity: 'reservation', entityId: r.id, summary: `${r.code}: ${r.date} ${r.time} → ${moved.date} ${moved.time}` });
  await processWaitlist(r.branchId, r.date, input.now);
  return moved;
}

// ————————————————————————————————————————— reminders —————————————————————————————————————————

/**
 * Reminder emails for confirmed bookings starting within the reminder window. Works whether the cron runs
 * hourly or daily; bookings made at the last minute are not reminded, and each booking is claimed before
 * sending so overlapping runs never send twice.
 */
export async function sendDueReminders(now = new Date()): Promise<number> {
  const horizon = new Date(now.getTime() + rules.reminderHoursBefore * HOUR);
  const due = await db.query.reservations.findMany({
    where: and(eq(s.reservations.status, 'confirmed'), isNull(s.reservations.reminderSentAt), gt(s.reservations.startsAt, now), lte(s.reservations.startsAt, horizon)),
  });
  let sent = 0;
  for (const r of due) {
    if (r.startsAt.getTime() - r.createdAt.getTime() < (rules.reminderHoursBefore / 2) * HOUR) continue;
    const claimed = await db
      .update(s.reservations)
      .set({ reminderSentAt: now })
      .where(and(eq(s.reservations.id, r.id), isNull(s.reservations.reminderSentAt), eq(s.reservations.status, 'confirmed')))
      .returning();
    const booking = claimed[0];
    if (!booking) continue;
    const branch = await getBranch(booking.branchId);
    if (!branch) continue;
    const { props } = await emailProps(booking, branch);
    await mail(booking.email, { name: 'reservation-reminder', props }, { meta: { reservation: booking.code } });
    sent++;
  }
  return sent;
}

// ————————————————————————————————————————— waitlist —————————————————————————————————————————

function formatDateLabel(date: string, branch: BranchDTO, locale: string): string {
  const noon = localToUtc(date, 12 * 60, branch.timeZone, true) ?? new Date(`${date}T12:00:00Z`);
  return formatDate(noon, locale, branch.timeZone, { weekday: 'long', day: 'numeric', month: 'long' });
}

export function waitlistSessionKey(entryId: string): string {
  return `waitlist:${entryId}`;
}

export async function joinWaitlist(input: { branch: BranchDTO; date: string; preferredTime: string; flexibleMinutes: number; partySize: number; name: string; email: string; phone: string; notes: string | null; locale: string }) {
  const id = createId();
  await db.insert(s.waitlistEntries).values({
    id,
    branchId: input.branch.id,
    date: input.date,
    preferredTime: input.preferredTime,
    flexibleMinutes: input.flexibleMinutes,
    partySize: input.partySize,
    name: input.name,
    email: input.email,
    phone: input.phone,
    notes: input.notes,
    locale: input.locale,
  });
  const tu = await getTranslations({ locale: input.locale, namespace: 'common.units' });
  await mail(input.email, {
    name: 'waitlist',
    props: {
      locale: input.locale,
      name: input.name,
      branchName: tr(input.branch.name, input.locale),
      dateLabel: formatDateLabel(input.date, input.branch, input.locale),
      timeLabel: formatWallTime(input.preferredTime, input.locale),
      partyLabel: tu('guests', plural(input.partySize, input.locale)),
    },
  });
  await notifyStaff({
    role: 'host',
    branchId: input.branch.id,
    kind: 'waitlist',
    title: { ar: `قائمة الانتظار: مجموعة من ${formatNumber(input.partySize, 'ar')}`, en: `Waitlist: party of ${input.partySize}` },
    body: { ar: `${input.name} · ${formatWallTime(input.preferredTime, 'ar')}`, en: `${input.name} · ${formatWallTime(input.preferredTime, 'en')}` },
    href: `/admin/reservations?date=${input.date}&branch=${input.branch.id}&view=waitlist`,
  });
  // A table may already be free near the preferred time (another guest may just have cancelled).
  await processWaitlist(input.branch.id, input.date, new Date());
  return id;
}

/** Minutes between a slot and a preferred 'HH:mm' (after-midnight seatings compare on the same service day). */
function distance(slotMinutes: number, preferred: string): number {
  const p = parseTime(preferred);
  return Math.min(Math.abs(slotMinutes - p), Math.abs(slotMinutes - (p + 1440)));
}

/**
 * Offers freed tables to the waitlist, first come first served: lapsed offers expire, then each waiting
 * party is offered the free time closest to its preference (within its flexibility), held for an hour.
 */
export async function processWaitlist(branchId: string, date: string, now = new Date()): Promise<number> {
  const branch = await getBranch(branchId);
  if (!branch) return 0;
  const offered = await db
    .select()
    .from(s.waitlistEntries)
    .where(and(eq(s.waitlistEntries.branchId, branchId), eq(s.waitlistEntries.date, date), eq(s.waitlistEntries.status, 'offered')));
  for (const e of offered) {
    const hold = await db.query.reservationHolds.findFirst({ where: eq(s.reservationHolds.sessionKey, waitlistSessionKey(e.id)) });
    if (!hold || hold.expiresAt <= now) {
      await db.update(s.waitlistEntries).set({ status: 'expired', updatedAt: now }).where(and(eq(s.waitlistEntries.id, e.id), eq(s.waitlistEntries.status, 'offered')));
    }
  }
  if (date < toDateString(now, branch.timeZone)) {
    await db
      .update(s.waitlistEntries)
      .set({ status: 'expired', updatedAt: now })
      .where(and(eq(s.waitlistEntries.branchId, branchId), eq(s.waitlistEntries.date, date), eq(s.waitlistEntries.status, 'waiting')));
    return 0;
  }
  const waiting = await db
    .select()
    .from(s.waitlistEntries)
    .where(and(eq(s.waitlistEntries.branchId, branchId), eq(s.waitlistEntries.date, date), eq(s.waitlistEntries.status, 'waiting')))
    .orderBy(asc(s.waitlistEntries.createdAt));
  let count = 0;
  for (const e of waiting) {
    const ctx = await availabilityContext(db, branch, date, now);
    const flex = e.flexibleMinutes >= 1440 ? Number.POSITIVE_INFINITY : e.flexibleMinutes;
    const best = computeAvailability(ctx, { date, partySize: e.partySize, area: 'any', now, minLeadMinutes: 60 })
      .filter((slot) => slot.available && distance(slot.minutes, e.preferredTime) <= flex)
      .sort((a, b) => distance(a.minutes, e.preferredTime) - distance(b.minutes, e.preferredTime))[0];
    if (!best) continue;
    const hold = await holdSlot({ branch, date, time: best.time, partySize: e.partySize, area: 'any', sessionKey: waitlistSessionKey(e.id), now, minutes: WAITLIST_HOLD_MINUTES, minLeadMinutes: 60 });
    if (!hold) continue;
    const claimed = await db
      .update(s.waitlistEntries)
      .set({ status: 'offered', updatedAt: now })
      .where(and(eq(s.waitlistEntries.id, e.id), eq(s.waitlistEntries.status, 'waiting')))
      .returning();
    if (!claimed[0]) {
      await releaseHolds(waitlistSessionKey(e.id));
      continue;
    }
    const tu = await getTranslations({ locale: e.locale, namespace: 'common.units' });
    await mail(e.email, {
      name: 'waitlist-offer',
      props: {
        locale: e.locale,
        name: e.name,
        branchName: tr(branch.name, e.locale),
        dateLabel: formatDateLabel(date, branch, e.locale),
        timeLabel: formatClock(hold.startsAt, e.locale, branch.timeZone),
        partyLabel: tu('guests', plural(e.partySize, e.locale)),
        expiresLabel: formatClock(hold.expiresAt, e.locale, branch.timeZone),
        bookUrl: absoluteUrl(`/${e.locale}/reserve/offer/${e.id}?token=${linkToken('waitlist', e.id)}`),
      },
    });
    count++;
  }
  return count;
}

/** Runs the waitlist for every house and date that has someone waiting or an open offer. */
export async function processAllWaitlists(now = new Date()): Promise<number> {
  const rows = await db
    .selectDistinct({ branchId: s.waitlistEntries.branchId, date: s.waitlistEntries.date })
    .from(s.waitlistEntries)
    .where(inArray(s.waitlistEntries.status, ['waiting', 'offered']));
  let offered = 0;
  for (const row of rows) offered += await processWaitlist(row.branchId, row.date, now);
  return offered;
}

/** A waitlist offer opened from its email link, with the table held for the guest (if still held). */
export async function waitlistOffer(entryId: string, token: string | null | undefined, now: Date) {
  if (!verifyLinkToken('waitlist', entryId, token)) return null;
  const entry = await db.query.waitlistEntries.findFirst({ where: eq(s.waitlistEntries.id, entryId) });
  if (!entry) return null;
  const hold =
    entry.status === 'offered'
      ? await db.query.reservationHolds.findFirst({ where: and(eq(s.reservationHolds.sessionKey, waitlistSessionKey(entry.id)), gt(s.reservationHolds.expiresAt, now)) })
      : null;
  return { entry, hold: hold ?? null };
}

export async function markWaitlistBooked(entryId: string, reservationId: string): Promise<void> {
  await db.update(s.waitlistEntries).set({ status: 'booked', reservationId, updatedAt: new Date() }).where(eq(s.waitlistEntries.id, entryId));
}
