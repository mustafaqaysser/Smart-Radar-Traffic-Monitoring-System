import 'server-only';
import { and, eq, gt, inArray, lt, ne, or, sql } from 'drizzle-orm';
import { revalidateTag } from 'next/cache';
import { db } from '@/lib/db/client';
import * as s from '@/lib/db/schema';
import { formatClock, formatDate, formatMoney, formatNumber } from '@/lib/i18n/format';
import { tr } from '@/lib/i18n/localized';
import { getBranch } from '@/lib/queries/branches';
import { TAGS } from '@/lib/queries/cache';
import { audit, notifyStaff } from '@/lib/server/audit';
import { mail } from '@/lib/server/mail';
import { linkToken, verifyLinkToken } from '@/lib/server/tokens';
import { googleDirectionsUrl } from '@/lib/services/maps';
import { absoluteUrl } from '@/lib/site/url';
import { CODE_PREFIX, createCode, createId } from '@/lib/utils/id';
import type { StartedPayment } from './payments';

export type EventBooking = typeof s.eventBookings.$inferSelect;

const MINUTE = 60_000;
/** An unpaid ticket keeps its seats this long. */
export const TICKET_PAYMENT_MINUTES = 30;

/** Whether an unpaid ticket can still be paid for (its seats are still held). */
export function ticketPaymentOpen(b: Pick<EventBooking, 'status' | 'createdAt'>, now: Date): boolean {
  return b.status === 'pending_payment' && now.getTime() - b.createdAt.getTime() < TICKET_PAYMENT_MINUTES * MINUTE;
}

export function ticketPath(b: Pick<EventBooking, 'id' | 'code'>): string {
  return `/experiences/ticket/${b.code}?token=${linkToken('ticket', b.id)}`;
}

export function ticketQrUrl(b: Pick<EventBooking, 'id' | 'code'>): string {
  return absoluteUrl(`/api/tickets/${b.code}/qr?token=${linkToken('ticket', b.id)}`);
}

/** What the QR code on a ticket opens: the staff check-in view for that ticket. */
export function ticketIcsPath(b: Pick<EventBooking, 'id' | 'code'>): string {
  return `/api/tickets/${b.code}/ics?token=${linkToken('ticket', b.id)}`;
}

export function checkInUrl(code: string): string {
  return absoluteUrl(`/admin/events/check-in?code=${encodeURIComponent(code)}`);
}

export async function bookingByCode(code: string, token: string | null | undefined): Promise<EventBooking | null> {
  if (!/^ZT-[A-Z0-9]{4,10}$/.test(code)) return null;
  const booking = await db.query.eventBookings.findFirst({ where: eq(s.eventBookings.code, code) });
  return booking && verifyLinkToken('ticket', booking.id, token) ? booking : null;
}

function refreshEvents() {
  revalidateTag(TAGS.events, { expire: 0 });
}

export type BookEventResult = { ok: true; booking: EventBooking; payment: StartedPayment | null } | { ok: false; error: 'not_found' | 'started' | 'sold_out' | 'ticket_sold_out' };

/**
 * Books tickets in one write transaction: seats are counted from live bookings (unpaid ones only while their
 * payment window is open), against both the event's capacity and the ticket type's own limit.
 */
export async function bookEvent(input: { eventSlug: string; ticketTypeId: string; quantity: number; name: string; email: string; phone: string; locale: string; userId: string | null; now: Date }): Promise<BookEventResult> {
  const event = await db.query.events.findFirst({ where: and(eq(s.events.slug, input.eventSlug), eq(s.events.isPublished, true)) });
  if (!event) return { ok: false, error: 'not_found' };
  if (event.startsAt <= input.now) return { ok: false, error: 'started' };
  const ticket = await db.query.eventTicketTypes.findFirst({ where: and(eq(s.eventTicketTypes.id, input.ticketTypeId), eq(s.eventTicketTypes.eventId, event.id)) });
  if (!ticket) return { ok: false, error: 'not_found' };
  const total = ticket.price * input.quantity;
  const fresh = new Date(input.now.getTime() - TICKET_PAYMENT_MINUTES * MINUTE);

  type Outcome = { kind: 'full'; error: 'sold_out' | 'ticket_sold_out' } | { kind: 'booked'; booking: EventBooking };
  const outcome = await db.transaction(async (tx): Promise<Outcome> => {
    const live = and(eq(s.eventBookings.eventId, event.id), ne(s.eventBookings.status, 'cancelled'), or(ne(s.eventBookings.status, 'pending_payment'), gt(s.eventBookings.createdAt, fresh)));
    const taken = await tx.select({ ticketTypeId: s.eventBookings.ticketTypeId, qty: sql<number>`sum(${s.eventBookings.quantity})` }).from(s.eventBookings).where(live).groupBy(s.eventBookings.ticketTypeId);
    const seats = taken.reduce((n, r) => n + Number(r.qty), 0);
    if (seats + input.quantity > event.capacity) return { kind: 'full', error: 'sold_out' };
    const sold = Number(taken.find((r) => r.ticketTypeId === ticket.id)?.qty ?? 0);
    if (ticket.capacity !== null && sold + input.quantity > ticket.capacity) return { kind: 'full', error: 'ticket_sold_out' };
    const rows = await tx
      .insert(s.eventBookings)
      .values({
        id: createId(),
        code: createCode(CODE_PREFIX.ticket),
        eventId: event.id,
        ticketTypeId: ticket.id,
        quantity: input.quantity,
        total,
        name: input.name,
        email: input.email,
        phone: input.phone,
        locale: input.locale,
        userId: input.userId,
        status: total > 0 ? 'pending_payment' : 'confirmed',
        createdAt: input.now,
      })
      .returning();
    return { kind: 'booked', booking: rows[0] as EventBooking };
  });
  if (outcome.kind === 'full') return { ok: false, error: outcome.error };
  refreshEvents();
  const booking = outcome.booking;
  if (total === 0) {
    await sendTicket(booking);
    return { ok: true, booking, payment: null };
  }
  const { startPayment } = await import('./payments');
  const payment = await startPayment({ purpose: 'event', referenceId: booking.id, amount: total, description: `${tr(event.title, 'en')} · ${booking.code}`, email: booking.email, locale: input.locale, returnPath: `/${input.locale}${ticketPath(booking)}` });
  return { ok: true, booking, payment };
}

/** Paid: the seats are confirmed and the ticket (with its QR code) is emailed. Late payments are refunded. */
export async function confirmEventBookingPaid(bookingId: string, paymentId: string): Promise<void> {
  const updated = await db.update(s.eventBookings).set({ status: 'confirmed', paymentId }).where(and(eq(s.eventBookings.id, bookingId), eq(s.eventBookings.status, 'pending_payment'))).returning();
  if (updated[0]) {
    refreshEvents();
    await sendTicket(updated[0]);
    return;
  }
  const booking = await db.query.eventBookings.findFirst({ where: eq(s.eventBookings.id, bookingId) });
  if (booking?.status === 'cancelled' && !booking.paymentId) {
    const { refundPayment } = await import('./payments');
    await refundPayment(paymentId);
    await db.update(s.eventBookings).set({ paymentId }).where(eq(s.eventBookings.id, bookingId));
  }
}

/** Unpaid tickets past their window release their seats. */
export async function expireUnpaidBookings(now = new Date()): Promise<number> {
  const stale = await db
    .update(s.eventBookings)
    .set({ status: 'cancelled' })
    .where(and(eq(s.eventBookings.status, 'pending_payment'), lt(s.eventBookings.createdAt, new Date(now.getTime() - TICKET_PAYMENT_MINUTES * MINUTE))))
    .returning({ id: s.eventBookings.id });
  if (stale.length) refreshEvents();
  return stale.length;
}

export async function ticketDetails(booking: EventBooking, locale = booking.locale) {
  const [event, ticket] = await Promise.all([
    db.query.events.findFirst({ where: eq(s.events.id, booking.eventId) }),
    db.query.eventTicketTypes.findFirst({ where: eq(s.eventTicketTypes.id, booking.ticketTypeId) }),
  ]);
  if (!event || !ticket) return null;
  const branch = await getBranch(event.branchId);
  if (!branch) return null;
  return {
    event,
    ticket,
    branch,
    title: tr(event.title, locale),
    dateLabel: formatDate(event.startsAt, locale, branch.timeZone, { weekday: 'long', day: 'numeric', month: 'long' }),
    timeLabel: formatClock(event.startsAt, locale, branch.timeZone),
    branchName: tr(branch.name, locale),
    ticketLabel: tr(ticket.name, locale),
    quantityLabel: formatNumber(booking.quantity, locale),
    totalLabel: formatMoney(booking.total, locale),
  };
}

async function sendTicket(booking: EventBooking): Promise<void> {
  const d = await ticketDetails(booking);
  if (!d) return;
  await mail(
    booking.email,
    {
      name: 'event-ticket',
      props: {
        locale: booking.locale,
        name: booking.name,
        eventTitle: d.title,
        dateLabel: d.dateLabel,
        timeLabel: d.timeLabel,
        branchName: d.branchName,
        ticketLabel: d.ticketLabel,
        quantityLabel: d.quantityLabel,
        totalLabel: d.totalLabel,
        code: booking.code,
        qrUrl: ticketQrUrl(booking),
        mapUrl: googleDirectionsUrl({ lat: d.branch.lat, lng: d.branch.lng }),
      },
    },
    { meta: { ticket: booking.code } },
  );
  await notifyStaff({
    role: 'manager',
    branchId: d.event.branchId,
    kind: 'event',
    title: { ar: `تذاكر: ${tr(d.event.title, 'ar')}`, en: `Tickets: ${tr(d.event.title, 'en')}` },
    body: { ar: `${booking.name} · ${formatNumber(booking.quantity, 'ar')}`, en: `${booking.name} · ${booking.quantity}` },
    href: `/admin/events/${d.event.id}`,
  });
}

/** Staff check-in at the door (from the QR code or the code typed in). */
export async function checkInTicket(code: string, actor: { id: string; email: string }): Promise<{ ok: true; booking: EventBooking } | { ok: false; error: 'not_found' | 'not_confirmed' | 'already'; booking?: EventBooking }> {
  const booking = await db.query.eventBookings.findFirst({ where: eq(s.eventBookings.code, code.trim().toUpperCase()) });
  if (!booking) return { ok: false, error: 'not_found' };
  if (booking.status === 'checked_in') return { ok: false, error: 'already', booking };
  if (booking.status !== 'confirmed') return { ok: false, error: 'not_confirmed', booking };
  const rows = await db.update(s.eventBookings).set({ status: 'checked_in', checkedInAt: new Date() }).where(and(eq(s.eventBookings.id, booking.id), eq(s.eventBookings.status, 'confirmed'))).returning();
  if (!rows[0]) return { ok: false, error: 'already', booking };
  await audit({ actor, action: 'event.check_in', entity: 'event_booking', entityId: booking.id, summary: booking.code });
  return { ok: true, booking: rows[0] };
}

/** Bookings for a signed-in guest (account page). */
export async function bookingsForUser(userId: string, email: string) {
  return db.query.eventBookings.findMany({ where: and(or(eq(s.eventBookings.userId, userId), eq(s.eventBookings.email, email)), inArray(s.eventBookings.status, ['confirmed', 'checked_in'])), limit: 30 });
}
