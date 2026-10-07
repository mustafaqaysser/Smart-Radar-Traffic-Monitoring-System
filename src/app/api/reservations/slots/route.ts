import { NextResponse, type NextRequest } from 'next/server';
import { z } from 'zod';
import restaurantConfig from '@config';
import { normalizeDigits } from '@/lib/i18n/digits';
import { getBranch } from '@/lib/queries/branches';
import { ensureBookingSession } from '@/lib/server/booking-session';
import { AREA_CHOICES, bookingWindow, findSlots, reservationByCode } from '@/lib/server/reservations';
import { featureEnabled } from '@/lib/server/settings';
import { limitByIp } from '@/lib/services/rate-limit';
import { toLocalMinutes } from '@/lib/time/zoned';
import { sunTimes } from '@/lib/time/sun';

const querySchema = z.object({
  branch: z.string().min(1).max(64),
  date: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
  party: z.coerce.number().int().min(1).max(restaurantConfig.reservations.maxPartyOnline),
  area: z.enum(AREA_CHOICES).default('any'),
  code: z.string().max(16).optional(),
  token: z.string().max(64).optional(),
});

/** Bookable times for a house, day and party (plus the day's maghrib for the time picker). */
export async function GET(request: NextRequest) {
  const rl = await limitByIp('reserve-slots', 240, 600);
  if (!rl.ok) return NextResponse.json({ error: 'rateLimited' }, { status: 429, headers: { 'retry-after': String(rl.retryAfterSeconds) } });
  const params = Object.fromEntries(request.nextUrl.searchParams.entries());
  const parsed = querySchema.safeParse({ ...params, party: normalizeDigits(params.party ?? '') });
  if (!parsed.success) return NextResponse.json({ error: 'validation' }, { status: 400 });
  const q = parsed.data;
  if (!(await featureEnabled('reservations'))) return NextResponse.json({ error: 'disabled' }, { status: 404 });
  const branch = await getBranch(q.branch);
  if (!branch || !branch.reservationsEnabled) return NextResponse.json({ error: 'notFound' }, { status: 404 });
  const now = new Date();
  const { first, last } = bookingWindow(branch, now);
  if (q.date < first || q.date > last) return NextResponse.json({ error: 'window' }, { status: 400 });

  // On the manage page the guest's own booking must not block the times around it.
  const own = q.code ? await reservationByCode(q.code, q.token) : null;
  // Issue the booking session now, so holding a time later never has to set a cookie.
  if (!own) await ensureBookingSession();
  const slots = await findSlots(branch, q.date, q.party, q.area, now, own ? [own.id] : []);
  const sunset = sunTimes(q.date, branch.lat, branch.lng).sunset;
  return NextResponse.json(
    { date: q.date, slots, sunset: sunset ? { at: sunset.toISOString(), minutes: toLocalMinutes(sunset, branch.timeZone) } : null },
    { headers: { 'cache-control': 'no-store' } },
  );
}
