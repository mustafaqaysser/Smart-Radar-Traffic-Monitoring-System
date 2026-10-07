import { NextResponse, type NextRequest } from 'next/server';
import { buildIcs } from '@/lib/domain/calendar';
import { tr } from '@/lib/i18n/localized';
import { bookingByCode, ticketDetails } from '@/lib/server/events';
import { absoluteUrl } from '@/lib/site/url';

/** A ticket as a calendar file. */
export async function GET(request: NextRequest, { params }: { params: Promise<{ code: string }> }) {
  const { code } = await params;
  const booking = await bookingByCode(code, request.nextUrl.searchParams.get('token'));
  if (!booking || booking.status === 'cancelled') return NextResponse.json({ error: 'notFound' }, { status: 404 });
  const d = await ticketDetails(booking);
  if (!d) return NextResponse.json({ error: 'notFound' }, { status: 404 });
  const ics = buildIcs({
    uid: `ticket-${booking.id}@${new URL(absoluteUrl('/')).hostname}`,
    title: `${d.title} · Zill`,
    start: d.event.startsAt,
    end: d.event.endsAt,
    location: tr(d.branch.address, booking.locale),
    description: `${d.ticketLabel} × ${d.quantityLabel} · ${booking.code}`,
    alarmMinutes: 120,
  });
  return new NextResponse(ics, { headers: { 'content-type': 'text/calendar; charset=utf-8', 'content-disposition': `attachment; filename="zill-${booking.code}.ics"`, 'cache-control': 'private, no-store' } });
}
