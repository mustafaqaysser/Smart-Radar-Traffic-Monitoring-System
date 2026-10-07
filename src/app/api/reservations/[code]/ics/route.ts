import { NextResponse, type NextRequest } from 'next/server';
import { getBranch } from '@/lib/queries/branches';
import { describeReservation, reservationByCode, reservationIcs } from '@/lib/server/reservations';

/** The booking as a calendar file (Apple Calendar, Outlook…), from the confirmation and manage pages. */
export async function GET(request: NextRequest, { params }: { params: Promise<{ code: string }> }) {
  const { code } = await params;
  const r = await reservationByCode(code, request.nextUrl.searchParams.get('token'));
  if (!r || r.status === 'cancelled') return NextResponse.json({ error: 'notFound' }, { status: 404 });
  const branch = await getBranch(r.branchId);
  if (!branch) return NextResponse.json({ error: 'notFound' }, { status: 404 });
  const labels = await describeReservation(r, branch);
  return new NextResponse(reservationIcs(r, branch, labels), {
    headers: {
      'content-type': 'text/calendar; charset=utf-8',
      'content-disposition': `attachment; filename="zill-${r.code}.ics"`,
      'cache-control': 'private, no-store',
    },
  });
}
