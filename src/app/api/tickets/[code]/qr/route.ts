import QRCode from 'qrcode';
import type { NextRequest } from 'next/server';
import { bookingByCode, checkInUrl } from '@/lib/server/events';

/** A ticket's QR code (PNG, for the email and the ticket page). It opens the staff check-in for the ticket. */
export async function GET(request: NextRequest, { params }: { params: Promise<{ code: string }> }) {
  const { code } = await params;
  const booking = await bookingByCode(code, request.nextUrl.searchParams.get('token'));
  if (!booking) return new Response('Not found', { status: 404 });
  const png = await QRCode.toBuffer(checkInUrl(booking.code), { type: 'png', margin: 1, width: 360, errorCorrectionLevel: 'M', color: { dark: '#221D27', light: '#FBF8F4' } });
  return new Response(new Uint8Array(png), { headers: { 'content-type': 'image/png', 'cache-control': 'private, max-age=86400' } });
}
