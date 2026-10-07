import { NextResponse, type NextRequest } from 'next/server';
import { localeFrom, subscriberByToken, unsubscribe } from '@/lib/server/newsletter';

async function handle(request: NextRequest, redirect: boolean) {
  const url = request.nextUrl;
  const locale = localeFrom(url.searchParams.get('locale'));
  const subscriber = await subscriberByToken(url.searchParams.get('token'));
  if (subscriber) await unsubscribe(subscriber.id);
  if (!redirect) return new NextResponse(null, { status: subscriber ? 200 : 404 });
  const target = new URL(`/${locale}/newsletter`, request.url);
  target.searchParams.set('status', subscriber ? 'unsubscribed' : 'invalid');
  return NextResponse.redirect(target, 303);
}

/** Link in every letter. */
export function GET(request: NextRequest) {
  return handle(request, true);
}

/** RFC 8058 one-click unsubscribe (List-Unsubscribe-Post), sent by mail clients. */
export function POST(request: NextRequest) {
  return handle(request, false);
}
