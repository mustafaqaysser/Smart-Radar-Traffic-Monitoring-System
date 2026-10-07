import { NextResponse, type NextRequest } from 'next/server';
import { confirmSubscriber, localeFrom, subscriberByToken } from '@/lib/server/newsletter';

/** Double opt-in: the link in the confirmation email lands here, then on the newsletter page. */
export async function GET(request: NextRequest) {
  const url = request.nextUrl;
  const locale = localeFrom(url.searchParams.get('locale'));
  const subscriber = await subscriberByToken(url.searchParams.get('token'));
  if (subscriber && subscriber.status !== 'unsubscribed') await confirmSubscriber(subscriber.id);
  const target = new URL(`/${locale}/newsletter`, request.url);
  target.searchParams.set('status', subscriber && subscriber.status !== 'unsubscribed' ? 'confirmed' : 'invalid');
  return NextResponse.redirect(target, 303);
}
