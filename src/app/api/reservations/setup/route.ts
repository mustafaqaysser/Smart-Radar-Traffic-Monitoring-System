import { NextResponse, type NextRequest } from 'next/server';
import { getCurrentUser } from '@/lib/auth/session';
import { localeFrom } from '@/lib/server/newsletter';
import { reserveSetup } from '@/lib/server/reserve-setup';
import { featureEnabled } from '@/lib/server/settings';
import { getSelectedBranch } from '@/lib/site/selection';

/** Data for the booking sheet opened from the mobile dock (the /reserve page renders the same on the server). */
export async function GET(request: NextRequest) {
  if (!(await featureEnabled('reservations'))) return NextResponse.json({ error: 'disabled' }, { status: 404 });
  const locale = localeFrom(request.nextUrl.searchParams.get('locale'));
  const [setup, user, newsletter, selected] = await Promise.all([reserveSetup(locale), getCurrentUser(), featureEnabled('newsletter'), getSelectedBranch()]);
  return NextResponse.json(
    { setup, guest: user ? { name: user.name, email: user.email, phone: user.phone ?? '' } : null, newsletter, selected: selected?.slug ?? null },
    { headers: { 'cache-control': 'private, no-store' } },
  );
}
