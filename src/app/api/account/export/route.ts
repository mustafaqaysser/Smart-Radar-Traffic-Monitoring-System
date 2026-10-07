import { getCurrentUser } from '@/lib/auth/session';
import { exportAccountData } from '@/lib/server/account';
import { limitByIp } from '@/lib/services/rate-limit';

/** "Download your data": everything kept about the signed-in guest, as a JSON file. */
export async function GET() {
  const user = await getCurrentUser();
  if (!user) return new Response('Sign in to download your data.', { status: 401, headers: { 'Cache-Control': 'no-store' } });
  const rl = await limitByIp('account:export', 6, 3600);
  if (!rl.ok) return new Response('Too many downloads; try again later.', { status: 429, headers: { 'Retry-After': String(rl.retryAfterSeconds), 'Cache-Control': 'no-store' } });
  const data = await exportAccountData(user);
  const day = new Date().toISOString().slice(0, 10);
  return new Response(JSON.stringify(data, null, 2), {
    headers: {
      'Content-Type': 'application/json; charset=utf-8',
      'Content-Disposition': `attachment; filename="zill-data-${day}.json"`,
      'Cache-Control': 'no-store',
    },
  });
}
