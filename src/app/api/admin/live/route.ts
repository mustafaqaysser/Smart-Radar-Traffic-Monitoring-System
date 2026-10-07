import { getCurrentUser } from '@/lib/auth/session';
import { isStaff } from '@/lib/auth/permissions';
import { getAdminScope } from '@/lib/admin/context';
import { livePulse } from '@/lib/admin/live';

export const dynamic = 'force-dynamic';

const POLL_MS = 3000;
const LIFETIME_MS = 50_000;

/**
 * Server-Sent Events for open admin screens: a pulse every few seconds when something changed (and a comment
 * line otherwise to keep the connection open). The stream ends after ~50 s and the browser reconnects, so it
 * works on serverless hosts that cap request duration.
 */
export async function GET(request: Request) {
  const user = await getCurrentUser();
  if (!user || !isStaff(user.role)) return new Response('Unauthorized', { status: 401 });
  const scope = await getAdminScope(user);
  const encoder = new TextEncoder();
  const stream = new ReadableStream<Uint8Array>({
    async start(controller) {
      let last = '';
      let open = true;
      const started = Date.now();
      const close = () => {
        if (!open) return;
        open = false;
        try {
          controller.close();
        } catch {
          // The client has already gone.
        }
      };
      const send = (chunk: string) => {
        if (!open) return;
        try {
          controller.enqueue(encoder.encode(chunk));
        } catch {
          open = false;
        }
      };
      request.signal.addEventListener('abort', close);
      send(`retry: 2000\n\n`);
      while (open && Date.now() - started < LIFETIME_MS) {
        try {
          const data = JSON.stringify(await livePulse(user, scope));
          send(data === last ? `: idle\n\n` : `event: pulse\ndata: ${data}\n\n`);
          last = data;
        } catch {
          send(`: retrying\n\n`);
        }
        await new Promise((r) => setTimeout(r, POLL_MS));
      }
      close();
    },
  });
  return new Response(stream, {
    headers: { 'content-type': 'text/event-stream; charset=utf-8', 'cache-control': 'no-cache, no-transform', connection: 'keep-alive', 'x-accel-buffering': 'no' },
  });
}
