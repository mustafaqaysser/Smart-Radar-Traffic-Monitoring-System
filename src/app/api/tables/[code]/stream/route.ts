import type { NextRequest } from 'next/server';
import { featureEnabled } from '@/lib/server/settings';
import { tableActivity, tableByCode } from '@/lib/server/table';

export const dynamic = 'force-dynamic';
export const maxDuration = 60;

const TICK_MS = 3000;
/** Each connection lives under a serverless time limit; EventSource reconnects on its own. */
const LIFETIME_MS = 50_000;

/**
 * The table's calls and orders as Server-Sent Events, so guests see "on the way" the moment a waiter answers
 * and each order move from the kitchen to the table.
 */
export async function GET(request: NextRequest, { params }: { params: Promise<{ code: string }> }) {
  if (!(await featureEnabled('dineInQr'))) return new Response('Not found', { status: 404 });
  const { code } = await params;
  const found = await tableByCode(code);
  if (!found) return new Response('Not found', { status: 404 });
  const encoder = new TextEncoder();
  let last = '';
  let timer: ReturnType<typeof setTimeout> | null = null;

  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      const started = Date.now();
      const close = () => {
        if (timer) clearTimeout(timer);
        try {
          controller.close();
        } catch {
          // Already closed by the client.
        }
      };
      controller.enqueue(encoder.encode(`retry: ${TICK_MS}\n\n`));
      const tick = async () => {
        if (request.signal.aborted) return close();
        const activity = await tableActivity(found.table);
        const key = JSON.stringify(activity);
        controller.enqueue(encoder.encode(key === last ? ': still here\n\n' : `event: activity\ndata: ${key}\n\n`));
        last = key;
        if (Date.now() - started > LIFETIME_MS) return close();
        timer = setTimeout(() => void tick().catch(close), TICK_MS);
      };
      void tick().catch(close);
      request.signal.addEventListener('abort', close);
    },
    cancel() {
      if (timer) clearTimeout(timer);
    },
  });

  return new Response(stream, {
    headers: { 'content-type': 'text/event-stream; charset=utf-8', 'cache-control': 'no-cache, no-transform', connection: 'keep-alive', 'x-accel-buffering': 'no' },
  });
}
