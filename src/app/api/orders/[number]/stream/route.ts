import { eq } from 'drizzle-orm';
import type { NextRequest } from 'next/server';
import { db } from '@/lib/db/client';
import { orders } from '@/lib/db/schema';
import { orderByNumber, trackingSnapshot } from '@/lib/server/orders';

export const dynamic = 'force-dynamic';
export const maxDuration = 60;

const TICK_MS = 3000;
/** Each connection lives under a serverless time limit; EventSource reconnects on its own. */
const LIFETIME_MS = 50_000;
const DONE = new Set(['completed', 'rejected', 'cancelled']);

/** Live order status as Server-Sent Events (no paid real-time service: a short poll of the database). */
export async function GET(request: NextRequest, { params }: { params: Promise<{ number: string }> }) {
  const { number } = await params;
  const order = await orderByNumber(number, request.nextUrl.searchParams.get('token'));
  if (!order) return new Response('Not found', { status: 404 });
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
        const current = await db.query.orders.findFirst({ where: eq(orders.id, order.id) });
        if (!current) return close();
        const snapshot = await trackingSnapshot(current);
        const key = `${snapshot.status}|${snapshot.updatedAt}|${snapshot.promisedAt}`;
        controller.enqueue(encoder.encode(key === last ? ': still here\n\n' : `event: status\ndata: ${JSON.stringify(snapshot)}\n\n`));
        last = key;
        if (DONE.has(snapshot.status) || Date.now() - started > LIFETIME_MS) return close();
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
