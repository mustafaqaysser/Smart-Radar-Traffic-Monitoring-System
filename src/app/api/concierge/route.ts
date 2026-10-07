import Anthropic from '@anthropic-ai/sdk';
import type { NextRequest } from 'next/server';
import { z } from 'zod';
import { routing } from '@/i18n/routing';
import { getCurrentUser } from '@/lib/auth/session';
import { CONCIERGE_MODEL, CONCIERGE_TOOLS, conciergeEnabled, requestContext, runConciergeTool, stableInstructions } from '@/lib/server/concierge';
import { limitByIp } from '@/lib/services/rate-limit';
import { siteUrl } from '@/lib/site/url';

export const dynamic = 'force-dynamic';
export const maxDuration = 120;

/** A conversation as the browser keeps it: the visible turns only (tool calls stay on the server). */
const bodySchema = z.object({
  locale: z.enum(routing.locales),
  messages: z
    .array(z.object({ role: z.enum(['user', 'assistant']), content: z.string().trim().min(1).max(2000) }))
    .min(1)
    .max(24)
    .refine((m) => m[0]?.role === 'user' && m[m.length - 1]?.role === 'user', 'The conversation starts and ends with the guest.'),
});

/** Tool rounds per guest message (check, then book, with room for a retry). */
const MAX_ROUNDS = 6;

let client: Anthropic | null = null;
const anthropic = () => (client ??= new Anthropic());

/**
 * The concierge: Claude with three tools (what is served now, free tables, book a table), streamed to the
 * browser as Server-Sent Events. Off unless the owner enables it and ANTHROPIC_API_KEY is set.
 */
export async function POST(request: NextRequest) {
  if (!(await conciergeEnabled())) return new Response('Not found', { status: 404 });
  // Same-origin only: the endpoint can book tables, so other sites may not post to it.
  const origin = request.headers.get('origin');
  if (origin && origin !== new URL(siteUrl()).origin && origin !== request.nextUrl.origin) return new Response('Forbidden', { status: 403 });
  const rl = await limitByIp('concierge', 30, 600);
  if (!rl.ok) return Response.json({ error: 'rateLimited' }, { status: 429, headers: { 'Retry-After': String(rl.retryAfterSeconds) } });
  const parsed = bodySchema.safeParse(await request.json().catch(() => null));
  if (!parsed.success) return Response.json({ error: 'validation' }, { status: 400 });
  const { locale } = parsed.data;
  const user = await getCurrentUser();
  const now = new Date();
  const [stable, context] = await Promise.all([stableInstructions(), requestContext(locale, user, now)]);
  // Stable instructions first and cached; the per-request context (time, guest) after the breakpoint.
  const system: Anthropic.Beta.BetaTextBlockParam[] = [
    { type: 'text', text: stable, cache_control: { type: 'ephemeral' } },
    { type: 'text', text: context },
  ];
  const messages: Anthropic.Beta.BetaMessageParam[] = parsed.data.messages.map((m) => ({ role: m.role, content: m.content }));
  const encoder = new TextEncoder();

  const stream = new ReadableStream<Uint8Array>({
    async start(controller) {
      const send = (event: string, data: unknown) => controller.enqueue(encoder.encode(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`));
      let jsonRetries = 0;
      try {
        for (let round = 0; round < MAX_ROUNDS; round++) {
          const turn = anthropic().beta.messages.stream(
            {
              model: CONCIERGE_MODEL,
              max_tokens: 16000,
              system,
              tools: CONCIERGE_TOOLS,
              messages,
              thinking: { type: 'adaptive' },
              output_config: { effort: 'medium' },
              // If a safety classifier declines, the API retries the turn on a fallback model it chooses.
              betas: ['server-side-fallback-2026-07-01'],
              fallbacks: 'default',
            },
            { signal: request.signal },
          );
          let wrote = false;
          turn.on('text', (delta) => {
            wrote = true;
            send('text', { delta });
          });

          let message: Anthropic.Beta.BetaMessage;
          try {
            message = await turn.finalMessage();
            jsonRetries = 0;
          } catch (err) {
            // Only an unparseable streamed tool input is retried; API errors go to the outer handler.
            if (err instanceof Anthropic.APIError || request.signal.aborted || jsonRetries++ >= 2) throw err;
            continue;
          }

          if (message.stop_reason === 'refusal') {
            send('notice', { kind: 'refusal' });
            break;
          }
          if (message.stop_reason === 'pause_turn') {
            messages.push({ role: 'assistant', content: message.content });
            continue;
          }
          const calls = message.content.filter((b): b is Anthropic.Beta.BetaToolUseBlock => b.type === 'tool_use');
          if (!calls.length) break;
          // A tool input cut off at max_tokens can still parse; never run it.
          if (message.stop_reason === 'max_tokens') {
            send('notice', { kind: 'truncated' });
            break;
          }
          messages.push({ role: 'assistant', content: message.content });
          const results: Anthropic.Beta.BetaToolResultBlockParam[] = [];
          for (const call of calls) {
            send('tool', { name: call.name });
            const outcome = await runConciergeTool(call.name, call.input, { locale, user, now: new Date() });
            if (outcome.booking) send('booking', outcome.booking);
            results.push({ type: 'tool_result', tool_use_id: call.id, content: outcome.content, ...(outcome.isError ? { is_error: true } : {}) });
          }
          // Every result for this turn goes back in one message, so parallel calls stay parallel.
          messages.push({ role: 'user', content: results });
          if (wrote) send('text', { delta: '\n\n' });
        }
        send('done', {});
      } catch (err) {
        if (!request.signal.aborted) {
          const kind = err instanceof Anthropic.RateLimitError ? 'busy' : err instanceof Anthropic.APIError && err.status >= 500 ? 'busy' : 'error';
          send('notice', { kind });
        }
      } finally {
        try {
          controller.close();
        } catch {
          // Already closed by the client.
        }
      }
    },
  });

  return new Response(stream, {
    headers: { 'content-type': 'text/event-stream; charset=utf-8', 'cache-control': 'no-cache, no-transform', connection: 'keep-alive', 'x-accel-buffering': 'no' },
  });
}
