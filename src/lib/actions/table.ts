'use server';

import { z } from 'zod';
import { routing } from '@/i18n/routing';
import { getCurrentUser } from '@/lib/auth/session';
import type { QuoteView } from '@/lib/order/types';
import { quoteOrder, type QuoteInput } from '@/lib/server/order-quote';
import { placeOrder, trackingPath } from '@/lib/server/orders';
import type { StartedPayment } from '@/lib/server/payments';
import { toQuoteView } from '@/lib/server/quote-view';
import { featureEnabled } from '@/lib/server/settings';
import { requestFromTable, tableActivity, tableByCode, type TableActivity } from '@/lib/server/table';
import { limitByIp } from '@/lib/services/rate-limit';
import { guardPublic } from './guard';
import { fail, ok, zodFieldErrors, type ActionResult } from './result';

const code = z.string().trim().min(4).max(32);
const locale = z.enum(routing.locales);
const lineSchema = z.object({
  slug: z.string().min(1).max(80).regex(/^[a-z0-9-]+$/),
  qty: z.number().int().min(1).max(20),
  optionIds: z.array(z.string().min(1).max(40)).max(20),
  note: z.string().max(140),
});
const tipSchema = z
  .union([z.object({ kind: z.literal('percent'), value: z.number().min(0).max(0.3) }), z.object({ kind: z.literal('amount'), value: z.number().int().min(0).max(100_000) })])
  .nullable()
  .optional();

/** Call the waiter or ask for the bill from the table. */
export async function callFromTable(input: { code: string; kind: 'waiter' | 'bill'; note?: string }): Promise<ActionResult<{ repeated: boolean; activity: TableActivity }>> {
  if (!(await featureEnabled('dineInQr'))) return fail('disabled');
  const rl = await limitByIp('table-call', 20, 600);
  if (!rl.ok) return fail('rateLimited', undefined, rl.retryAfterSeconds);
  const parsed = z.object({ code, kind: z.enum(['waiter', 'bill']), note: z.string().trim().max(140, 'tooLong').optional() }).safeParse(input);
  if (!parsed.success) return fail('validation', zodFieldErrors(parsed.error));
  const found = await tableByCode(parsed.data.code);
  if (!found) return fail('notFound');
  const res = await requestFromTable(found.table, parsed.data.kind, parsed.data.note || null);
  return ok({ repeated: res.repeated, activity: await tableActivity(found.table) });
}

const quoteSchema = z.object({ code, lines: z.array(lineSchema).min(1).max(40), tip: tipSchema, locale });

async function tableQuoteInput(d: z.infer<typeof quoteSchema>): Promise<(QuoteInput & { tableId: string; tableLabel: string }) | null> {
  const found = await tableByCode(d.code);
  if (!found) return null;
  const user = await getCurrentUser();
  return {
    branch: found.branch,
    channel: 'dine_in',
    lines: d.lines,
    when: { asap: true },
    tip: d.tip ?? null,
    user: user ? { id: user.id, email: user.email } : null,
    email: user?.email ?? null,
    now: new Date(),
    locale: d.locale,
    tableId: found.table.id,
    tableLabel: found.table.label,
  };
}

/** Live total for the table's basket: service charge, VAT, tip, and whether the kitchen can take it now. */
export async function quoteTableOrder(input: z.input<typeof quoteSchema>): Promise<ActionResult<QuoteView>> {
  const rl = await limitByIp('table-quote', 200, 600);
  if (!rl.ok) return fail('rateLimited', undefined, rl.retryAfterSeconds);
  if (!(await featureEnabled('dineInQr'))) return fail('disabled');
  const parsed = quoteSchema.safeParse(input);
  if (!parsed.success) return fail('validation');
  const q = await tableQuoteInput(parsed.data);
  if (!q) return fail('notFound');
  return ok(toQuoteView(await quoteOrder(q), parsed.data.locale));
}

const placeSchema = quoteSchema.extend({
  name: z.string().trim().max(80, 'tooLong').optional().or(z.literal('')),
  notes: z.string().trim().max(300, 'tooLong').optional().or(z.literal('')),
  paymentMethod: z.enum(['pay_at_venue', 'card']),
  company_website: z.string().optional(),
  rendered_at: z.union([z.string(), z.number()]).optional(),
});

/** Sends the table's basket to the kitchen: paid now by card, or settled at the table with the bill. */
export async function placeTableOrder(input: z.input<typeof placeSchema>): Promise<ActionResult<{ number: string; trackPath: string; payment: StartedPayment | null; activity: TableActivity }>> {
  if (!(await featureEnabled('dineInQr'))) return fail('disabled');
  const blocked = await guardPublic('table-order', input as Record<string, unknown>, 12, 600);
  if (blocked) return blocked;
  const parsed = placeSchema.safeParse(input);
  if (!parsed.success) return fail('validation', zodFieldErrors(parsed.error));
  const d = parsed.data;
  const q = await tableQuoteInput(d);
  if (!q) return fail('notFound');
  const user = await getCurrentUser();
  const table = d.locale === 'ar' ? `الطاولة ${q.tableLabel}` : `Table ${q.tableLabel}`;
  const result = await placeOrder({
    ...q,
    name: d.name || user?.name || table,
    email: user?.email ?? '',
    phone: user?.phone ?? '',
    address: null,
    notes: [d.name ? table : null, d.notes || null].filter(Boolean).join(' · ') || null,
    paymentMethod: d.paymentMethod,
    tableId: q.tableId,
  });
  if (!result.ok) return fail(result.error === 'quote' ? 'quote' : result.error);
  const found = await tableByCode(d.code);
  return ok({ number: result.order.number, trackPath: trackingPath(result.order), payment: result.payment, activity: found ? await tableActivity(found.table) : { requests: [], orders: [] } });
}
