'use server';

import { eq } from 'drizzle-orm';
import { z } from 'zod';
import restaurantConfig from '@config';
import { routing } from '@/i18n/routing';
import { getCurrentUser } from '@/lib/auth/session';
import { db } from '@/lib/db/client';
import { carts } from '@/lib/db/schema';
import { tr } from '@/lib/i18n/localized';
import type { CheckoutLine, QuoteView } from '@/lib/order/types';
import { getBranch } from '@/lib/queries/branches';
import { quoteOrder, type Quote, type QuoteInput } from '@/lib/server/order-quote';
import { orderByNumber, placeOrder, reorderLines, trackingPath } from '@/lib/server/orders';
import type { StartedPayment } from '@/lib/server/payments';
import { featureEnabled } from '@/lib/server/settings';
import { normalizePhone } from '@/lib/services/phone';
import { limitByIp } from '@/lib/services/rate-limit';
import { guardPublic } from './guard';
import { fail, ok, zodFieldErrors, type ActionResult } from './result';

const lineSchema = z.object({
  slug: z.string().min(1).max(80).regex(/^[a-z0-9-]+$/),
  qty: z.number().int().min(1).max(20),
  optionIds: z.array(z.string().min(1).max(40)).max(20),
  note: z.string().max(140),
});

const checkoutSchema = z.object({
  branch: z.string().min(1).max(64),
  channel: z.enum(['delivery', 'pickup']),
  lines: z.array(lineSchema).max(60),
  when: z.discriminatedUnion('asap', [z.object({ asap: z.literal(true) }), z.object({ asap: z.literal(false), at: z.iso.datetime() })]),
  zoneId: z.string().max(40).nullable().optional(),
  location: z.object({ lat: z.number().min(-90).max(90), lng: z.number().min(-180).max(180) }).nullable().optional(),
  promoCode: z.string().max(40).nullable().optional(),
  giftCardCode: z.string().max(40).nullable().optional(),
  loyaltyPoints: z.number().int().min(0).max(10_000_000).optional(),
  tip: z
    .union([z.object({ kind: z.literal('percent'), value: z.number().min(0).max(0.3) }), z.object({ kind: z.literal('amount'), value: z.number().int().min(0).max(100_000) })])
    .nullable()
    .optional(),
  email: z.string().max(254).optional(),
  locale: z.enum(routing.locales),
});

export type CheckoutRequest = z.input<typeof checkoutSchema>;

function toView(q: Quote, locale: string): QuoteView {
  return {
    lines: q.lines.map((l) => ({ key: l.key, slug: l.slug, name: l.name, qty: l.qty, unitPrice: l.unitPrice, lineTotal: l.lineTotal, options: l.options.map((o) => tr(o.name, locale)), note: l.note, problem: l.problem })),
    pricing: q.pricing,
    promo: q.promo,
    giftCard: q.giftCard ? (q.giftCard.ok ? { code: q.giftCard.code, ok: true, balance: q.giftCard.balance, applied: q.giftCard.applied } : q.giftCard) : null,
    loyalty: q.loyalty,
    zone: q.zone ? { id: q.zone.id, name: tr(q.zone.name, locale), kind: q.zone.kind, areas: q.zone.areas.map((a) => tr(a, locale)), radiusKm: q.zone.radiusKm, fee: q.zone.fee, minOrder: q.zone.minOrder, etaMinutes: q.zone.etaMinutes } : null,
    zoneProblem: q.zoneProblem,
    timing: q.timing,
    tipAllowed: q.tipAllowed,
    serviceChargeRate: q.serviceChargeRate,
    taxRate: q.taxRate,
    problems: q.problems,
    canPlace: q.canPlace,
  };
}

async function quoteInput(d: z.infer<typeof checkoutSchema>): Promise<QuoteInput | null> {
  const branch = await getBranch(d.branch);
  if (!branch) return null;
  const user = await getCurrentUser();
  return {
    branch,
    channel: d.channel,
    lines: d.lines,
    when: d.when.asap ? { asap: true } : { asap: false, at: new Date(d.when.at) },
    zoneId: d.zoneId ?? null,
    location: d.location ?? null,
    promoCode: d.promoCode ?? null,
    giftCardCode: d.giftCardCode ?? null,
    loyaltyPoints: d.loyaltyPoints ?? 0,
    tip: d.tip ?? null,
    user: user ? { id: user.id, email: user.email } : null,
    email: d.email ?? null,
    now: new Date(),
    locale: d.locale,
  };
}

/** The live checkout summary: prices, fees, codes, points and the times the kitchen can promise. */
export async function quoteCheckout(input: CheckoutRequest): Promise<ActionResult<QuoteView>> {
  const rl = await limitByIp('order-quote', 300, 600);
  if (!rl.ok) return fail('rateLimited', undefined, rl.retryAfterSeconds);
  if (!(await featureEnabled('ordering'))) return fail('disabled');
  const parsed = checkoutSchema.safeParse(input);
  if (!parsed.success) return fail('validation');
  const q = await quoteInput(parsed.data);
  if (!q) return fail('notFound');
  return ok(toView(await quoteOrder(q), parsed.data.locale));
}

const placeSchema = checkoutSchema.extend({
  name: z.string().trim().min(2, 'tooShort').max(80, 'tooLong'),
  email: z.email('email').max(254),
  phone: z.string().trim().min(6, 'phone').max(32, 'phone'),
  notes: z.string().trim().max(500, 'tooLong').optional().or(z.literal('')),
  paymentMethod: z.enum(['card', 'cash', 'pay_at_venue']),
  address: z
    .object({
      area: z.string().trim().min(2, 'required').max(80, 'tooLong'),
      street: z.string().trim().min(2, 'required').max(160, 'tooLong'),
      building: z.string().trim().max(40, 'tooLong').optional().or(z.literal('')),
      floor: z.string().trim().max(40, 'tooLong').optional().or(z.literal('')),
      notes: z.string().trim().max(240, 'tooLong').optional().or(z.literal('')),
    })
    .nullable(),
  saveAddressAs: z.string().trim().max(40).nullable().optional(),
  company_website: z.string().optional(),
  rendered_at: z.union([z.string(), z.number()]).optional(),
});

export type PlaceRequest = z.input<typeof placeSchema>;

/** Places the order; card orders continue to payment, others go straight to the kitchen. */
export async function placeOrderAction(input: PlaceRequest): Promise<ActionResult<{ href: string; number: string; payment: StartedPayment | null }>> {
  if (!(await featureEnabled('ordering'))) return fail('disabled');
  const blocked = await guardPublic('order', input as Record<string, unknown>, 10, 600);
  if (blocked) return blocked;
  const parsed = placeSchema.safeParse(input);
  if (!parsed.success) return fail('validation', zodFieldErrors(parsed.error));
  const d = parsed.data;
  if (d.channel === 'delivery' && !d.address) return fail('validation', { 'address.street': 'required' });
  const phone = normalizePhone(d.phone, restaurantConfig.country);
  if (!phone) return fail('validation', { phone: 'phone' });
  const base = await quoteInput({ ...d, email: d.email });
  if (!base) return fail('notFound');
  const result = await placeOrder({
    ...base,
    name: d.name,
    email: d.email,
    phone,
    notes: d.notes || null,
    paymentMethod: d.paymentMethod,
    address:
      d.channel === 'delivery' && d.address
        ? { zoneId: d.zoneId ?? null, area: d.address.area, street: d.address.street, building: d.address.building || null, floor: d.address.floor || null, notes: d.address.notes || null, lat: d.location?.lat ?? null, lng: d.location?.lng ?? null }
        : null,
    saveAddressAs: d.saveAddressAs || null,
  });
  if (!result.ok) return fail(result.error === 'quote' ? (result.quote?.problems[0] ?? 'quote') : result.error);
  return ok({ href: `/${d.locale}${trackingPath(result.order)}`, number: result.order.number, payment: result.payment });
}

/** "Order this again": the order's dishes that can still be ordered, for the basket. */
export async function reorderAction(input: { number: string; token: string }): Promise<ActionResult<{ branch: string; channel: 'delivery' | 'pickup'; lines: CheckoutLine[]; skipped: number }>> {
  const rl = await limitByIp('order-reorder', 30, 600);
  if (!rl.ok) return fail('rateLimited', undefined, rl.retryAfterSeconds);
  const parsed = z.object({ number: z.string().max(16), token: z.string().max(64) }).safeParse(input);
  if (!parsed.success) return fail('validation');
  const order = await orderByNumber(parsed.data.number, parsed.data.token);
  if (!order) return fail('notFound');
  const branch = await getBranch(order.branchId);
  if (!branch) return fail('notFound');
  const { lines, skipped } = await reorderLines(order);
  return ok({ branch: branch.slug, channel: order.channel === 'delivery' ? 'delivery' : 'pickup', lines, skipped });
}

const cartSchema = z.object({
  branch: z.string().max(64).nullable(),
  channel: z.enum(['delivery', 'pickup', 'dine_in']),
  lines: z.array(lineSchema.extend({ key: z.string().max(300) })).max(60),
});

/** Keeps a signed-in guest's basket on the server, so it follows them across devices. */
export async function saveCart(input: z.input<typeof cartSchema>): Promise<ActionResult> {
  const user = await getCurrentUser();
  if (!user) return fail('unauthorized');
  const parsed = cartSchema.safeParse(input);
  if (!parsed.success) return fail('validation');
  const branch = parsed.data.branch ? await getBranch(parsed.data.branch) : null;
  const values = { userId: user.id, branchId: branch?.id ?? null, channel: parsed.data.channel, lines: parsed.data.lines, updatedAt: new Date() };
  await db.insert(carts).values(values).onConflictDoUpdate({ target: carts.userId, set: values });
  return ok(undefined);
}

export async function loadCart(): Promise<ActionResult<{ branch: string | null; channel: 'delivery' | 'pickup' | 'dine_in'; lines: (CheckoutLine & { key: string })[]; updatedAt: number } | null>> {
  const user = await getCurrentUser();
  if (!user) return fail('unauthorized');
  const row = await db.query.carts.findFirst({ where: eq(carts.userId, user.id) });
  if (!row) return ok(null);
  const branch = row.branchId ? await getBranch(row.branchId) : null;
  return ok({ branch: branch?.slug ?? null, channel: row.channel ?? 'pickup', lines: row.lines, updatedAt: row.updatedAt.getTime() });
}
