'use server';

import { eq } from 'drizzle-orm';
import { z } from 'zod';
import { db } from '@/lib/db/client';
import { payments } from '@/lib/db/schema';
import { normalizeDigits } from '@/lib/i18n/digits';
import { markPaymentFailed, markPaymentSucceeded } from '@/lib/server/payments';
import { limitByIp } from '@/lib/services/rate-limit';
import { simulateCard } from '@/lib/services/payments/simulated-provider';
import { fail, ok, type ActionResult } from './result';

const cardSchema = z.object({
  paymentId: z.string().min(10).max(40),
  number: z.string().min(12).max(30),
  expiry: z.string().min(4).max(7),
  cvc: z.string().min(3).max(4),
  name: z.string().trim().min(2).max(80),
});

/**
 * Completes a payment with the built-in simulated provider (no keys configured). Nothing is charged and no
 * card data is stored or logged: the number is checked and discarded.
 */
export async function confirmSimulatedPayment(input: z.input<typeof cardSchema>): Promise<ActionResult<{ status: 'succeeded' }>> {
  const rl = await limitByIp('sim-pay', 20, 600);
  if (!rl.ok) return fail('rateLimited', undefined, rl.retryAfterSeconds);
  const parsed = cardSchema.safeParse({ ...input, number: normalizeDigits(input.number ?? ''), expiry: normalizeDigits(input.expiry ?? ''), cvc: normalizeDigits(input.cvc ?? '') });
  if (!parsed.success) return fail('card_invalid');
  const payment = await db.query.payments.findFirst({ where: eq(payments.id, parsed.data.paymentId) });
  if (!payment || payment.provider !== 'simulated') return fail('notFound');
  if (payment.status === 'succeeded') return ok({ status: 'succeeded' });
  if (payment.status !== 'requires_action' && payment.status !== 'failed') return fail('payment_closed');
  if (!/^\d{3,4}$/.test(parsed.data.cvc)) return fail('card_invalid');
  const outcome = simulateCard(parsed.data.number, parsed.data.expiry);
  if (!outcome.ok) {
    await markPaymentFailed(payment.id, outcome.reason);
    return fail(`card_${outcome.reason === 'invalid_card' ? 'invalid' : outcome.reason === 'expired_card' ? 'expired' : outcome.reason === 'insufficient_funds' ? 'funds' : 'declined'}`);
  }
  await markPaymentSucceeded(payment.id);
  return ok({ status: 'succeeded' });
}
