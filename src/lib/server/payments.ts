import 'server-only';
import { and, desc, eq, ne } from 'drizzle-orm';
import restaurantConfig from '@config';
import { db } from '@/lib/db/client';
import { payments } from '@/lib/db/schema';
import { getPaymentProvider, getProviderById } from '@/lib/services/payments/registry';
import type { PaymentPurpose } from '@/lib/services/payments/types';
import { absoluteUrl } from '@/lib/site/url';
import { createId } from '@/lib/utils/id';

export interface StartedPayment {
  paymentId: string;
  clientKind: 'stripe' | 'simulated';
  clientSecret: string | null;
  amount: number;
  /** Absolute URL the visitor returns to after paying (Stripe redirects; the simulated form navigates). */
  returnUrl: string;
}

/** Creates a payment row and the provider-side payment for a deposit, order, gift card or ticket. */
export async function startPayment(input: {
  purpose: PaymentPurpose;
  referenceId: string;
  amount: number;
  description: string;
  email: string;
  locale: string;
  /** Path (with locale) of the confirmation page, e.g. /ar/order/track/ZL-123?token=… */
  returnPath: string;
}): Promise<StartedPayment> {
  const provider = getPaymentProvider();
  const paymentId = createId();
  const returnUrl = absoluteUrl(`${input.returnPath}${input.returnPath.includes('?') ? '&' : '?'}payment=${paymentId}`);
  await db.insert(payments).values({
    id: paymentId,
    provider: provider.id,
    purpose: input.purpose,
    referenceId: input.referenceId,
    amount: input.amount,
    currency: restaurantConfig.currency,
    status: 'requires_action',
  });
  const created = await provider.create({
    paymentId,
    amount: input.amount,
    currency: restaurantConfig.currency,
    purpose: input.purpose,
    referenceId: input.referenceId,
    description: input.description,
    customerEmail: input.email,
    locale: input.locale,
    returnUrl,
  });
  await db.update(payments).set({ providerRef: created.providerRef, clientSecret: created.clientSecret, updatedAt: new Date() }).where(eq(payments.id, paymentId));
  if (created.status === 'succeeded') await markPaymentSucceeded(paymentId);
  return { paymentId, clientKind: provider.clientKind, clientSecret: created.clientSecret, amount: input.amount, returnUrl };
}

/**
 * Marks a payment as succeeded and fulfils what it paid for. Idempotent: only the first call that flips the
 * status runs the fulfilment (webhooks, return pages and the simulated form may all report the same payment).
 */
export async function markPaymentSucceeded(paymentId: string): Promise<boolean> {
  const updated = await db
    .update(payments)
    .set({ status: 'succeeded', failureReason: null, updatedAt: new Date() })
    .where(and(eq(payments.id, paymentId), ne(payments.status, 'succeeded'), ne(payments.status, 'refunded')))
    .returning();
  const payment = updated[0];
  if (!payment) return false;
  const { fulfilPayment } = await import('./fulfilment');
  await fulfilPayment(payment);
  return true;
}

export async function markPaymentFailed(paymentId: string, reason: string): Promise<void> {
  await db
    .update(payments)
    .set({ status: 'failed', failureReason: reason.slice(0, 200), updatedAt: new Date() })
    .where(and(eq(payments.id, paymentId), ne(payments.status, 'succeeded')));
}

/** Asks the provider for the latest status (used on return pages in case the webhook is late). */
export async function syncPayment(paymentId: string): Promise<typeof payments.$inferSelect | null> {
  const payment = await db.query.payments.findFirst({ where: eq(payments.id, paymentId) });
  if (!payment) return null;
  if (payment.status === 'succeeded' || payment.status === 'refunded' || !payment.providerRef) return payment;
  const provider = getProviderById(payment.provider);
  if (!provider || provider.clientKind === 'simulated') return payment;
  const result = await provider.retrieve(payment.providerRef);
  if (result.status === 'succeeded') await markPaymentSucceeded(payment.id);
  else if (result.status === 'failed') await markPaymentFailed(payment.id, result.failureReason ?? 'failed');
  else if (result.status !== payment.status) await db.update(payments).set({ status: result.status, updatedAt: new Date() }).where(eq(payments.id, payment.id));
  return (await db.query.payments.findFirst({ where: eq(payments.id, paymentId) })) ?? null;
}

/** Refunds a payment in full or in part through its provider and records it. */
export async function refundPayment(paymentId: string, amount?: number): Promise<void> {
  const payment = await db.query.payments.findFirst({ where: eq(payments.id, paymentId) });
  if (!payment || payment.status !== 'succeeded') return;
  const provider = getProviderById(payment.provider);
  if (provider && payment.providerRef) await provider.refund(payment.providerRef, amount);
  if (!amount || amount >= payment.amount) await db.update(payments).set({ status: 'refunded', updatedAt: new Date() }).where(eq(payments.id, paymentId));
}

/**
 * The payment a guest can still complete for something (e.g. a deposit after closing the tab): the latest
 * open payment, or a fresh one when the last attempt failed. Null when it has already been paid.
 */
export async function resumablePayment(input: Omit<Parameters<typeof startPayment>[0], 'amount'> & { amount: number }): Promise<StartedPayment | null> {
  const latest = await db.query.payments.findFirst({
    where: and(eq(payments.purpose, input.purpose), eq(payments.referenceId, input.referenceId)),
    orderBy: [desc(payments.createdAt)],
  });
  if (latest?.status === 'succeeded' || latest?.status === 'refunded') return null;
  if (latest && (latest.status === 'requires_action' || latest.status === 'processing')) {
    const provider = getProviderById(latest.provider);
    if (provider && (provider.clientKind === 'simulated' || latest.clientSecret)) {
      const returnUrl = absoluteUrl(`${input.returnPath}${input.returnPath.includes('?') ? '&' : '?'}payment=${latest.id}`);
      return { paymentId: latest.id, clientKind: provider.clientKind, clientSecret: latest.clientSecret, amount: latest.amount, returnUrl };
    }
  }
  return startPayment(input);
}

/** A payment row, only if it was made for the given reference (return pages never act on someone else's payment). */
export async function paymentFor(paymentId: string, referenceId: string) {
  const payment = await db.query.payments.findFirst({ where: and(eq(payments.id, paymentId), eq(payments.referenceId, referenceId)) });
  return payment ?? null;
}
