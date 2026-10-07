import { NextResponse, type NextRequest } from 'next/server';
import { eq } from 'drizzle-orm';
import { db } from '@/lib/db/client';
import { payments } from '@/lib/db/schema';
import { markPaymentFailed, markPaymentSucceeded } from '@/lib/server/payments';
import { getProviderById } from '@/lib/services/payments/registry';
import { StripeProvider } from '@/lib/services/payments/stripe-provider';

/** Stripe webhook: verifies the signature, then settles the matching payment (idempotently). */
export async function POST(request: NextRequest) {
  const secret = process.env.STRIPE_WEBHOOK_SECRET;
  const provider = getProviderById('stripe');
  if (!secret || !(provider instanceof StripeProvider)) return new NextResponse('Stripe is not configured', { status: 404 });
  const signature = request.headers.get('stripe-signature');
  if (!signature) return new NextResponse('Missing signature', { status: 400 });
  let event;
  try {
    event = provider.constructWebhookEvent(await request.text(), signature, secret);
  } catch {
    return new NextResponse('Invalid signature', { status: 400 });
  }
  if (event.type === 'payment_intent.succeeded' || event.type === 'payment_intent.payment_failed') {
    const intent = event.data.object;
    const payment = await db.query.payments.findFirst({ where: eq(payments.providerRef, intent.id) });
    if (payment) {
      if (event.type === 'payment_intent.succeeded') await markPaymentSucceeded(payment.id);
      else await markPaymentFailed(payment.id, intent.last_payment_error?.message ?? 'failed');
    }
  }
  return NextResponse.json({ received: true });
}
