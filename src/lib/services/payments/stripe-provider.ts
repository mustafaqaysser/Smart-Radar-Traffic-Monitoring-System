import Stripe from 'stripe';
import type { CreatePaymentInput, CreatedPayment, PaymentProvider, PaymentStatusResult } from './types';

/** Stripe (test or live) with automatic payment methods: cards, Apple Pay and Google Pay via the Payment Element. */
export class StripeProvider implements PaymentProvider {
  readonly id = 'stripe';
  readonly clientKind = 'stripe' as const;
  private stripe: Stripe;

  constructor(secretKey: string) {
    this.stripe = new Stripe(secretKey);
  }

  async create(input: CreatePaymentInput): Promise<CreatedPayment> {
    const intent = await this.stripe.paymentIntents.create(
      {
        amount: input.amount,
        currency: input.currency.toLowerCase(),
        automatic_payment_methods: { enabled: true },
        description: input.description,
        ...(input.customerEmail ? { receipt_email: input.customerEmail } : {}),
        metadata: { paymentId: input.paymentId, purpose: input.purpose, referenceId: input.referenceId },
      },
      { idempotencyKey: `zill-${input.paymentId}` },
    );
    return { providerRef: intent.id, clientSecret: intent.client_secret, status: intent.status === 'succeeded' ? 'succeeded' : 'requires_action' };
  }

  async retrieve(providerRef: string): Promise<PaymentStatusResult> {
    const intent = await this.stripe.paymentIntents.retrieve(providerRef);
    switch (intent.status) {
      case 'succeeded':
        return { status: 'succeeded' };
      case 'processing':
        return { status: 'processing' };
      case 'canceled':
        return { status: 'cancelled' };
      case 'requires_payment_method':
        return { status: intent.last_payment_error ? 'failed' : 'requires_action', failureReason: intent.last_payment_error?.message ?? null };
      default:
        return { status: 'requires_action' };
    }
  }

  async refund(providerRef: string, amount?: number): Promise<void> {
    await this.stripe.refunds.create({ payment_intent: providerRef, ...(amount ? { amount } : {}) });
  }

  /** Verifies and parses a webhook payload. */
  constructWebhookEvent(payload: string, signature: string, secret: string): Stripe.Event {
    return this.stripe.webhooks.constructEvent(payload, signature, secret);
  }
}
