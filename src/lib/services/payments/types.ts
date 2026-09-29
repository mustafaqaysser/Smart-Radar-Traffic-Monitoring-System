/**
 * Payment provider contract. Adding a regional gateway (e.g. a Saudi acquirer) means adding one adapter file
 * that implements `PaymentProvider` and registering it in ./registry.ts — nothing else changes.
 */

export type PaymentPurpose = 'order' | 'deposit' | 'gift_card' | 'event';

export interface CreatePaymentInput {
  paymentId: string;
  amount: number; // minor units
  currency: string;
  purpose: PaymentPurpose;
  referenceId: string;
  description: string;
  customerEmail: string;
  locale: string;
  returnUrl: string;
}

export interface CreatedPayment {
  /** Provider reference (e.g. a Stripe PaymentIntent id). */
  providerRef: string;
  /** Secret the client needs to complete the payment (Stripe client secret); null for simulated. */
  clientSecret: string | null;
  status: 'requires_action' | 'succeeded' | 'processing';
}

export interface PaymentStatusResult {
  status: 'requires_action' | 'processing' | 'succeeded' | 'failed' | 'cancelled' | 'refunded';
  failureReason?: string | null;
}

export interface PaymentProvider {
  readonly id: string;
  /** What the checkout UI renders: Stripe's Payment Element or the built-in simulated card form. */
  readonly clientKind: 'stripe' | 'simulated';
  create(input: CreatePaymentInput): Promise<CreatedPayment>;
  retrieve(providerRef: string): Promise<PaymentStatusResult>;
  refund(providerRef: string, amount?: number): Promise<void>;
}
