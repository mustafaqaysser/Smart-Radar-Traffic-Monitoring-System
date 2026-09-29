import type { CreatePaymentInput, CreatedPayment, PaymentProvider, PaymentStatusResult } from './types';

/** Test cards understood by the simulated provider (mirrors Stripe's conventions). */
export const SIMULATED_CARDS = {
  success: '4242424242424242',
  decline: '4000000000000002',
  insufficientFunds: '4000000000009995',
} as const;

export type SimulatedOutcome = { ok: true } | { ok: false; reason: 'card_declined' | 'insufficient_funds' | 'invalid_card' | 'expired_card' };

/** Luhn check for card numbers. */
export function luhnValid(number: string): boolean {
  const digits = number.replace(/\D/g, '');
  if (digits.length < 12 || digits.length > 19) return false;
  let sum = 0;
  let double = false;
  for (let i = digits.length - 1; i >= 0; i--) {
    let d = Number(digits[i]);
    if (double) {
      d *= 2;
      if (d > 9) d -= 9;
    }
    sum += d;
    double = !double;
  }
  return sum % 10 === 0;
}

/** Decides the outcome of a simulated card payment. Nothing is charged; no card data is stored. */
export function simulateCard(number: string, expiry: string, now = new Date()): SimulatedOutcome {
  const digits = number.replace(/\D/g, '');
  if (!luhnValid(digits)) return { ok: false, reason: 'invalid_card' };
  const match = /^(\d{2})\s*\/\s*(\d{2})$/.exec(expiry.trim());
  if (!match) return { ok: false, reason: 'invalid_card' };
  const month = Number(match[1]);
  const year = 2000 + Number(match[2]);
  if (month < 1 || month > 12) return { ok: false, reason: 'invalid_card' };
  const endOfMonth = new Date(Date.UTC(year, month, 1));
  if (endOfMonth <= now) return { ok: false, reason: 'expired_card' };
  if (digits === SIMULATED_CARDS.decline) return { ok: false, reason: 'card_declined' };
  if (digits === SIMULATED_CARDS.insufficientFunds) return { ok: false, reason: 'insufficient_funds' };
  return { ok: true };
}

/**
 * The built-in provider used when no payment keys are configured. Payments are created as
 * "requires_action"; the checkout's simulated card form confirms them through a server action.
 */
export class SimulatedProvider implements PaymentProvider {
  readonly id = 'simulated';
  readonly clientKind = 'simulated' as const;

  async create(input: CreatePaymentInput): Promise<CreatedPayment> {
    return { providerRef: `sim_${input.paymentId}`, clientSecret: null, status: 'requires_action' };
  }

  async retrieve(): Promise<PaymentStatusResult> {
    // State lives in our payments table; the simulated confirmation updates it directly.
    return { status: 'requires_action' };
  }

  async refund(): Promise<void> {
    // Nothing to return: no money moved.
  }
}
