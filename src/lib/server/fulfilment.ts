import 'server-only';
import type { payments } from '@/lib/db/schema';

type Payment = typeof payments.$inferSelect;

/** Runs once per succeeded payment (see markPaymentSucceeded) and completes whatever it paid for. */
export async function fulfilPayment(payment: Payment): Promise<void> {
  switch (payment.purpose) {
    case 'deposit': {
      const { confirmDepositPaid } = await import('./reservations');
      await confirmDepositPaid(payment.referenceId, payment.id);
      return;
    }
    default:
      throw new Error(`No fulfilment registered for ${payment.purpose} payments.`);
  }
}
