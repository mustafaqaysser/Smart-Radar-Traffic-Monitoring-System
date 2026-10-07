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
    case 'order': {
      const { confirmOrderPaid } = await import('./orders');
      await confirmOrderPaid(payment.referenceId, payment.id);
      return;
    }
    case 'gift_card': {
      const { confirmGiftCardPaid } = await import('./gift-cards');
      await confirmGiftCardPaid(payment.referenceId, payment.id);
      return;
    }
    case 'event': {
      const { confirmEventBookingPaid } = await import('./events');
      await confirmEventBookingPaid(payment.referenceId, payment.id);
      return;
    }
  }
}
