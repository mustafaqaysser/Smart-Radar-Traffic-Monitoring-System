'use client';

import { useRouter } from 'next/navigation';
import { PaymentForm } from '@/components/site/payment-form';
import type { StartedPayment } from '@/lib/server/payments';

/** Completes a payment from its return page (a deposit or an order) after a closed tab or a declined card. */
export function PaymentResume({ payment }: { payment: StartedPayment }) {
  const router = useRouter();
  return (
    <PaymentForm
      {...payment}
      onSucceeded={() => {
        const target = new URL(payment.returnUrl);
        router.replace(`${target.pathname}${target.search}`);
        router.refresh();
      }}
    />
  );
}
