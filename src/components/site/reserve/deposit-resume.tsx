'use client';

import { useRouter } from 'next/navigation';
import { PaymentForm } from '@/components/site/payment-form';
import type { StartedPayment } from '@/lib/server/payments';

/** Completes a deposit from the confirmation page (after a closed tab or a declined card). */
export function DepositResume({ payment }: { payment: StartedPayment }) {
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
