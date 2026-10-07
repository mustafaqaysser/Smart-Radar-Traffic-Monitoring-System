'use client';

import { useRouter } from 'next/navigation';
import { useLocale, useTranslations } from 'next-intl';
import { useState } from 'react';
import { Icon } from '@/components/brand/icon';
import { PaymentForm } from '@/components/site/payment-form';
import { acceptWaitlistOffer, type BookingOutcome } from '@/lib/actions/reservations';
import { formatMoney } from '@/lib/i18n/format';
import type { GuestPrefill } from '@/lib/reserve/types';
import { DetailsForm } from './details-form';
import { formatCountdown, useCountdown } from './use-countdown';

interface OfferBookingProps {
  entry: string;
  token: string;
  expiresAt: string;
  guest: GuestPrefill;
  deposit: number;
  rules: { minParty: number; perGuest: number; cutoffHours: number };
  newsletter: boolean;
}

/** Confirms a table offered from the waitlist, with the guest's details already filled in. */
export function OfferBooking({ entry, token, expiresAt, guest, deposit, rules, newsletter }: OfferBookingProps) {
  const t = useTranslations('reserve');
  const locale = useLocale();
  const router = useRouter();
  const [expired, setExpired] = useState(false);
  const [payment, setPayment] = useState<Extract<BookingOutcome, { kind: 'payment' }> | null>(null);
  const seconds = useCountdown(payment ? null : expiresAt, () => setExpired(true));

  if (expired) {
    return (
      <p role="alert" className="t-body-lg measure">
        {t('offer.expired')}
      </p>
    );
  }

  if (payment) {
    return (
      <div className="flex flex-col gap-6">
        <p className="t-heading-sm">{t('deposit.title', { amount: formatMoney(payment.payment.amount, locale) })}</p>
        <PaymentForm
          {...payment.payment}
          onSucceeded={() => {
            const target = new URL(payment.payment.returnUrl);
            router.push(`${target.pathname}${target.search}`);
          }}
        />
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-8">
      {seconds !== null ? (
        <p className="t-small flex items-center gap-2" role="timer" aria-live="off">
          <Icon name="clock" size={16} />
          {t('hold.held', { time: formatCountdown(seconds, locale) })}
        </p>
      ) : null}
      <DetailsForm
        guest={guest}
        signedIn={false}
        deposit={deposit}
        rules={rules}
        newsletter={newsletter}
        hidden={{ entry, token, occasion: 'none' }}
        action={acceptWaitlistOffer}
        onOutcome={(outcome) => {
          if (outcome.kind === 'confirmed') router.push(outcome.href);
          else setPayment(outcome);
        }}
        onFlowError={(key) => {
          if (key !== 'holdExpired' && key !== 'unavailable' && key !== 'notFound') return false;
          setExpired(true);
          return true;
        }}
      />
    </div>
  );
}
