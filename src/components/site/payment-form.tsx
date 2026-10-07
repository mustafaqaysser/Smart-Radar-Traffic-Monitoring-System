'use client';

import { useLocale, useTranslations } from 'next-intl';
import { useEffect, useId, useState, useTransition, type FormEvent } from 'react';
import type { Stripe, StripeElements } from '@stripe/stripe-js';
import { Button } from '@/components/site/ui/button';
import { describedBy, Field, Input } from '@/components/site/ui/field';
import { confirmSimulatedPayment } from '@/lib/actions/payments';
import { formatMoney } from '@/lib/i18n/format';
import { normalizeDigits } from '@/lib/i18n/digits';

export interface PaymentFormProps {
  paymentId: string;
  clientKind: 'stripe' | 'simulated';
  clientSecret: string | null;
  amount: number;
  returnUrl: string;
  /** Called after a successful simulated payment (Stripe redirects to returnUrl itself). */
  onSucceeded: () => void;
}

/** The checkout's payment step: Stripe's Payment Element when configured, the simulated card form otherwise. */
export function PaymentForm(props: PaymentFormProps) {
  return props.clientKind === 'stripe' && props.clientSecret ? <StripePayment {...props} clientSecret={props.clientSecret} /> : <SimulatedPayment {...props} />;
}

function formatCardNumber(value: string): string {
  return normalizeDigits(value)
    .replace(/\D/g, '')
    .slice(0, 19)
    .replace(/(\d{4})(?=\d)/g, '$1 ');
}

function formatExpiry(value: string): string {
  const digits = normalizeDigits(value).replace(/\D/g, '').slice(0, 4);
  return digits.length > 2 ? `${digits.slice(0, 2)}/${digits.slice(2)}` : digits;
}

function SimulatedPayment({ paymentId, amount, onSucceeded }: PaymentFormProps) {
  const t = useTranslations('forms.payment');
  const tf = useTranslations('forms.errors');
  const locale = useLocale();
  const id = useId();
  const [pending, start] = useTransition();
  const [number, setNumber] = useState('');
  const [expiry, setExpiry] = useState('');
  const [cvc, setCvc] = useState('');
  const [name, setName] = useState('');
  const [error, setError] = useState<string | null>(null);

  const submit = (e: FormEvent) => {
    e.preventDefault();
    setError(null);
    start(async () => {
      const res = await confirmSimulatedPayment({ paymentId, number, expiry, cvc, name });
      if (res.ok) onSucceeded();
      else setError(tf.has(res.error) ? tf(res.error) : tf('unknown'));
    });
  };

  return (
    <form onSubmit={submit} className="flex flex-col gap-6" noValidate>
      <div className="border-s-2 border-sun bg-surface p-5">
        <p className="t-heading-sm">{t('simulatedTitle')}</p>
        <p className="t-small mt-2 text-muted">{t('simulatedBody')}</p>
        <ul className="t-small mt-3 flex flex-col gap-1">
          <li>
            <bdi dir="ltr" className="tabular font-mono">4242 4242 4242 4242</bdi> — {t('testSuccess')}
          </li>
          <li>
            <bdi dir="ltr" className="tabular font-mono">4000 0000 0000 0002</bdi> — {t('testDecline')}
          </li>
          <li>
            <bdi dir="ltr" className="tabular font-mono">4000 0000 0000 9995</bdi> — {t('testFunds')}
          </li>
        </ul>
      </div>
      <Field id={`${id}-name`} label={t('nameOnCard')} required>
        <Input id={`${id}-name`} autoComplete="cc-name" value={name} onChange={(e) => setName(e.target.value)} required />
      </Field>
      <Field id={`${id}-number`} label={t('cardNumber')} required>
        <Input id={`${id}-number`} inputMode="numeric" autoComplete="cc-number" dir="ltr" className="tabular text-start" value={number} onChange={(e) => setNumber(formatCardNumber(e.target.value))} placeholder="4242 4242 4242 4242" required />
      </Field>
      <div className="grid grid-cols-2 gap-4">
        <Field id={`${id}-exp`} label={t('expiry')} required>
          <Input id={`${id}-exp`} inputMode="numeric" autoComplete="cc-exp" dir="ltr" className="tabular text-start" value={expiry} onChange={(e) => setExpiry(formatExpiry(e.target.value))} placeholder="12/29" required />
        </Field>
        <Field id={`${id}-cvc`} label={t('cvc')} required>
          <Input id={`${id}-cvc`} inputMode="numeric" autoComplete="cc-csc" dir="ltr" className="tabular text-start" value={cvc} onChange={(e) => setCvc(normalizeDigits(e.target.value).replace(/\D/g, '').slice(0, 4))} placeholder="123" required {...describedBy(`${id}-cvc`, { error: Boolean(error) })} />
        </Field>
      </div>
      {error ? (
        <p role="alert" className="t-small text-danger">
          {error}
        </p>
      ) : null}
      <Button type="submit" size="lg" icon={null} leadingIcon="lock" disabled={pending}>
        {pending ? t('processing') : t('pay', { amount: formatMoney(amount, locale) })}
      </Button>
      <p className="t-small text-muted">{t('secure')}</p>
    </form>
  );
}

function StripePayment({ clientSecret, amount, returnUrl }: PaymentFormProps & { clientSecret: string }) {
  const t = useTranslations('forms.payment');
  const locale = useLocale();
  const mountId = useId().replace(/:/g, '');
  const [stripe, setStripe] = useState<Stripe | null>(null);
  const [elements, setElements] = useState<StripeElements | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [pending, setPending] = useState(false);

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      const key = process.env.NEXT_PUBLIC_STRIPE_PUBLISHABLE_KEY;
      if (!key) {
        setError(t('providerError'));
        return;
      }
      const { loadStripe } = await import('@stripe/stripe-js');
      const instance = await loadStripe(key, { locale: locale === 'ar' ? 'ar' : 'en' });
      if (cancelled || !instance) return;
      const styles = getComputedStyle(document.documentElement);
      const els = instance.elements({
        clientSecret,
        appearance: {
          theme: 'flat',
          variables: {
            colorPrimary: styles.getPropertyValue('--c-accent').trim() || '#A3402A',
            colorBackground: styles.getPropertyValue('--c-raised').trim() || '#FAF6EF',
            colorText: styles.getPropertyValue('--c-ink').trim() || '#221D27',
            colorDanger: styles.getPropertyValue('--c-danger').trim() || '#9E2F22',
            borderRadius: '2px',
          },
        },
      });
      els.create('payment', { layout: 'tabs' }).mount(`#${mountId}`);
      setStripe(instance);
      setElements(els);
    })();
    return () => {
      cancelled = true;
    };
  }, [clientSecret, locale, mountId, t]);

  return (
    <form
      className="flex flex-col gap-6"
      onSubmit={async (e) => {
        e.preventDefault();
        if (!stripe || !elements) return;
        setPending(true);
        setError(null);
        const result = await stripe.confirmPayment({ elements, confirmParams: { return_url: returnUrl } });
        if (result.error) setError(result.error.message ?? t('providerError'));
        setPending(false);
      }}
    >
      {!stripe ? <p className="t-small text-muted">{t('loadingProvider')}</p> : null}
      {/* Stripe mounts its iframe here; React never renders children into this element. */}
      <div id={mountId} className="min-h-40" />
      {error ? (
        <p role="alert" className="t-small text-danger">
          {error}
        </p>
      ) : null}
      <Button type="submit" size="lg" icon={null} leadingIcon="lock" disabled={!stripe || pending}>
        {pending ? t('processing') : t('pay', { amount: formatMoney(amount, locale) })}
      </Button>
      <p className="t-small text-muted">{t('secure')}</p>
    </form>
  );
}
