'use client';

import { useLocale, useTranslations } from 'next-intl';
import { useId, useState, useTransition } from 'react';
import { Icon } from '@/components/brand/icon';
import { Button } from '@/components/site/ui/button';
import { Field, Input } from '@/components/site/ui/field';
import { checkGiftCardBalance, type BalanceView } from '@/lib/actions/commerce';
import { formatDate, formatMoney } from '@/lib/i18n/format';

/** Balance check by card code (rate limited on the server). */
export function BalanceCheck({ initialCode, timeZone }: { initialCode: string; timeZone: string }) {
  const t = useTranslations('gather.giftCards.balance');
  const tf = useTranslations('forms.errors');
  const locale = useLocale();
  const id = useId();
  const [code, setCode] = useState(initialCode);
  const [result, setResult] = useState<BalanceView | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [pending, start] = useTransition();

  return (
    <form
      className="flex flex-col gap-5"
      onSubmit={(e) => {
        e.preventDefault();
        setError(null);
        start(async () => {
          const res = await checkGiftCardBalance(code);
          if (res.ok) setResult(res.data);
          else {
            setResult(null);
            setError(t.has(`errors.${res.error}`) ? t(`errors.${res.error}`) : tf.has(res.error) ? tf(res.error) : tf('unknown'));
          }
        });
      }}
    >
      <Field id={`${id}-code`} label={t('code')}>
        <div className="flex gap-2">
          <Input id={`${id}-code`} value={code} onChange={(e) => setCode(e.target.value)} placeholder="ZILL-XXXX-XXXX-XXXX" dir="ltr" autoComplete="off" className="flex-1 uppercase tabular" required />
          <Button type="submit" variant="secondary" disabled={pending || code.trim().length < 8}>
            {t('check')}
          </Button>
        </div>
      </Field>
      <div aria-live="polite">
        {result ? (
          <div className="flex flex-col gap-1 border-s-2 border-sun ps-4">
            <p className="t-heading-md tabular">
              <bdi>{t('result', { balance: formatMoney(result.balance, locale), initial: formatMoney(result.initial, locale) })}</bdi>
            </p>
            {result.expiresAt ? <p className="t-small text-muted">{t('until', { date: formatDate(new Date(result.expiresAt), locale, timeZone, { day: 'numeric', month: 'long', year: 'numeric' }) })}</p> : null}
            {!result.usable && result.reason ? <p className="t-small text-danger">{t(`errors.${result.reason}`)}</p> : null}
          </div>
        ) : null}
        {error ? (
          <p role="alert" className="t-small flex items-center gap-2 text-danger">
            <Icon name="alert" size={16} />
            {error}
          </p>
        ) : null}
      </div>
    </form>
  );
}
