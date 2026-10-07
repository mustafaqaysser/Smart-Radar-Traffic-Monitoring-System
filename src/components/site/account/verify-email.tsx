'use client';

import { useTranslations } from 'next-intl';
import { useId, useState, useTransition } from 'react';
import { Icon } from '@/components/brand/icon';
import { Button } from '@/components/site/ui/button';
import { describedBy, Field, Input } from '@/components/site/ui/field';
import { toast } from '@/components/site/ui/toast';
import { useRouter } from '@/i18n/navigation';
import { authErrorKey } from '@/lib/account/next';
import { authClient } from '@/lib/auth/client';
import { normalizeDigits } from '@/lib/i18n/digits';

/** Confirms the account's email with a code, which also unlocks history made with that email as a guest. */
export function VerifyEmail({ email }: { email: string }) {
  const t = useTranslations('account.overview.verify');
  const ta = useTranslations('account.auth.errors');
  const router = useRouter();
  const id = useId();
  const [sent, setSent] = useState(false);
  const [done, setDone] = useState(false);
  const [code, setCode] = useState('');
  const [message, setMessage] = useState<string | null>(null);
  const [pending, start] = useTransition();

  if (done) {
    return (
      <p role="status" className="t-body flex items-center gap-3 border-t border-ink pt-4">
        <Icon name="check" size={20} />
        {t('done')}
      </p>
    );
  }

  return (
    <section aria-labelledby={`${id}-title`} className="flex flex-col gap-4 bg-raised p-6 md:p-8">
      <h2 id={`${id}-title`} className="t-heading-md flex items-center gap-3">
        <Icon name="mail" size={22} />
        {t('title')}
      </h2>
      <p className="t-body measure">{t('body', { email })}</p>
      {!sent ? (
        <Button
          variant="secondary"
          icon={null}
          disabled={pending}
          className="self-start"
          onClick={() =>
            start(async () => {
              setMessage(null);
              const res = await authClient.emailOtp.sendVerificationOtp({ email, type: 'email-verification' });
              if (res.error) setMessage(ta(authErrorKey(res.error)));
              else setSent(true);
            })
          }
        >
          {pending ? t('sending') : t('send')}
        </Button>
      ) : (
        <form
          noValidate
          className="flex flex-wrap items-end gap-4"
          onSubmit={(e) => {
            e.preventDefault();
            const otp = normalizeDigits(code).replace(/\D/g, '');
            if (otp.length !== 6) {
              setMessage(ta('invalidOtp'));
              return;
            }
            start(async () => {
              setMessage(null);
              const res = await authClient.emailOtp.verifyEmail({ email, otp });
              if (res.error) setMessage(ta(authErrorKey(res.error)));
              else {
                setDone(true);
                toast(t('done'));
                router.refresh();
              }
            });
          }}
        >
          <p className="t-small w-full text-muted" role="status">
            {t('sent', { email })}
          </p>
          <Field id={`${id}-code`} label={t('code')}>
            <Input id={`${id}-code`} inputMode="numeric" autoComplete="one-time-code" dir="ltr" maxLength={12} value={code} onChange={(e) => setCode(e.target.value)} className="max-w-48 text-center tracking-[0.3em] tabular" {...describedBy(`${id}-code`, { error: Boolean(message) })} />
          </Field>
          <Button type="submit" icon={null} disabled={pending}>
            {pending ? t('confirming') : t('confirm')}
          </Button>
        </form>
      )}
      {message ? (
        <p role="alert" className="t-small text-danger">
          {message}
        </p>
      ) : null}
    </section>
  );
}
