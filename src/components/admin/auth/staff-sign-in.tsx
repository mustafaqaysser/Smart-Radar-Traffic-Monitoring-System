'use client';

import { useTranslations } from 'next-intl';
import { useRouter } from 'next/navigation';
import { useId, useState, useTransition } from 'react';
import { authErrorKey } from '@/lib/account/next';
import { authClient } from '@/lib/auth/client';
import { normalizeDigits } from '@/lib/i18n/digits';
import { completeStaffSignIn } from '@/lib/actions/admin/shell';
import type { Role } from '@/lib/db/schema';
import { Button } from '../ui/button';
import { Field } from '../ui/field';
import { Input } from '../ui/input';

type Mode = 'password' | 'code';

/**
 * Staff sign-in with a password or a one-time code by email. Customer accounts are signed out again with a
 * clear message: the back-office is for staff only, enforced on the server as well.
 */
export function StaffSignIn({ demoAccounts, demoPassword }: { demoAccounts: { role: Role; email: string }[]; demoPassword: string | null }) {
  const t = useTranslations('admin.auth');
  const tr = useTranslations('admin.shell.roles');
  const router = useRouter();
  const id = useId();
  const [mode, setMode] = useState<Mode>('password');
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [code, setCode] = useState('');
  const [codeSent, setCodeSent] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [pending, start] = useTransition();

  const authError = (e: { code?: string | null; status?: number | null } | null | undefined) => {
    const key = authErrorKey(e);
    return t.has(`errors.${key}`) ? t(`errors.${key}`) : t('errors.unknown');
  };

  const finish = async () => {
    const result = await completeStaffSignIn();
    if (result.ok) {
      router.replace(result.data.href);
      router.refresh();
      return;
    }
    await authClient.signOut();
    setError(result.error === 'notStaff' ? t('errors.notStaff') : t('errors.unknown'));
  };

  const submitPassword = () =>
    start(async () => {
      setError(null);
      const res = await authClient.signIn.email({ email: email.trim(), password, rememberMe: true });
      if (res.error) return setError(authError(res.error));
      await finish();
    });

  const sendCode = () =>
    start(async () => {
      setError(null);
      const res = await authClient.emailOtp.sendVerificationOtp({ email: email.trim(), type: 'sign-in' });
      if (res.error) return setError(authError(res.error));
      setCodeSent(true);
    });

  const submitCode = () =>
    start(async () => {
      setError(null);
      const res = await authClient.signIn.emailOtp({ email: email.trim(), otp: normalizeDigits(code.trim()) });
      if (res.error) return setError(authError(res.error));
      await finish();
    });

  return (
    <div className="mt-6">
      <div role="radiogroup" aria-label={t('signIn.method')} className="mb-5 grid grid-cols-2 rounded-hair border border-line p-0.5">
        {(['password', 'code'] as const).map((m) => (
          <button
            key={m}
            type="button"
            role="radio"
            aria-checked={mode === m}
            onClick={() => {
              setMode(m);
              setError(null);
            }}
            className="h-8 rounded-[1px] text-[0.8125rem] text-muted aria-checked:bg-ink aria-checked:text-bg pointer-coarse:h-10"
          >
            {t(`signIn.modes.${m}`)}
          </button>
        ))}
      </div>

      <form
        className="flex flex-col gap-4"
        onSubmit={(e) => {
          e.preventDefault();
          if (mode === 'password') submitPassword();
          else if (!codeSent) sendCode();
          else submitCode();
        }}
      >
        <Field id={`${id}-email`} label={t('signIn.email')}>
          {(a11y) => <Input {...a11y} type="email" autoComplete="username" required value={email} onChange={(e) => setEmail(e.target.value)} readOnly={mode === 'code' && codeSent} />}
        </Field>
        {mode === 'password' ? (
          <Field id={`${id}-password`} label={t('signIn.password')}>
            {(a11y) => <Input {...a11y} type="password" autoComplete="current-password" required minLength={8} value={password} onChange={(e) => setPassword(e.target.value)} />}
          </Field>
        ) : codeSent ? (
          <Field id={`${id}-code`} label={t('signIn.code')} hint={t('signIn.codeSent', { email: email.trim() })}>
            {(a11y) => <Input {...a11y} inputMode="numeric" autoComplete="one-time-code" required pattern="[0-9٠-٩]{6}" maxLength={6} value={code} onChange={(e) => setCode(e.target.value)} className="tracking-[0.3em] tabular" dir="ltr" />}
          </Field>
        ) : null}

        {error ? (
          <p role="alert" className="border-s-2 border-danger ps-3 text-[0.8125rem] text-danger">
            {error}
          </p>
        ) : null}

        <Button type="submit" variant="primary" size="lg" disabled={pending} aria-busy={pending}>
          {pending ? t('signIn.working') : mode === 'password' ? t('signIn.submit') : codeSent ? t('signIn.verify') : t('signIn.sendCode')}
        </Button>
        {mode === 'code' && codeSent ? (
          <Button variant="link" size="sm" className="self-start" onClick={() => sendCode()} disabled={pending}>
            {t('signIn.resend')}
          </Button>
        ) : null}
      </form>

      {demoAccounts.length && demoPassword ? (
        <details className="mt-8 rounded-soft border border-line bg-raised">
          <summary className="cursor-pointer px-4 py-3 text-[0.8125rem] font-medium">{t('demo.title')}</summary>
          <div className="border-t border-line px-4 py-3">
            <p className="mb-3 text-xs text-muted">
              {t('demo.body')}{' '}
              <code dir="ltr" className="rounded-hair bg-surface px-1 font-mono">
                {demoPassword}
              </code>
            </p>
            <ul className="grid grid-cols-2 gap-1.5">
              {demoAccounts.map((a) => (
                <li key={a.email}>
                  <button
                    type="button"
                    onClick={() => {
                      setMode('password');
                      setEmail(a.email);
                      setPassword(demoPassword);
                      setError(null);
                    }}
                    className="w-full rounded-hair border border-line px-2.5 py-1.5 text-start text-[0.8125rem] hover-capable:hover:border-field hover-capable:hover:bg-surface"
                  >
                    <span className="block font-medium">{tr(a.role)}</span>
                    <span className="block truncate text-[0.6875rem] text-muted" dir="ltr">
                      {a.email}
                    </span>
                  </button>
                </li>
              ))}
            </ul>
          </div>
        </details>
      ) : null}
    </div>
  );
}
