'use client';

import { useLocale, useTranslations } from 'next-intl';
import { useId, useState, useTransition } from 'react';
import { Icon } from '@/components/brand/icon';
import { Button } from '@/components/site/ui/button';
import { Checkbox, Chip, describedBy, Field, Input } from '@/components/site/ui/field';
import { Link, useRouter } from '@/i18n/navigation';
import { authErrorKey } from '@/lib/account/next';
import { savePreferences } from '@/lib/actions/account';
import { authClient } from '@/lib/auth/client';
import { normalizeDigits } from '@/lib/i18n/digits';

function useAuthCopy() {
  const t = useTranslations('account.auth');
  const tf = useTranslations('forms');
  const tc = useTranslations('common');
  return { t, tf, tc, error: (e: { code?: string | null; status?: number | null } | null | undefined) => t(`errors.${authErrorKey(e)}`) };
}

function FormAlert({ message }: { message: string | null }) {
  return message ? (
    <p role="alert" className="t-small text-danger">
      {message}
    </p>
  ) : null;
}

/** After signing in: go where the guest was heading, with the server-rendered header and basket refreshed. */
function useSignedIn(next: string) {
  const router = useRouter();
  return () => {
    router.replace(next);
    router.refresh();
  };
}

// ————————————————————————————————————————— sign in —————————————————————————————————————————

export function SignInForm({ next, initialMode = 'password', initialEmail = '' }: { next: string; initialMode?: 'password' | 'code'; initialEmail?: string }) {
  const { t } = useAuthCopy();
  const [mode, setMode] = useState(initialMode);
  const [email, setEmail] = useState(initialEmail);
  return (
    <div className="flex flex-col gap-8">
      <div className="flex flex-wrap gap-2" role="group" aria-label={t('signIn.title')}>
        <Chip pressed={mode === 'password'} onClick={() => setMode('password')}>
          <Icon name="lock" size={16} />
          {t('signIn.withPassword')}
        </Chip>
        <Chip pressed={mode === 'code'} onClick={() => setMode('code')}>
          <Icon name="mail" size={16} />
          {t('signIn.withCode')}
        </Chip>
      </div>
      {mode === 'password' ? <PasswordSignIn next={next} email={email} onEmail={setEmail} /> : <CodeSignIn next={next} email={email} onEmail={setEmail} />}
      <p className="t-small border-t border-line pt-6">
        {t('signIn.noAccount')}{' '}
        <Link href={{ pathname: '/account/sign-up', query: next !== '/account' ? { next } : {} }} className="underline underline-offset-4">
          {t('signIn.create')}
        </Link>
      </p>
    </div>
  );
}

function PasswordSignIn({ next, email, onEmail }: { next: string; email: string; onEmail: (v: string) => void }) {
  const { t, tf, tc, error } = useAuthCopy();
  const id = useId();
  const done = useSignedIn(next);
  const [message, setMessage] = useState<string | null>(null);
  const [pending, start] = useTransition();
  return (
    <form
      noValidate
      className="flex flex-col gap-6"
      onSubmit={(e) => {
        e.preventDefault();
        const data = new FormData(e.currentTarget);
        setMessage(null);
        start(async () => {
          const res = await authClient.signIn.email({ email: String(data.get('email') ?? '').trim(), password: String(data.get('password') ?? ''), rememberMe: true });
          if (res.error) setMessage(error(res.error));
          else done();
        });
      }}
    >
      <Field id={`${id}-email`} label={tf('fields.email')} required requiredLabel={tc('a11y.required')}>
        <Input id={`${id}-email`} name="email" type="email" autoComplete="email" required value={email} onChange={(e) => onEmail(e.target.value)} />
      </Field>
      <Field id={`${id}-password`} label={tf('fields.password')} required requiredLabel={tc('a11y.required')}>
        <Input id={`${id}-password`} name="password" type="password" autoComplete="current-password" required minLength={8} {...describedBy(`${id}-password`, { error: Boolean(message) })} />
      </Field>
      <FormAlert message={message} />
      <div className="flex flex-wrap items-center gap-x-6 gap-y-4">
        <Button type="submit" size="lg" icon="arrow" disabled={pending}>
          {pending ? t('signIn.signingIn') : t('signIn.submit')}
        </Button>
        <Link href={{ pathname: '/account/reset', query: email ? { email } : {} }} className="t-small underline decoration-line underline-offset-4">
          {t('signIn.forgot')}
        </Link>
      </div>
    </form>
  );
}

function CodeSignIn({ next, email, onEmail }: { next: string; email: string; onEmail: (v: string) => void }) {
  const { t, tf, tc, error } = useAuthCopy();
  const id = useId();
  const done = useSignedIn(next);
  const [sentTo, setSentTo] = useState<string | null>(null);
  const [code, setCode] = useState('');
  const [message, setMessage] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [pending, start] = useTransition();

  const send = (address: string, again = false) =>
    start(async () => {
      setMessage(null);
      const res = await authClient.emailOtp.sendVerificationOtp({ email: address, type: 'sign-in' });
      if (res.error) {
        setMessage(error(res.error));
        return;
      }
      setSentTo(address);
      setNotice(again ? t('code.resent') : null);
    });

  if (!sentTo) {
    return (
      <form
        noValidate
        className="flex flex-col gap-6"
        onSubmit={(e) => {
          e.preventDefault();
          const address = email.trim();
          if (!/^\S+@\S+\.\S+$/.test(address)) {
            setMessage(tf('errors.email'));
            return;
          }
          send(address);
        }}
      >
        <p className="t-small text-muted">{t('code.hint')}</p>
        <Field id={`${id}-email`} label={tf('fields.email')} required requiredLabel={tc('a11y.required')}>
          <Input id={`${id}-email`} name="email" type="email" autoComplete="email" required value={email} onChange={(e) => onEmail(e.target.value)} {...describedBy(`${id}-email`, { error: Boolean(message) })} />
        </Field>
        <FormAlert message={message} />
        <Button type="submit" size="lg" icon="arrow" disabled={pending} className="self-start">
          {pending ? t('code.sending') : t('code.send')}
        </Button>
      </form>
    );
  }

  return (
    <form
      noValidate
      className="flex flex-col gap-6"
      onSubmit={(e) => {
        e.preventDefault();
        const otp = normalizeDigits(code).replace(/\D/g, '');
        if (otp.length !== 6) {
          setMessage(t('errors.invalidOtp'));
          return;
        }
        setMessage(null);
        start(async () => {
          const res = await authClient.signIn.emailOtp({ email: sentTo, otp });
          if (res.error) setMessage(error(res.error));
          else done();
        });
      }}
    >
      <p className="t-body" role="status">
        {t('code.sent', { email: sentTo })}
      </p>
      <Field id={`${id}-code`} label={t('code.label')} required requiredLabel={tc('a11y.required')}>
        <Input
          id={`${id}-code`}
          name="otp"
          inputMode="numeric"
          autoComplete="one-time-code"
          maxLength={12}
          required
          value={code}
          onChange={(e) => setCode(e.target.value)}
          dir="ltr"
          className="max-w-56 text-center text-2xl tracking-[0.4em] tabular"
          {...describedBy(`${id}-code`, { error: Boolean(message) })}
        />
      </Field>
      <FormAlert message={message} />
      {notice ? <p className="t-small text-muted">{notice}</p> : null}
      <div className="flex flex-wrap items-center gap-x-6 gap-y-4">
        <Button type="submit" size="lg" icon="arrow" disabled={pending}>
          {pending ? t('signIn.signingIn') : t('code.submit')}
        </Button>
        <button type="button" className="t-small underline decoration-line underline-offset-4" disabled={pending} onClick={() => send(sentTo, true)}>
          {t('code.resend')}
        </button>
        <button
          type="button"
          className="t-small underline decoration-line underline-offset-4"
          onClick={() => {
            setSentTo(null);
            setCode('');
            setMessage(null);
          }}
        >
          {t('code.otherEmail')}
        </button>
      </div>
    </form>
  );
}

// ————————————————————————————————————————— sign up —————————————————————————————————————————

export function SignUpForm({ next, bonusLabel }: { next: string; bonusLabel: string | null }) {
  const { t, tf, tc, error } = useAuthCopy();
  const locale = useLocale();
  const id = useId();
  const done = useSignedIn(next);
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [message, setMessage] = useState<string | null>(null);
  const [pending, start] = useTransition();
  const err = (k: string) => errors[k];

  return (
    <form
      noValidate
      className="grid gap-6 sm:grid-cols-2"
      onSubmit={(e) => {
        e.preventDefault();
        const data = new FormData(e.currentTarget);
        const name = String(data.get('name') ?? '').trim();
        const email = String(data.get('email') ?? '').trim();
        const password = String(data.get('password') ?? '');
        const problems: Record<string, string> = {};
        if (name.length < 2) problems.name = tf('errors.tooShort');
        if (!/^\S+@\S+\.\S+$/.test(email)) problems.email = tf('errors.email');
        if (password.length < 8) problems.password = t('errors.passwordShort');
        if (data.get('consent') !== 'on') problems.consent = tf('errors.consent');
        setErrors(problems);
        setMessage(Object.keys(problems).length ? tf('errors.validation') : null);
        if (Object.keys(problems).length) return;
        const marketing = data.get('marketing') === 'on';
        start(async () => {
          const res = await authClient.signUp.email({ name, email, password, locale });
          if (res.error) {
            setMessage(error(res.error));
            return;
          }
          if (marketing) await savePreferences({ marketingEmail: true, marketingSms: false });
          done();
        });
      }}
    >
      {bonusLabel ? (
        <p className="t-body flex items-center gap-3 sm:col-span-2">
          <Icon name="gift" size={22} className="text-accent" />
          {t('signUp.bonus', { points: bonusLabel })}
        </p>
      ) : null}
      <Field id={`${id}-name`} label={tf('fields.name')} required requiredLabel={tc('a11y.required')} error={err('name')}>
        <Input id={`${id}-name`} name="name" autoComplete="name" required maxLength={80} {...describedBy(`${id}-name`, { error: Boolean(err('name')) })} />
      </Field>
      <Field id={`${id}-email`} label={tf('fields.email')} required requiredLabel={tc('a11y.required')} error={err('email')}>
        <Input id={`${id}-email`} name="email" type="email" autoComplete="email" required {...describedBy(`${id}-email`, { error: Boolean(err('email')) })} />
      </Field>
      <Field id={`${id}-password`} label={tf('fields.password')} hint={t('signUp.passwordHint')} required requiredLabel={tc('a11y.required')} error={err('password')} className="sm:col-span-2">
        <Input id={`${id}-password`} name="password" type="password" autoComplete="new-password" required minLength={8} maxLength={128} className="sm:max-w-80" {...describedBy(`${id}-password`, { hint: true, error: Boolean(err('password')) })} />
      </Field>
      <div className="flex flex-col gap-4 sm:col-span-2">
        <Checkbox id={`${id}-marketing`} name="marketing" label={t('signUp.marketing')} />
        <Checkbox id={`${id}-consent`} name="consent" label={tf('consent.privacy')} required aria-invalid={Boolean(err('consent')) || undefined} />
        {err('consent') ? (
          <p role="alert" className="t-small text-danger">
            {err('consent')}
          </p>
        ) : null}
      </div>
      <div className="flex flex-col gap-4 sm:col-span-2">
        <FormAlert message={message} />
        <Button type="submit" size="lg" icon="arrow" disabled={pending} className="self-start">
          {pending ? t('signUp.creating') : t('signUp.submit')}
        </Button>
        <p className="t-small border-t border-line pt-6">
          {t('signUp.haveAccount')}{' '}
          <Link href={{ pathname: '/account/sign-in', query: next !== '/account' ? { next } : {} }} className="underline underline-offset-4">
            {t('signUp.signIn')}
          </Link>
        </p>
      </div>
    </form>
  );
}

// ————————————————————————————————————————— password reset —————————————————————————————————————————

export function ResetRequestForm({ initialEmail = '' }: { initialEmail?: string }) {
  const { t, tf, tc, error } = useAuthCopy();
  const locale = useLocale();
  const id = useId();
  const [sent, setSent] = useState<string | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [pending, start] = useTransition();

  if (sent) {
    return (
      <div role="status" className="flex flex-col gap-4">
        <p className="t-body-lg flex items-start gap-3">
          <Icon name="mail" size={24} className="mt-1 shrink-0" />
          {t('reset.sent', { email: sent })}
        </p>
        <Link href="/account/sign-in" className="t-small self-start underline underline-offset-4">
          {t('reset.back')}
        </Link>
      </div>
    );
  }
  return (
    <form
      noValidate
      className="flex flex-col gap-6"
      onSubmit={(e) => {
        e.preventDefault();
        const email = String(new FormData(e.currentTarget).get('email') ?? '').trim();
        if (!/^\S+@\S+\.\S+$/.test(email)) {
          setMessage(tf('errors.email'));
          return;
        }
        setMessage(null);
        start(async () => {
          const res = await authClient.requestPasswordReset({ email, redirectTo: `/${locale}/account/reset/new` });
          if (res.error && res.error.status === 429) setMessage(error(res.error));
          else setSent(email);
        });
      }}
    >
      <Field id={`${id}-email`} label={tf('fields.email')} required requiredLabel={tc('a11y.required')}>
        <Input id={`${id}-email`} name="email" type="email" autoComplete="email" required defaultValue={initialEmail} {...describedBy(`${id}-email`, { error: Boolean(message) })} />
      </Field>
      <FormAlert message={message} />
      <div className="flex flex-wrap items-center gap-x-6 gap-y-4">
        <Button type="submit" size="lg" icon="arrow" disabled={pending}>
          {pending ? t('reset.sending') : t('reset.submit')}
        </Button>
        <Link href="/account/sign-in" className="t-small underline decoration-line underline-offset-4">
          {t('reset.back')}
        </Link>
      </div>
    </form>
  );
}

export function ResetNewForm({ token }: { token: string | null }) {
  const { t, tc, error } = useAuthCopy();
  const id = useId();
  const [done, setDone] = useState(false);
  const [invalid, setInvalid] = useState(!token);
  const [message, setMessage] = useState<string | null>(null);
  const [pending, start] = useTransition();

  if (invalid || !token) {
    return (
      <div role="alert" className="flex flex-col gap-4">
        <p className="t-body-lg">{t('resetNew.invalid')}</p>
        <Link href="/account/reset" className="t-small self-start underline underline-offset-4">
          {t('resetNew.again')}
        </Link>
      </div>
    );
  }
  if (done) {
    return (
      <div role="status" className="flex flex-col gap-4">
        <p className="t-body-lg flex items-center gap-3">
          <Icon name="check" size={24} />
          {t('resetNew.done')}
        </p>
        <Link href="/account/sign-in" className="t-small self-start underline underline-offset-4">
          {t('signUp.signIn')}
        </Link>
      </div>
    );
  }
  return (
    <form
      noValidate
      className="flex flex-col gap-6"
      onSubmit={(e) => {
        e.preventDefault();
        const data = new FormData(e.currentTarget);
        const password = String(data.get('password') ?? '');
        if (password.length < 8) return setMessage(t('errors.passwordShort'));
        if (password !== String(data.get('confirm') ?? '')) return setMessage(t('errors.passwordMismatch'));
        setMessage(null);
        start(async () => {
          const res = await authClient.resetPassword({ newPassword: password, token });
          if (!res.error) setDone(true);
          else if (authErrorKey(res.error) === 'invalidToken') setInvalid(true);
          else setMessage(error(res.error));
        });
      }}
    >
      <Field id={`${id}-password`} label={t('resetNew.password')} required requiredLabel={tc('a11y.required')}>
        <Input id={`${id}-password`} name="password" type="password" autoComplete="new-password" required minLength={8} maxLength={128} className="sm:max-w-80" />
      </Field>
      <Field id={`${id}-confirm`} label={t('resetNew.confirm')} required requiredLabel={tc('a11y.required')}>
        <Input id={`${id}-confirm`} name="confirm" type="password" autoComplete="new-password" required minLength={8} maxLength={128} className="sm:max-w-80" {...describedBy(`${id}-confirm`, { error: Boolean(message) })} />
      </Field>
      <FormAlert message={message} />
      <Button type="submit" size="lg" icon="arrow" disabled={pending} className="self-start">
        {pending ? t('resetNew.saving') : t('resetNew.submit')}
      </Button>
    </form>
  );
}
