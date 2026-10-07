'use client';

import { useTranslations } from 'next-intl';
import { useId, useState, useTransition } from 'react';
import { Button } from '@/components/site/ui/button';
import { Checkbox, describedBy, Field, Input, Select } from '@/components/site/ui/field';
import { toast } from '@/components/site/ui/toast';
import { useRouter } from '@/i18n/navigation';
import { authErrorKey } from '@/lib/account/next';
import { addPassword, updateProfile } from '@/lib/actions/account';
import { authClient } from '@/lib/auth/client';

export function ProfileForm({ initial }: { initial: { name: string; phone: string; locale: string } }) {
  const t = useTranslations('account.profile');
  const tf = useTranslations('forms');
  const tc = useTranslations('common');
  const router = useRouter();
  const id = useId();
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [pending, start] = useTransition();
  const err = (k: string) => (errors[k] ? tf(`errors.${errors[k]}`) : undefined);
  return (
    <form
      noValidate
      className="grid gap-6 sm:grid-cols-2"
      onSubmit={(e) => {
        e.preventDefault();
        const data = new FormData(e.currentTarget);
        start(async () => {
          const res = await updateProfile({ name: String(data.get('name') ?? ''), phone: String(data.get('phone') ?? ''), locale: String(data.get('locale') ?? 'ar') as 'ar' | 'en' });
          if (res.ok) {
            setErrors({});
            toast(t('saved'));
            router.refresh();
          } else {
            setErrors(res.fieldErrors ?? {});
            if (!res.fieldErrors) toast(tf.has(`errors.${res.error}`) ? tf(`errors.${res.error}`) : tf('errors.unknown'));
          }
        });
      }}
    >
      <Field id={`${id}-name`} label={t('name')} required requiredLabel={tc('a11y.required')} error={err('name')}>
        <Input id={`${id}-name`} name="name" autoComplete="name" required maxLength={80} defaultValue={initial.name === '—' ? '' : initial.name} {...describedBy(`${id}-name`, { error: Boolean(err('name')) })} />
      </Field>
      <Field id={`${id}-phone`} label={`${t('phone')} (${tf('fields.optional')})`} hint={tf('fields.phoneHint')} error={err('phone')}>
        <Input id={`${id}-phone`} name="phone" type="tel" autoComplete="tel" inputMode="tel" defaultValue={initial.phone} {...describedBy(`${id}-phone`, { hint: true, error: Boolean(err('phone')) })} />
      </Field>
      <Field id={`${id}-locale`} label={t('language')}>
        <Select id={`${id}-locale`} name="locale" defaultValue={initial.locale}>
          <option value="ar" lang="ar">
            العربية
          </option>
          <option value="en" lang="en">
            English
          </option>
        </Select>
      </Field>
      <div className="flex items-end sm:col-span-2">
        <Button type="submit" icon={null} disabled={pending}>
          {pending ? t('saving') : t('save')}
        </Button>
      </div>
    </form>
  );
}

export function PasswordPanel({ hasPassword }: { hasPassword: boolean }) {
  const t = useTranslations('account.profile.password');
  const ta = useTranslations('account.auth.errors');
  const tf = useTranslations('forms');
  const tc = useTranslations('common');
  const router = useRouter();
  const id = useId();
  const [message, setMessage] = useState<string | null>(null);
  const [formKey, setFormKey] = useState(0);
  const [pending, start] = useTransition();

  return (
    <form
      key={formKey}
      noValidate
      className="grid gap-6 sm:grid-cols-2"
      onSubmit={(e) => {
        e.preventDefault();
        const data = new FormData(e.currentTarget);
        const current = String(data.get('current') ?? '');
        const next = String(data.get('new') ?? '');
        if (next.length < 8) return setMessage(ta('passwordShort'));
        if (next !== String(data.get('confirm') ?? '')) return setMessage(ta('passwordMismatch'));
        setMessage(null);
        start(async () => {
          if (hasPassword) {
            const res = await authClient.changePassword({ currentPassword: current, newPassword: next, revokeOtherSessions: data.get('others') === 'on' });
            if (res.error) return setMessage(ta(authErrorKey(res.error)));
            toast(t('changed'));
          } else {
            const res = await addPassword({ newPassword: next });
            if (!res.ok) return setMessage(tf.has(`errors.${res.error}`) ? tf(`errors.${res.error}`) : ta('unknown'));
            toast(t('setDone'));
            router.refresh();
          }
          setFormKey((k) => k + 1);
        });
      }}
    >
      {!hasPassword ? <p className="t-body text-muted sm:col-span-2">{t('setBody')}</p> : null}
      {hasPassword ? (
        <Field id={`${id}-current`} label={t('current')} required requiredLabel={tc('a11y.required')} className="sm:col-span-2">
          <Input id={`${id}-current`} name="current" type="password" autoComplete="current-password" required className="sm:max-w-80" />
        </Field>
      ) : null}
      <Field id={`${id}-new`} label={t('new')} required requiredLabel={tc('a11y.required')}>
        <Input id={`${id}-new`} name="new" type="password" autoComplete="new-password" required minLength={8} maxLength={128} />
      </Field>
      <Field id={`${id}-confirm`} label={t('confirm')} required requiredLabel={tc('a11y.required')}>
        <Input id={`${id}-confirm`} name="confirm" type="password" autoComplete="new-password" required minLength={8} maxLength={128} {...describedBy(`${id}-confirm`, { error: Boolean(message) })} />
      </Field>
      {hasPassword ? <Checkbox id={`${id}-others`} name="others" label={t('others')} className="sm:col-span-2" /> : null}
      {message ? (
        <p role="alert" className="t-small text-danger sm:col-span-2">
          {message}
        </p>
      ) : null}
      <div className="sm:col-span-2">
        <Button type="submit" variant="secondary" icon={null} disabled={pending}>
          {pending ? t('changing') : hasPassword ? t('change') : t('set')}
        </Button>
      </div>
    </form>
  );
}

export function RevokeSessions() {
  const t = useTranslations('account.profile.sessions');
  const ta = useTranslations('account.auth.errors');
  const [pending, start] = useTransition();
  return (
    <div className="flex flex-col items-start gap-4">
      <p className="t-body text-muted">{t('body')}</p>
      <Button
        variant="secondary"
        icon={null}
        leadingIcon="logout"
        disabled={pending}
        onClick={() =>
          start(async () => {
            const res = await authClient.revokeOtherSessions();
            toast(res.error ? ta(authErrorKey(res.error)) : t('revoked'));
          })
        }
      >
        {pending ? t('revoking') : t('revoke')}
      </Button>
    </div>
  );
}
