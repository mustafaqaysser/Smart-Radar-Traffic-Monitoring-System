'use client';

import { useTranslations } from 'next-intl';
import { useId, useState, useTransition } from 'react';
import { Button } from '@/components/site/ui/button';
import { Checkbox } from '@/components/site/ui/field';
import { toast } from '@/components/site/ui/toast';
import { savePreferences } from '@/lib/actions/account';

export function PreferencesForm({ initial }: { initial: { marketingEmail: boolean; marketingSms: boolean } }) {
  const t = useTranslations('account.preferences');
  const tf = useTranslations('forms');
  const id = useId();
  const [email, setEmail] = useState(initial.marketingEmail);
  const [sms, setSms] = useState(initial.marketingSms);
  const [pending, start] = useTransition();
  return (
    <form
      className="flex flex-col gap-6"
      onSubmit={(e) => {
        e.preventDefault();
        start(async () => {
          const res = await savePreferences({ marketingEmail: email, marketingSms: sms });
          if (res.ok) toast(res.data.pendingConfirmation ? t('pending') : t('saved'));
          else toast(tf.has(`errors.${res.error}`) ? tf(`errors.${res.error}`) : tf('errors.unknown'));
        });
      }}
    >
      <fieldset className="flex flex-col gap-4">
        <legend className="sr-only">{t('title')}</legend>
        <Checkbox id={`${id}-email`} checked={email} onChange={(e) => setEmail(e.target.checked)} label={t('email')} />
        <Checkbox id={`${id}-sms`} checked={sms} onChange={(e) => setSms(e.target.checked)} label={t('sms')} />
      </fieldset>
      <p className="t-small measure text-muted">{t('service')}</p>
      <Button type="submit" icon={null} disabled={pending} className="self-start">
        {pending ? t('saving') : t('save')}
      </Button>
    </form>
  );
}
