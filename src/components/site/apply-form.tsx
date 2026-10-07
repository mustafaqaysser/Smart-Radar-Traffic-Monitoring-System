'use client';

import { useLocale, useTranslations } from 'next-intl';
import { useId, useState, useTransition } from 'react';
import { Icon } from '@/components/brand/icon';
import { BotFields } from '@/components/site/forms/bot-fields';
import { Button } from '@/components/site/ui/button';
import { Checkbox, describedBy, Field, Input, Textarea } from '@/components/site/ui/field';
import { applyForJob } from '@/lib/actions/inquiries';

const MAX_BYTES = 5 * 1024 * 1024;

export function ApplyForm({ posting }: { posting: string }) {
  const t = useTranslations('visit.careers');
  const tf = useTranslations('forms');
  const tc = useTranslations('common');
  const locale = useLocale();
  const id = useId();
  const [pending, start] = useTransition();
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [formError, setFormError] = useState<string | null>(null);
  const [sent, setSent] = useState<string | null>(null);
  const err = (k: string) => (errors[k] ? tf(`errors.${errors[k]}`) : undefined);

  if (sent) {
    return (
      <p role="status" className="t-body-lg flex items-start gap-3 border-t border-ink pt-6">
        <Icon name="check" size={24} className="mt-1" />
        {t('done', { email: sent })}
      </p>
    );
  }

  return (
    <form
      noValidate
      encType="multipart/form-data"
      className="grid gap-6"
      onSubmit={(e) => {
        e.preventDefault();
        const form = new FormData(e.currentTarget);
        const cv = form.get('cv');
        if (cv instanceof File && cv.size > MAX_BYTES) {
          setErrors({ cv: 'fileTooLarge' });
          return;
        }
        start(async () => {
          const res = await applyForJob(form);
          if (res.ok) setSent(res.data.email);
          else {
            setErrors(res.fieldErrors ?? {});
            setFormError(tf(`errors.${res.error}`));
          }
        });
      }}
    >
      <BotFields />
      <input type="hidden" name="locale" value={locale} />
      <input type="hidden" name="posting" value={posting} />
      <Field id={`${id}-name`} label={tf('fields.name')} required requiredLabel={tc('a11y.required')} error={err('name')}>
        <Input id={`${id}-name`} name="name" autoComplete="name" required {...describedBy(`${id}-name`, { error: Boolean(err('name')) })} />
      </Field>
      <Field id={`${id}-email`} label={tf('fields.email')} required requiredLabel={tc('a11y.required')} error={err('email')}>
        <Input id={`${id}-email`} name="email" type="email" autoComplete="email" required {...describedBy(`${id}-email`, { error: Boolean(err('email')) })} />
      </Field>
      <Field id={`${id}-phone`} label={tf('fields.phone')} required requiredLabel={tc('a11y.required')} hint={tf('fields.phoneHint')} error={err('phone')}>
        <Input id={`${id}-phone`} name="phone" type="tel" autoComplete="tel" inputMode="tel" required {...describedBy(`${id}-phone`, { hint: true, error: Boolean(err('phone')) })} />
      </Field>
      <Field id={`${id}-cv`} label={t('cv')} required requiredLabel={tc('a11y.required')} hint={t('cvHint')} error={err('cv')}>
        <input
          id={`${id}-cv`}
          name="cv"
          type="file"
          required
          accept=".pdf,.docx,application/pdf,application/vnd.openxmlformats-officedocument.wordprocessingml.document"
          className="block w-full border border-field bg-raised p-3 file:me-4 file:border-0 file:bg-ink file:px-4 file:py-2 file:text-bg"
          {...describedBy(`${id}-cv`, { hint: true, error: Boolean(err('cv')) })}
        />
      </Field>
      <Field id={`${id}-message`} label={`${t('message')} (${tf('fields.optional')})`} error={err('message')}>
        <Textarea id={`${id}-message`} name="message" rows={5} maxLength={3000} />
      </Field>
      <div>
        <Checkbox id={`${id}-consent`} name="consent" label={tf('consent.privacy')} required />
        {err('consent') ? <p role="alert" className="t-small mt-2 text-danger">{err('consent')}</p> : null}
      </div>
      <div className="flex flex-col gap-3">
        {formError ? <p role="alert" className="t-small text-danger">{formError}</p> : null}
        <Button type="submit" size="lg" disabled={pending} className="self-start">
          {pending ? tc('actions.sending') : t('submit')}
        </Button>
      </div>
    </form>
  );
}
