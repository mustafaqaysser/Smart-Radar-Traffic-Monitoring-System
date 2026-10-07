'use client';

import { useLocale, useTranslations } from 'next-intl';
import { useId, useState, useTransition } from 'react';
import { Icon } from '@/components/brand/icon';
import { BotFields } from '@/components/site/forms/bot-fields';
import { Button } from '@/components/site/ui/button';
import { Checkbox, describedBy, Field, Input, Select, Textarea } from '@/components/site/ui/field';
import { Link } from '@/i18n/navigation';
import { sendContactMessage } from '@/lib/actions/inquiries';

const SUBJECTS = ['general', 'reservations', 'events', 'privateDining', 'press', 'feedback', 'other'] as const;

export function ContactForm({ branches, defaultSubject }: { branches: { slug: string; name: string }[]; defaultSubject?: string }) {
  const t = useTranslations('visit.contact');
  const tf = useTranslations('forms');
  const tc = useTranslations('common');
  const locale = useLocale();
  const id = useId();
  const [pending, start] = useTransition();
  const [subject, setSubject] = useState<string>(SUBJECTS.includes(defaultSubject as never) ? (defaultSubject as string) : 'general');
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [formError, setFormError] = useState<string | null>(null);
  const [sent, setSent] = useState<string | null>(null);
  const err = (k: string) => (errors[k] ? tf(`errors.${errors[k]}`) : undefined);

  if (sent) {
    return (
      <div role="status" className="flex flex-col gap-3 border-t border-ink pt-6">
        <p className="t-heading-md flex items-center gap-3">
          <Icon name="check" size={24} />
          {tf('success.sent')}
        </p>
        <p className="t-body">{tf('success.sentBody', { email: sent })}</p>
      </div>
    );
  }

  return (
    <form
      noValidate
      className="grid gap-6 md:grid-cols-2"
      onSubmit={(e) => {
        e.preventDefault();
        const form = new FormData(e.currentTarget);
        start(async () => {
          const res = await sendContactMessage(form);
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
      <Field id={`${id}-subject`} label={t('subject')} className="md:col-span-2">
        <Select id={`${id}-subject`} name="subject" value={subject} onChange={(e) => setSubject(e.target.value)}>
          {SUBJECTS.map((s) => (
            <option key={s} value={s}>
              {t(`subjects.${s}`)}
            </option>
          ))}
        </Select>
      </Field>
      {subject === 'privateDining' ? (
        <p className="t-small md:col-span-2">
          {t('privateHint')}{' '}
          <Link href="/private-dining" className="underline underline-offset-4">
            {tc('nav.privateDining')}
          </Link>
        </p>
      ) : null}
      <Field id={`${id}-name`} label={tf('fields.name')} required requiredLabel={tc('a11y.required')} error={err('name')}>
        <Input id={`${id}-name`} name="name" autoComplete="name" required {...describedBy(`${id}-name`, { error: Boolean(err('name')) })} />
      </Field>
      <Field id={`${id}-email`} label={tf('fields.email')} required requiredLabel={tc('a11y.required')} error={err('email')}>
        <Input id={`${id}-email`} name="email" type="email" autoComplete="email" required {...describedBy(`${id}-email`, { error: Boolean(err('email')) })} />
      </Field>
      <Field id={`${id}-phone`} label={`${tf('fields.phone')} (${tf('fields.optional')})`} hint={tf('fields.phoneHint')} error={err('phone')}>
        <Input id={`${id}-phone`} name="phone" type="tel" autoComplete="tel" inputMode="tel" {...describedBy(`${id}-phone`, { hint: true, error: Boolean(err('phone')) })} />
      </Field>
      <Field id={`${id}-branch`} label={`${tf('fields.branch')} (${tf('fields.optional')})`}>
        <Select id={`${id}-branch`} name="branch" defaultValue="">
          <option value="">—</option>
          {branches.map((b) => (
            <option key={b.slug} value={b.slug}>
              {b.name}
            </option>
          ))}
        </Select>
      </Field>
      <Field id={`${id}-message`} label={tf('fields.message')} required requiredLabel={tc('a11y.required')} error={err('message')} className="md:col-span-2">
        <Textarea id={`${id}-message`} name="message" rows={7} required maxLength={4000} {...describedBy(`${id}-message`, { error: Boolean(err('message')) })} />
      </Field>
      <div className="md:col-span-2">
        <Checkbox id={`${id}-consent`} name="consent" label={tf('consent.privacy')} required aria-invalid={Boolean(err('consent')) || undefined} />
        {err('consent') ? <p role="alert" className="t-small mt-2 text-danger">{err('consent')}</p> : null}
      </div>
      <div className="flex flex-col gap-3 md:col-span-2">
        {formError ? <p role="alert" className="t-small text-danger">{formError}</p> : null}
        <Button type="submit" size="lg" disabled={pending} className="self-start">
          {pending ? tc('actions.sending') : t('submit')}
        </Button>
      </div>
    </form>
  );
}
