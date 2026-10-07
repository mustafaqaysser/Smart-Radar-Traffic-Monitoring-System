'use client';

import { useLocale, useTranslations } from 'next-intl';
import { useId, useState, useTransition } from 'react';
import { BotFields } from '@/components/site/forms/bot-fields';
import { Button } from '@/components/site/ui/button';
import { Checkbox, describedBy, Field, Input, Textarea } from '@/components/site/ui/field';
import { Link } from '@/i18n/navigation';
import type { ActionResult } from '@/lib/actions/result';
import type { BookingOutcome } from '@/lib/actions/reservations';
import { formatMoney } from '@/lib/i18n/format';
import { plural } from '@/lib/i18n/plural';
import type { GuestPrefill } from '@/lib/reserve/types';

/** Booking errors with their own wording in the `reserve.errors` namespace. */
const FLOW_ERRORS = ['holdExpired', 'unavailable', 'taken', 'unchanged', 'closed'];

interface DetailsFormProps {
  guest: GuestPrefill | null;
  signedIn: boolean;
  deposit: number;
  rules: { minParty: number; perGuest: number; cutoffHours: number };
  newsletter: boolean;
  hidden: Record<string, string>;
  action: (form: FormData) => Promise<ActionResult<BookingOutcome>>;
  onOutcome: (outcome: BookingOutcome) => void;
  /** Hold-related failures the flow handles itself (the table was lost); returns true when handled. */
  onFlowError?: (key: string) => boolean;
}

/** The guest's details and the confirm button (shared by the booking flow and waitlist offers). */
export function DetailsForm({ guest, signedIn, deposit, rules, newsletter, hidden, action, onOutcome, onFlowError }: DetailsFormProps) {
  const t = useTranslations('reserve');
  const tf = useTranslations('forms');
  const tc = useTranslations('common');
  const tu = useTranslations('common.units');
  const locale = useLocale();
  const id = useId();
  const [pending, start] = useTransition();
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [formError, setFormError] = useState<string | null>(null);
  const err = (k: string) => (errors[k] ? tf(`errors.${errors[k]}`) : undefined);
  const message = (key: string) => (FLOW_ERRORS.includes(key) ? t(`errors.${key}`) : tf.has(`errors.${key}`) ? tf(`errors.${key}`) : tf('errors.unknown'));

  return (
    <form
      noValidate
      className="grid gap-6 md:grid-cols-2"
      onSubmit={(e) => {
        e.preventDefault();
        const form = new FormData(e.currentTarget);
        setFormError(null);
        start(async () => {
          const res = await action(form);
          if (res.ok) {
            onOutcome(res.data);
            return;
          }
          if (onFlowError?.(res.error)) return;
          setErrors(res.fieldErrors ?? {});
          setFormError(message(res.error));
        });
      }}
    >
      <BotFields />
      {Object.entries(hidden).map(([name, value]) => (
        <input key={name} type="hidden" name={name} value={value} />
      ))}
      <input type="hidden" name="locale" value={locale} />
      {signedIn && guest ? <p className="t-small text-muted md:col-span-2">{t('details.signedIn', { name: guest.name })}</p> : null}
      <Field id={`${id}-name`} label={tf('fields.name')} required requiredLabel={tc('a11y.required')} error={err('name')}>
        <Input id={`${id}-name`} name="name" autoComplete="name" required defaultValue={guest?.name ?? ''} {...describedBy(`${id}-name`, { error: Boolean(err('name')) })} />
      </Field>
      <Field id={`${id}-email`} label={tf('fields.email')} required requiredLabel={tc('a11y.required')} error={err('email')}>
        <Input id={`${id}-email`} name="email" type="email" autoComplete="email" required defaultValue={guest?.email ?? ''} {...describedBy(`${id}-email`, { error: Boolean(err('email')) })} />
      </Field>
      <Field id={`${id}-phone`} label={tf('fields.phone')} hint={tf('fields.phoneHint')} required requiredLabel={tc('a11y.required')} error={err('phone')} className="md:col-span-2">
        <Input id={`${id}-phone`} name="phone" type="tel" inputMode="tel" autoComplete="tel" required defaultValue={guest?.phone ?? ''} {...describedBy(`${id}-phone`, { hint: true, error: Boolean(err('phone')) })} />
      </Field>
      <Field id={`${id}-notes`} label={`${t('details.notes')} (${tf('fields.optional')})`} hint={t('details.notesHint')} error={err('notes')} className="md:col-span-2">
        <Textarea id={`${id}-notes`} name="notes" rows={3} maxLength={500} {...describedBy(`${id}-notes`, { hint: true, error: Boolean(err('notes')) })} />
      </Field>
      <Field id={`${id}-dietary`} label={`${t('details.dietary')} (${tf('fields.optional')})`} hint={t('details.dietaryHint')} error={err('dietary')} className="md:col-span-2">
        <Input id={`${id}-dietary`} name="dietary" maxLength={300} {...describedBy(`${id}-dietary`, { hint: true, error: Boolean(err('dietary')) })} />
      </Field>
      {newsletter ? <Checkbox id={`${id}-marketing`} name="marketing" label={t('details.marketing')} className="md:col-span-2" /> : null}

      {deposit > 0 ? (
        <div className="flex flex-col gap-2 border-s-2 border-accent ps-5 md:col-span-2">
          <p className="t-heading-sm">{t('deposit.title', { amount: formatMoney(deposit, locale) })}</p>
          <p className="t-small text-muted">
            {t('deposit.body', { n: plural(rules.minParty, locale).n, perGuest: formatMoney(rules.perGuest, locale), hours: tu('hours', plural(rules.cutoffHours, locale)) })}
          </p>
        </div>
      ) : null}

      <div className="flex flex-col gap-4 md:col-span-2">
        {formError ? (
          <p role="alert" className="t-small text-danger">
            {formError}
          </p>
        ) : null}
        <Button type="submit" size="lg" icon="arrow" disabled={pending} className="w-full sm:w-auto sm:self-start">
          {pending ? t('details.submitting') : deposit > 0 ? t('details.submitDeposit') : t('details.submit')}
        </Button>
        <p className="t-small text-muted">
          {t('details.privacy')}{' '}
          <Link href="/legal/privacy" className="underline underline-offset-4">
            {t('details.privacyLink')}
          </Link>
        </p>
      </div>
    </form>
  );
}
