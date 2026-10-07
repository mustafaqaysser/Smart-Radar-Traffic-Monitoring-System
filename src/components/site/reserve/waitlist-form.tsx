'use client';

import { useLocale, useTranslations } from 'next-intl';
import { useId, useState, useTransition } from 'react';
import { Icon } from '@/components/brand/icon';
import { BotFields } from '@/components/site/forms/bot-fields';
import { Button } from '@/components/site/ui/button';
import { describedBy, Field, Input, Select, Textarea } from '@/components/site/ui/field';
import { joinReservationWaitlist } from '@/lib/actions/reservations';
import { formatWallTime } from '@/lib/i18n/format';
import type { GuestPrefill } from '@/lib/reserve/types';
import { cn } from '@/lib/utils/cn';

const FLEX = [
  { value: '30', key: 'flex30' },
  { value: '60', key: 'flex60' },
  { value: '120', key: 'flex120' },
  { value: '1440', key: 'flexAny' },
] as const;

interface WaitlistFormProps {
  branch: string;
  date: string;
  dateLabel: string;
  party: number;
  times: string[];
  guest: GuestPrefill | null;
}

/** Joins the waitlist for a full day: preferred time, flexibility and contact details. */
export function WaitlistForm({ branch, date, dateLabel, party, times, guest }: WaitlistFormProps) {
  const t = useTranslations('reserve.waitlist');
  const tf = useTranslations('forms');
  const tc = useTranslations('common');
  const locale = useLocale();
  const id = useId();
  const [pending, start] = useTransition();
  const [flex, setFlex] = useState<string>('60');
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [formError, setFormError] = useState<string | null>(null);
  const [done, setDone] = useState<string | null>(null);
  const err = (k: string) => (errors[k] ? tf(`errors.${errors[k]}`) : undefined);
  const preferred = times[Math.floor(times.length / 2)] ?? times[0] ?? '20:00';

  if (done) {
    return (
      <div role="status" className="flex items-start gap-3 border-t border-ink pt-5">
        <Icon name="check" size={22} className="mt-1 shrink-0" />
        <p className="t-body">{t('done', { date: dateLabel, email: done })}</p>
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
        setFormError(null);
        start(async () => {
          const res = await joinReservationWaitlist(form);
          if (res.ok) setDone(res.data.email);
          else {
            setErrors(res.fieldErrors ?? {});
            setFormError(tf.has(`errors.${res.error}`) ? tf(`errors.${res.error}`) : tf('errors.unknown'));
          }
        });
      }}
    >
      <BotFields />
      <input type="hidden" name="branch" value={branch} />
      <input type="hidden" name="date" value={date} />
      <input type="hidden" name="party" value={party} />
      <input type="hidden" name="locale" value={locale} />
      <input type="hidden" name="flexible" value={flex} />
      <p className="t-body md:col-span-2">{t('body')}</p>
      <Field id={`${id}-time`} label={t('preferred')}>
        <Select id={`${id}-time`} name="preferredTime" defaultValue={preferred}>
          {times.map((time) => (
            <option key={time} value={time}>
              {formatWallTime(time, locale)}
            </option>
          ))}
        </Select>
      </Field>
      <fieldset className="md:col-span-2">
        <legend className="t-label mb-3 text-muted">{t('flexible')}</legend>
        <div className="flex flex-wrap gap-2" role="radiogroup">
          {FLEX.map((f) => (
            <button
              key={f.value}
              type="button"
              role="radio"
              aria-checked={flex === f.value}
              onClick={() => setFlex(f.value)}
              className={cn('inline-flex min-h-11 items-center rounded-pill border px-4 text-[0.9375rem] transition-colors', flex === f.value ? 'border-ink bg-ink text-bg' : 'border-line hover-capable:hover:border-ink')}
            >
              {t(f.key)}
            </button>
          ))}
        </div>
      </fieldset>
      <Field id={`${id}-name`} label={tf('fields.name')} required requiredLabel={tc('a11y.required')} error={err('name')}>
        <Input id={`${id}-name`} name="name" autoComplete="name" required defaultValue={guest?.name ?? ''} {...describedBy(`${id}-name`, { error: Boolean(err('name')) })} />
      </Field>
      <Field id={`${id}-email`} label={tf('fields.email')} required requiredLabel={tc('a11y.required')} error={err('email')}>
        <Input id={`${id}-email`} name="email" type="email" autoComplete="email" required defaultValue={guest?.email ?? ''} {...describedBy(`${id}-email`, { error: Boolean(err('email')) })} />
      </Field>
      <Field id={`${id}-phone`} label={tf('fields.phone')} hint={tf('fields.phoneHint')} required requiredLabel={tc('a11y.required')} error={err('phone')} className="md:col-span-2">
        <Input id={`${id}-phone`} name="phone" type="tel" inputMode="tel" autoComplete="tel" required defaultValue={guest?.phone ?? ''} {...describedBy(`${id}-phone`, { hint: true, error: Boolean(err('phone')) })} />
      </Field>
      <Field id={`${id}-notes`} label={`${tf('fields.notes')} (${tf('fields.optional')})`} error={err('notes')} className="md:col-span-2">
        <Textarea id={`${id}-notes`} name="notes" rows={3} maxLength={500} {...describedBy(`${id}-notes`, { error: Boolean(err('notes')) })} />
      </Field>
      <div className="flex flex-col gap-3 md:col-span-2">
        {formError ? (
          <p role="alert" className="t-small text-danger">
            {formError}
          </p>
        ) : null}
        <Button type="submit" variant="secondary" size="lg" disabled={pending} className="w-full sm:w-auto sm:self-start">
          {pending ? tc('actions.sending') : t('submit')}
        </Button>
      </div>
    </form>
  );
}
