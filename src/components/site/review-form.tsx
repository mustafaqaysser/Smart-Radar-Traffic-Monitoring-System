'use client';

import { useLocale, useTranslations } from 'next-intl';
import { useId, useState, useTransition } from 'react';
import { Icon } from '@/components/brand/icon';
import { iconPaths } from '@/components/brand/icon-paths';
import { BotFields } from '@/components/site/forms/bot-fields';
import { Button } from '@/components/site/ui/button';
import { describedBy, Field, Input, Select, Textarea } from '@/components/site/ui/field';
import { submitReview } from '@/lib/actions/reviews';
import { formatNumber } from '@/lib/i18n/format';
import { cn } from '@/lib/utils/cn';

export function ReviewForm({ branches, selected, today }: { branches: { slug: string; name: string }[]; selected: string | null; today: string }) {
  const t = useTranslations('pages.reviews.write');
  const tf = useTranslations('forms');
  const tc = useTranslations('common');
  const locale = useLocale();
  const id = useId();
  const [pending, start] = useTransition();
  const [rating, setRating] = useState(0);
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [formError, setFormError] = useState<string | null>(null);
  const [done, setDone] = useState(false);

  if (done) {
    return (
      <p role="status" className="t-body-lg flex items-start gap-3 border-t border-ink pt-6">
        <Icon name="check" size={24} className="mt-1" />
        {t('done')}
      </p>
    );
  }
  const err = (k: string) => (errors[k] ? tf(`errors.${errors[k]}`) : undefined);

  return (
    <form
      noValidate
      className="grid gap-6 md:grid-cols-2"
      onSubmit={(e) => {
        e.preventDefault();
        const form = new FormData(e.currentTarget);
        form.set('rating', String(rating));
        start(async () => {
          const res = await submitReview(form);
          if (res.ok) setDone(true);
          else {
            setErrors(res.fieldErrors ?? {});
            setFormError(tf(`errors.${res.error}`));
          }
        });
      }}
    >
      <BotFields />
      <input type="hidden" name="locale" value={locale} />
      <fieldset className="md:col-span-2">
        <legend className="t-label mb-3 text-muted">{t('rating')}</legend>
        <div className="flex gap-1" role="radiogroup" aria-label={t('rating')}>
          {[1, 2, 3, 4, 5].map((n) => (
            <label key={n} className="cursor-pointer p-1">
              <input type="radio" name="rating-choice" value={n} checked={rating === n} onChange={() => setRating(n)} className="peer sr-only" />
              <span className="sr-only">{t('ratingValue', { n: formatNumber(n, locale) })}</span>
              <svg viewBox="0 0 24 24" width={32} height={32} aria-hidden="true" className={cn('text-accent peer-focus-visible:outline-2 peer-focus-visible:outline-ink', n <= rating ? 'fill-current' : 'fill-none')} stroke="currentColor" strokeWidth={1.5}>
                <path d={iconPaths.star} />
              </svg>
            </label>
          ))}
        </div>
        {err('rating') ? <p role="alert" className="t-small mt-2 text-danger">{err('rating')}</p> : null}
      </fieldset>
      <Field id={`${id}-branch`} label={tf('fields.branch')} error={err('branch')}>
        <Select id={`${id}-branch`} name="branch" defaultValue={selected ?? branches[0]?.slug}>
          {branches.map((b) => (
            <option key={b.slug} value={b.slug}>
              {b.name}
            </option>
          ))}
        </Select>
      </Field>
      <Field id={`${id}-visit`} label={t('visit')} error={err('visitDate')}>
        <Input id={`${id}-visit`} name="visitDate" type="date" max={today} {...describedBy(`${id}-visit`, { error: Boolean(err('visitDate')) })} />
      </Field>
      <Field id={`${id}-name`} label={tf('fields.name')} required error={err('name')}>
        <Input id={`${id}-name`} name="name" autoComplete="name" required {...describedBy(`${id}-name`, { error: Boolean(err('name')) })} />
      </Field>
      <Field id={`${id}-email`} label={tf('fields.email')} required error={err('email')}>
        <Input id={`${id}-email`} name="email" type="email" autoComplete="email" required {...describedBy(`${id}-email`, { error: Boolean(err('email')) })} />
      </Field>
      <Field id={`${id}-title`} label={t('titleField')} error={err('title')} className="md:col-span-2">
        <Input id={`${id}-title`} name="title" maxLength={90} {...describedBy(`${id}-title`, { error: Boolean(err('title')) })} />
      </Field>
      <Field id={`${id}-body`} label={t('body')} hint={t('bodyHint')} required error={err('body')} className="md:col-span-2">
        <Textarea id={`${id}-body`} name="body" required minLength={30} maxLength={2000} rows={6} {...describedBy(`${id}-body`, { hint: true, error: Boolean(err('body')) })} />
      </Field>
      <div className="flex flex-col gap-3 md:col-span-2">
        {formError ? <p role="alert" className="t-small text-danger">{formError}</p> : null}
        <Button type="submit" size="lg" disabled={pending} className="self-start">
          {pending ? tc('actions.sending') : t('submit')}
        </Button>
      </div>
    </form>
  );
}
