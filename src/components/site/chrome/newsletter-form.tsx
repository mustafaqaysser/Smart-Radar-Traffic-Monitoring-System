'use client';

import { useLocale, useTranslations } from 'next-intl';
import { useId, useState, useTransition } from 'react';
import { subscribeNewsletter } from '@/lib/actions/site';
import { BotFields } from '@/components/site/forms/bot-fields';
import { Icon } from '@/components/brand/icon';
import { cn } from '@/lib/utils/cn';

export function NewsletterForm({ source = 'footer', className }: { source?: string; className?: string }) {
  const t = useTranslations('forms');
  const tc = useTranslations('common');
  const locale = useLocale();
  const id = useId();
  const [pending, start] = useTransition();
  const [state, setState] = useState<{ kind: 'idle' } | { kind: 'done'; email: string } | { kind: 'error'; message: string }>({ kind: 'idle' });

  if (state.kind === 'done') {
    return (
      <p role="status" className={cn('t-body flex items-start gap-3', className)}>
        <Icon name="check" size={22} className="mt-1" />
        <span>{t('newsletter.sent', { email: state.email })}</span>
      </p>
    );
  }

  return (
    <form
      className={className}
      noValidate
      onSubmit={(e) => {
        e.preventDefault();
        const form = new FormData(e.currentTarget);
        start(async () => {
          const res = await subscribeNewsletter(form);
          if (res.ok) setState({ kind: 'done', email: res.data.email });
          else setState({ kind: 'error', message: res.fieldErrors?.email ? t(`errors.${res.fieldErrors.email}`) : t(`errors.${res.error}`) });
        });
      }}
    >
      <input type="hidden" name="locale" value={locale} />
      <input type="hidden" name="source" value={source} />
      <BotFields />
      <label htmlFor={`${id}-email`} className="t-label mb-2 block opacity-80">
        {t('fields.email')}
      </label>
      <div className="flex border-b border-current/40 focus-within:border-current">
        <input
          id={`${id}-email`}
          name="email"
          type="email"
          dir="ltr"
          required
          autoComplete="email"
          inputMode="email"
          placeholder="name@example.com"
          aria-invalid={state.kind === 'error' || undefined}
          aria-describedby={state.kind === 'error' ? `${id}-error` : undefined}
          className="min-h-12 min-w-0 flex-1 bg-transparent py-2 text-start placeholder:opacity-50 focus:outline-none rtl:text-end"
        />
        <button type="submit" disabled={pending} className="group/btn inline-flex min-h-12 items-center gap-2 ps-4 font-label text-[0.75rem] tracking-[0.14em] uppercase rtl:text-base rtl:tracking-normal rtl:normal-case">
          {pending ? tc('actions.sending') : tc('actions.subscribe')}
          <Icon name="arrow" size={18} />
        </button>
      </div>
      {state.kind === 'error' ? (
        <p id={`${id}-error`} role="alert" className="t-small mt-2">
          {state.message}
        </p>
      ) : null}
    </form>
  );
}
