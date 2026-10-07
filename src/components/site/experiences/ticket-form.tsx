'use client';

import { useRouter } from 'next/navigation';
import { useLocale, useTranslations } from 'next-intl';
import { useId, useState, useTransition } from 'react';
import { BotFields } from '@/components/site/forms/bot-fields';
import { PaymentForm } from '@/components/site/payment-form';
import { Button } from '@/components/site/ui/button';
import { describedBy, Field, Input } from '@/components/site/ui/field';
import { Quantity } from '@/components/site/ui/quantity';
import { bookEventSeats } from '@/lib/actions/commerce';
import { formatMoney } from '@/lib/i18n/format';
import type { StartedPayment } from '@/lib/server/payments';
import { cn } from '@/lib/utils/cn';

interface TicketOption {
  id: string;
  name: string;
  description: string | null;
  price: number;
  left: number;
}

/** Ticket type, seats and contact details; paid tickets continue to payment, free ones are confirmed at once. */
export function TicketForm({ event, tickets, seatsLeft, user }: { event: string; tickets: TicketOption[]; seatsLeft: number; user: { name: string; email: string; phone: string } | null }) {
  const t = useTranslations('gather.experiences');
  const tf = useTranslations('forms');
  const tc = useTranslations('common');
  const locale = useLocale();
  const router = useRouter();
  const id = useId();
  const firstOpen = tickets.find((x) => x.left > 0) ?? tickets[0];
  const [ticketId, setTicketId] = useState(firstOpen?.id ?? '');
  const [qty, setQty] = useState(1);
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [formError, setFormError] = useState<string | null>(null);
  const [placed, setPlaced] = useState<{ href: string; payment: StartedPayment } | null>(null);
  const [pending, start] = useTransition();
  const ticket = tickets.find((x) => x.id === ticketId);
  const max = Math.max(1, Math.min(12, seatsLeft, ticket?.left ?? 0));
  const total = (ticket?.price ?? 0) * qty;
  const err = (k: string) => (errors[k] ? tf(`errors.${errors[k]}`) : undefined);

  if (placed) {
    return (
      <PaymentForm
        {...placed.payment}
        onSucceeded={() => {
          const target = new URL(placed.payment.returnUrl);
          router.push(`${target.pathname}${target.search}`);
        }}
      />
    );
  }

  return (
    <form
      noValidate
      className="flex flex-col gap-8"
      onSubmit={(e) => {
        e.preventDefault();
        const data = new FormData(e.currentTarget);
        setErrors({});
        setFormError(null);
        start(async () => {
          const res = await bookEventSeats({
            event,
            ticketTypeId: ticketId,
            quantity: qty,
            name: String(data.get('name') ?? ''),
            email: String(data.get('email') ?? ''),
            phone: String(data.get('phone') ?? ''),
            locale: locale as 'ar' | 'en',
            company_website: String(data.get('company_website') ?? ''),
            rendered_at: String(data.get('rendered_at') ?? ''),
          });
          if (!res.ok) {
            setErrors(res.fieldErrors ?? {});
            setFormError(t.has(`errors.${res.error}`) ? t(`errors.${res.error}`) : tf.has(`errors.${res.error}`) ? tf(`errors.${res.error}`) : tf('errors.unknown'));
            return;
          }
          if (res.data.payment) setPlaced({ href: res.data.href, payment: res.data.payment });
          else router.push(res.data.href);
        });
      }}
    >
      <BotFields />
      <fieldset className="flex flex-col gap-3">
        <legend className="t-label mb-3 text-muted">{t('tickets')}</legend>
        {tickets.map((x) => (
          <button
            key={x.id}
            type="button"
            aria-pressed={ticketId === x.id}
            disabled={x.left === 0}
            onClick={() => {
              setTicketId(x.id);
              setQty((q) => Math.min(q, Math.max(1, x.left)));
            }}
            className={cn('flex flex-col gap-1 border px-5 py-4 text-start disabled:cursor-not-allowed disabled:opacity-50', ticketId === x.id ? 'border-ink bg-ink text-bg' : 'border-line hover-capable:hover:border-ink')}
          >
            <span className="flex items-baseline justify-between gap-4">
              <span className="t-heading-sm">{x.name}</span>
              <span className="t-body tabular">{x.price === 0 ? t('free') : <bdi>{formatMoney(x.price, locale)}</bdi>}</span>
            </span>
            {x.description ? <span className={cn('t-small', ticketId === x.id ? 'text-bg/80' : 'text-muted')}>{x.description}</span> : null}
            {x.left === 0 ? <span className="t-small">{t('soldOut')}</span> : null}
          </button>
        ))}
      </fieldset>
      <div className="flex items-center justify-between gap-4">
        <span className="t-label text-muted">{t('seats')}</span>
        <Quantity value={Math.min(qty, max)} onChange={setQty} min={1} max={max} label={t('seats')} />
      </div>
      <div className="grid gap-6 sm:grid-cols-2">
        <Field id={`${id}-name`} label={tf('fields.name')} required requiredLabel={tc('a11y.required')} error={err('name')}>
          <Input id={`${id}-name`} name="name" autoComplete="name" defaultValue={user?.name ?? ''} required {...describedBy(`${id}-name`, { error: Boolean(err('name')) })} />
        </Field>
        <Field id={`${id}-email`} label={tf('fields.email')} required requiredLabel={tc('a11y.required')} error={err('email')}>
          <Input id={`${id}-email`} name="email" type="email" autoComplete="email" defaultValue={user?.email ?? ''} required {...describedBy(`${id}-email`, { error: Boolean(err('email')) })} />
        </Field>
        <Field id={`${id}-phone`} label={tf('fields.phone')} hint={tf('fields.phoneHint')} required requiredLabel={tc('a11y.required')} error={err('phone')} className="sm:col-span-2">
          <Input id={`${id}-phone`} name="phone" type="tel" inputMode="tel" autoComplete="tel" defaultValue={user?.phone ?? ''} required {...describedBy(`${id}-phone`, { hint: true, error: Boolean(err('phone')) })} />
        </Field>
      </div>
      <div className="flex items-baseline justify-between border-t border-ink pt-4">
        <span className="t-label">{t('total')}</span>
        <span className="t-heading-md tabular">
          <bdi>{formatMoney(total, locale)}</bdi>
        </span>
      </div>
      {formError ? (
        <p role="alert" className="t-small text-danger">
          {formError}
        </p>
      ) : null}
      <Button type="submit" size="lg" icon="arrow" disabled={pending || !ticket || ticket.left === 0} className="w-full">
        {pending ? t('booking') : total > 0 ? t('pay', { total: formatMoney(total, locale) }) : t('reserveFree')}
      </Button>
    </form>
  );
}
