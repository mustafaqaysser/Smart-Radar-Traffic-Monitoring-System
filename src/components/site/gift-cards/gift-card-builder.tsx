'use client';

import { useRouter } from 'next/navigation';
import { useLocale, useTranslations } from 'next-intl';
import { useId, useState, useTransition } from 'react';
import { BotFields } from '@/components/site/forms/bot-fields';
import { PaymentForm } from '@/components/site/payment-form';
import { Button } from '@/components/site/ui/button';
import { Chip, describedBy, Field, Input, Textarea } from '@/components/site/ui/field';
import { buyGiftCard } from '@/lib/actions/commerce';
import { normalizeDigits, parseMoneyInput } from '@/lib/i18n/digits';
import { formatMoney } from '@/lib/i18n/format';
import type { StartedPayment } from '@/lib/server/payments';
import { localToUtc, parseTime } from '@/lib/time/zoned';
import { cn } from '@/lib/utils/cn';
import { GiftCardPreview } from './gift-card-preview';

interface GiftCardBuilderProps {
  designs: string[];
  presets: number[];
  min: number;
  max: number;
  timeZone: string;
  today: string;
  user: { name: string; email: string } | null;
}

/** Design, amount, recipient, message and delivery time, with the card previewed as it will arrive. */
export function GiftCardBuilder({ designs, presets, min, max, timeZone, today, user }: GiftCardBuilderProps) {
  const t = useTranslations('gather.giftCards');
  const tf = useTranslations('forms');
  const tc = useTranslations('common');
  const locale = useLocale();
  const router = useRouter();
  const id = useId();
  const [design, setDesign] = useState(designs.includes('long-shade') ? 'long-shade' : (designs[0] ?? 'noon'));
  const [preset, setPreset] = useState<number | null>(presets[1] ?? presets[0] ?? null);
  const [custom, setCustom] = useState('');
  const [recipientName, setRecipientName] = useState('');
  const [message, setMessage] = useState('');
  const [purchaserName, setPurchaserName] = useState(user?.name ?? '');
  const [later, setLater] = useState(false);
  const [date, setDate] = useState(today);
  const [time, setTime] = useState('09:00');
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [formError, setFormError] = useState<string | null>(null);
  const [placed, setPlaced] = useState<{ href: string; payment: StartedPayment } | null>(null);
  const [pending, start] = useTransition();
  const amount = preset !== null ? preset : parseMoneyInput(custom);
  const amountOk = Number.isFinite(amount) && amount >= min && amount <= max;
  const err = (k: string) => (errors[k] ? tf(`errors.${errors[k]}`) : undefined);
  const money = (v: number) => formatMoney(v, locale);

  const submit = (form: HTMLFormElement) => {
    const data = new FormData(form);
    setErrors({});
    setFormError(null);
    if (!amountOk) {
      setFormError(t('errors.amount', { min: money(min), max: money(max) }));
      return;
    }
    let deliverAt: string | null = null;
    if (later) {
      const at = localToUtc(date, parseTime(normalizeDigits(time)), timeZone);
      if (!at || at.getTime() < Date.now()) {
        setFormError(t('errors.deliverAt'));
        return;
      }
      deliverAt = at.toISOString();
    }
    start(async () => {
      const res = await buyGiftCard({
        amount,
        design,
        recipientName: String(data.get('recipientName') ?? ''),
        recipientEmail: String(data.get('recipientEmail') ?? ''),
        purchaserName: String(data.get('purchaserName') ?? ''),
        purchaserEmail: String(data.get('purchaserEmail') ?? ''),
        message: String(data.get('message') ?? ''),
        deliverAt,
        locale: locale as 'ar' | 'en',
        company_website: String(data.get('company_website') ?? ''),
        rendered_at: String(data.get('rendered_at') ?? ''),
      });
      if (res.ok) setPlaced(res.data);
      else {
        setErrors(res.fieldErrors ?? {});
        setFormError(res.error === 'validation' && res.fieldErrors?.deliverAt ? t('errors.deliverAt') : tf.has(`errors.${res.error}`) ? tf(`errors.${res.error}`) : tf('errors.unknown'));
      }
    });
  };

  const preview = <GiftCardPreview design={design} amount={amountOk ? amount : null} to={recipientName} from={purchaserName} message={message} label={t('designAlt', { design: t(`designs.${design}`) })} />;

  if (placed) {
    return (
      <div className="site-grid gap-y-10 pb-[var(--spacing-section)]">
        <section aria-labelledby={`${id}-pay`} className="col-span-full flex flex-col gap-6 border-t border-ink pt-6 lg:col-span-6">
          <h2 id={`${id}-pay`} className="t-heading-md">
            {t('pay')}
          </h2>
          <PaymentForm
            {...placed.payment}
            onSucceeded={() => {
              const target = new URL(placed.payment.returnUrl);
              router.push(`${target.pathname}${target.search}`);
            }}
          />
        </section>
        <div className="col-span-full lg:col-span-5 lg:col-start-8">{preview}</div>
      </div>
    );
  }

  return (
    <form
      noValidate
      className="site-grid gap-y-12 pb-[var(--spacing-section)]"
      onSubmit={(e) => {
        e.preventDefault();
        submit(e.currentTarget);
      }}
    >
      <BotFields />
      <div className="col-span-full flex flex-col gap-12 lg:col-span-6">
        <fieldset className="flex flex-col gap-4 border-t border-ink pt-6">
          <legend className="t-heading-md mb-4 float-start w-full">{t('steps.design')}</legend>
          <div className="grid grid-cols-2 gap-3">
            {designs.map((d) => (
              <button key={d} type="button" aria-pressed={design === d} onClick={() => setDesign(d)} className={cn('flex flex-col gap-2 text-start', design === d ? 'opacity-100' : 'opacity-75 hover-capable:hover:opacity-100')}>
                {/* eslint-disable-next-line @next/next/no-img-element -- generated SVG artwork, served by our own route */}
                <img src={`/api/gift-cards/art/${d}`} alt="" width={1000} height={630} className={cn('aspect-[1000/630] w-full border', design === d ? 'border-ink outline-2 outline-offset-2 outline-ink' : 'border-line')} />
                <span className="t-small">{t(`designs.${d}`)}</span>
              </button>
            ))}
          </div>
        </fieldset>

        <fieldset className="flex flex-col gap-4 border-t border-ink pt-6">
          <legend className="t-heading-md mb-4 float-start w-full">{t('steps.amount')}</legend>
          <div className="flex flex-wrap gap-2">
            {presets.map((p) => (
              <Chip key={p} pressed={preset === p} onClick={() => { setPreset(p); setCustom(''); }} className="tabular">
                <bdi>{money(p)}</bdi>
              </Chip>
            ))}
            <Chip pressed={preset === null} onClick={() => setPreset(null)}>
              {t('custom')}
            </Chip>
          </div>
          {preset === null ? (
            <Field id={`${id}-custom`} label={t('custom')} hint={t('range', { min: money(min), max: money(max) })}>
              <Input id={`${id}-custom`} inputMode="decimal" value={custom} onChange={(e) => setCustom(e.target.value)} className="max-w-48 tabular" {...describedBy(`${id}-custom`, { hint: true })} />
            </Field>
          ) : null}
        </fieldset>

        <fieldset className="grid gap-6 border-t border-ink pt-6 sm:grid-cols-2">
          <legend className="t-heading-md mb-4 float-start w-full sm:col-span-2">{t('steps.to')}</legend>
          <Field id={`${id}-rn`} label={t('recipientName')} required requiredLabel={tc('a11y.required')} error={err('recipientName')}>
            <Input id={`${id}-rn`} name="recipientName" value={recipientName} onChange={(e) => setRecipientName(e.target.value)} maxLength={80} required {...describedBy(`${id}-rn`, { error: Boolean(err('recipientName')) })} />
          </Field>
          <Field id={`${id}-re`} label={t('recipientEmail')} required requiredLabel={tc('a11y.required')} error={err('recipientEmail')}>
            <Input id={`${id}-re`} name="recipientEmail" type="email" required {...describedBy(`${id}-re`, { error: Boolean(err('recipientEmail')) })} />
          </Field>
          <Field id={`${id}-msg`} label={`${t('message')} (${tf('fields.optional')})`} hint={t('messageHint')} error={err('message')} className="sm:col-span-2">
            <Textarea id={`${id}-msg`} name="message" rows={3} maxLength={300} value={message} onChange={(e) => setMessage(e.target.value)} {...describedBy(`${id}-msg`, { hint: true, error: Boolean(err('message')) })} />
          </Field>
        </fieldset>

        <fieldset className="grid gap-6 border-t border-ink pt-6 sm:grid-cols-2">
          <legend className="t-heading-md mb-4 float-start w-full sm:col-span-2">{t('steps.from')}</legend>
          <Field id={`${id}-pn`} label={t('yourName')} required requiredLabel={tc('a11y.required')} error={err('purchaserName')}>
            <Input id={`${id}-pn`} name="purchaserName" autoComplete="name" value={purchaserName} onChange={(e) => setPurchaserName(e.target.value)} maxLength={80} required {...describedBy(`${id}-pn`, { error: Boolean(err('purchaserName')) })} />
          </Field>
          <Field id={`${id}-pe`} label={t('yourEmail')} required requiredLabel={tc('a11y.required')} error={err('purchaserEmail')}>
            <Input id={`${id}-pe`} name="purchaserEmail" type="email" autoComplete="email" defaultValue={user?.email ?? ''} required {...describedBy(`${id}-pe`, { error: Boolean(err('purchaserEmail')) })} />
          </Field>
        </fieldset>

        <fieldset className="flex flex-col gap-4 border-t border-ink pt-6">
          <legend className="t-heading-md mb-4 float-start w-full">{t('steps.when')}</legend>
          <div className="flex flex-wrap gap-2">
            <Chip pressed={!later} onClick={() => setLater(false)}>
              {t('now')}
            </Chip>
            <Chip pressed={later} onClick={() => setLater(true)}>
              {t('later')}
            </Chip>
          </div>
          {later ? (
            <div className="grid grid-cols-2 gap-4">
              <Field id={`${id}-date`} label={t('date')}>
                <Input id={`${id}-date`} type="date" min={today} value={date} onChange={(e) => setDate(e.target.value)} />
              </Field>
              <Field id={`${id}-time`} label={t('time')}>
                <Input id={`${id}-time`} type="time" step={900} value={time} onChange={(e) => setTime(e.target.value)} />
              </Field>
            </div>
          ) : null}
        </fieldset>

        <div className="flex flex-col gap-3">
          {formError ? (
            <p role="alert" className="t-small text-danger">
              {formError}
            </p>
          ) : null}
          <Button type="submit" size="lg" icon="arrow" disabled={pending} className="w-full sm:w-auto sm:self-start">
            {pending ? t('buying') : t('buy', { amount: amountOk ? money(amount) : '—' })}
          </Button>
        </div>
      </div>

      <div className="col-span-full lg:col-span-5 lg:col-start-8">
        <div className="flex flex-col gap-4 lg:sticky lg:top-24">
          <p className="t-label text-muted">{t('preview')}</p>
          {preview}
        </div>
      </div>
    </form>
  );
}
