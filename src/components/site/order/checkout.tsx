'use client';

import { useRouter } from 'next/navigation';
import { useLocale, useTranslations } from 'next-intl';
import { useEffect, useId, useRef, useState, useTransition, type ReactNode } from 'react';
import { Icon } from '@/components/brand/icon';
import { BotFields } from '@/components/site/forms/bot-fields';
import { PaymentForm } from '@/components/site/payment-form';
import { Button, ButtonLink } from '@/components/site/ui/button';
import { Checkbox, Chip, describedBy, Field, Input, Select, Textarea } from '@/components/site/ui/field';
import { Link } from '@/i18n/navigation';
import { placeOrderAction, quoteCheckout, type CheckoutRequest } from '@/lib/actions/orders';
import { cart, useCart } from '@/lib/cart/store';
import { parseMoneyInput } from '@/lib/i18n/digits';
import { formatClock, formatDate, formatMoney, formatNumber, formatPercent } from '@/lib/i18n/format';
import { plural } from '@/lib/i18n/plural';
import { LAST_ORDER_KEY, type CheckoutBranch, type QuoteView, type SavedAddress } from '@/lib/order/types';
import type { StartedPayment } from '@/lib/server/payments';
import { addDays, toDateString } from '@/lib/time/zoned';
import { cn } from '@/lib/utils/cn';
import { PinPicker } from './pin-picker';

interface CheckoutProps {
  branch: CheckoutBranch;
  houses: Record<string, string>;
  user: { name: string; email: string; phone: string } | null;
  addresses: SavedAddress[];
  tipPresets: number[];
  loyaltyEnabled: boolean;
  giftCardsEnabled: boolean;
}

type When = { asap: true } | { asap: false; at: string };
type Tip = { kind: 'percent'; value: number } | { kind: 'amount'; value: number } | null;

function Section({ id, title, children }: { id: string; title: string; children: ReactNode }) {
  return (
    <section aria-labelledby={id} className="flex flex-col gap-6 border-t border-ink pt-6">
      <h2 id={id} className="t-heading-md">
        {title}
      </h2>
      {children}
    </section>
  );
}

/**
 * Checkout on one page: how, where, when, who, codes and points, tip, payment. Every change re-prices the
 * order on the server (the summary is never computed in the browser alone) and the times offered come from
 * the kitchen's hours and capacity.
 */
export function Checkout({ branch, houses, user, addresses, tipPresets, loyaltyEnabled, giftCardsEnabled }: CheckoutProps) {
  const t = useTranslations('order.checkout');
  const to = useTranslations('order');
  const tf = useTranslations('forms');
  const tc = useTranslations('common');
  const tu = useTranslations('common.units');
  const locale = useLocale();
  const router = useRouter();
  const id = useId();
  const c = useCart();
  const channels = (['delivery', 'pickup'] as const).filter((ch) => branch.channels[ch]);
  const channel = channels.includes(c.channel as 'delivery' | 'pickup') ? (c.channel as 'delivery' | 'pickup') : (channels[0] ?? 'pickup');
  const defaultAddress = addresses.find((a) => a.isDefault) ?? addresses[0] ?? null;

  const [addressId, setAddressId] = useState<string>(defaultAddress?.id ?? 'new');
  const [zoneId, setZoneId] = useState<string | null>(defaultAddress?.zoneId ?? null);
  const [area, setArea] = useState(defaultAddress?.area ?? '');
  const [location, setLocation] = useState<{ lat: number; lng: number } | null>(defaultAddress?.lat != null && defaultAddress?.lng != null ? { lat: defaultAddress.lat, lng: defaultAddress.lng } : null);
  const [street, setStreet] = useState(defaultAddress?.street ?? '');
  const [building, setBuilding] = useState(defaultAddress?.building ?? '');
  const [floor, setFloor] = useState(defaultAddress?.floor ?? '');
  const [addressNotes, setAddressNotes] = useState(defaultAddress?.notes ?? '');
  const [saveAddress, setSaveAddress] = useState(false);
  const [saveLabel, setSaveLabel] = useState('');
  const [when, setWhen] = useState<When>({ asap: true });
  const [promoInput, setPromoInput] = useState('');
  const [promoCode, setPromoCode] = useState<string | null>(null);
  const [giftInput, setGiftInput] = useState('');
  const [giftCode, setGiftCode] = useState<string | null>(null);
  const [usePoints, setUsePoints] = useState(false);
  const [tip, setTip] = useState<Tip>(null);
  const [customTip, setCustomTip] = useState('');
  const [method, setMethod] = useState<'card' | 'cash' | 'pay_at_venue'>('card');
  const [email, setEmail] = useState(user?.email ?? '');
  const [quote, setQuote] = useState<QuoteView | null>(null);
  const [quoteFailed, setQuoteFailed] = useState(false);
  const [quoting, setQuoting] = useState(true);
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [formError, setFormError] = useState<string | null>(null);
  const [placed, setPlaced] = useState<{ href: string; number: string; payment: StartedPayment } | null>(null);
  const [placing, startPlacing] = useTransition();
  const seq = useRef(0);

  const lines = c.lines.map((l) => ({ slug: l.slug, qty: l.qty, optionIds: l.optionIds, note: l.note }));
  const zones = branch.zones;
  const zone = zones.find((z) => z.id === zoneId) ?? null;
  const request: CheckoutRequest = {
    branch: branch.slug,
    channel,
    lines,
    when,
    zoneId: channel === 'delivery' ? zoneId : null,
    location: channel === 'delivery' && zone?.kind === 'radius' ? location : null,
    promoCode,
    giftCardCode: giftCode,
    loyaltyPoints: usePoints ? 10_000_000 : 0,
    tip,
    email: promoCode ? email : undefined,
    locale: locale as CheckoutRequest['locale'],
  };
  const requestKey = JSON.stringify(request);

  // Re-price on every change (debounced); stale answers are ignored.
  useEffect(() => {
    const mine = ++seq.current;
    const timer = window.setTimeout(() => {
      setQuoting(true);
      quoteCheckout(JSON.parse(requestKey) as CheckoutRequest)
        .then((res) => {
          if (mine !== seq.current) return;
          setQuoting(false);
          if (res.ok) {
            const q = res.data;
            setQuote(q);
            setQuoteFailed(false);
            // Kitchen closed or full right now: offer the first time that works instead of a dead "ASAP".
            if (!q.timing.asapAvailable) {
              const first = q.timing.options.find((o) => o.available && o.servesAll) ?? q.timing.options.find((o) => o.available);
              setWhen((w) => (w.asap && first ? { asap: false, at: first.at } : w));
            }
          } else setQuoteFailed(true);
        })
        .catch(() => {
          if (mine === seq.current) {
            setQuoting(false);
            setQuoteFailed(true);
          }
        });
    }, 250);
    return () => window.clearTimeout(timer);
  }, [requestKey]);

  const money = (v: number) => formatMoney(v, locale);
  const err = (k: string) => (errors[k] ? tf(`errors.${errors[k]}`) : undefined);
  const problemText = (key: string) => {
    if (key === 'below_minimum' && quote?.pricing.belowMinimum) return t('problems.below_minimum', { amount: money(quote.pricing.belowMinimum.minOrder) });
    return t.has(`problems.${key}`) ? t(`problems.${key}`) : tf.has(`errors.${key}`) ? tf(`errors.${key}`) : tf('errors.unknown');
  };

  // ——— Times, grouped by the house's calendar day.
  const options = quote?.timing.options ?? [];
  const today = toDateString(new Date(), branch.timeZone);
  const days = [...new Set(options.map((o) => toDateString(new Date(o.at), branch.timeZone)))];
  const dayLabel = (d: string) => (d === today ? t('when.today') : d === addDays(today, 1) ? t('when.tomorrow') : formatDate(new Date(`${d}T12:00:00Z`), locale, 'UTC', { weekday: 'long', day: 'numeric', month: 'long' }));
  const chosenDay = !when.asap ? toDateString(new Date(when.at), branch.timeZone) : (days[0] ?? today);
  const pickFirst = (day: string) => {
    const first = options.find((o) => toDateString(new Date(o.at), branch.timeZone) === day && o.available && o.servesAll) ?? options.find((o) => toDateString(new Date(o.at), branch.timeZone) === day && o.available);
    if (first) setWhen({ asap: false, at: first.at });
  };

  const due = quote?.pricing.amountDue ?? 0;
  const coveredByGift = Boolean(quote && quote.pricing.total > 0 && due === 0);

  const place = (form: HTMLFormElement) => {
    const data = new FormData(form);
    setErrors({});
    setFormError(null);
    startPlacing(async () => {
      const res = await placeOrderAction({
        ...request,
        email: String(data.get('email') ?? ''),
        name: String(data.get('name') ?? ''),
        phone: String(data.get('phone') ?? ''),
        notes: String(data.get('notes') ?? ''),
        paymentMethod: coveredByGift ? 'card' : method,
        address: channel === 'delivery' ? { area, street, building, floor, notes: addressNotes } : null,
        saveAddressAs: user && channel === 'delivery' && addressId === 'new' && saveAddress ? saveLabel || area : null,
        company_website: String(data.get('company_website') ?? ''),
        rendered_at: String(data.get('rendered_at') ?? ''),
      });
      if (!res.ok) {
        setErrors(res.fieldErrors ?? {});
        setFormError(problemText(res.error));
        return;
      }
      try {
        window.sessionStorage.setItem(LAST_ORDER_KEY, res.data.number);
      } catch {
        // Without session storage the basket is simply emptied now.
        cart.clear();
      }
      if (res.data.payment) setPlaced({ href: res.data.href, number: res.data.number, payment: res.data.payment });
      else {
        cart.clear();
        router.push(res.data.href);
      }
    });
  };

  if (!c.lines.length && !placed) {
    return (
      <div className="site-grid pb-[var(--spacing-section)]">
        <div className="col-span-full flex flex-col items-start gap-5 border-t border-ink pt-8 lg:col-span-7">
          <p className="t-heading-lg">{to('basket.empty')}</p>
          <ButtonLink href="/order">{to('basket.browse')}</ButtonLink>
        </div>
      </div>
    );
  }

  if (c.branchSlug && c.branchSlug !== branch.slug) {
    return (
      <div className="site-grid pb-[var(--spacing-section)]">
        <div className="col-span-full flex flex-col items-start gap-5 border-t border-ink pt-8 lg:col-span-7">
          <p className="t-body-lg">{to('basket.elsewhere', { house: houses[c.branchSlug] ?? c.branchSlug })}</p>
          <ButtonLink href="/order/cart">{to('basket.review')}</ButtonLink>
        </div>
      </div>
    );
  }

  const summary = (
    <aside aria-labelledby={`${id}-summary`} className="flex flex-col gap-5 bg-raised p-6" aria-busy={quoting || undefined}>
      <div className="flex items-baseline justify-between gap-4">
        <h2 id={`${id}-summary`} className="t-heading-md">
          {t('summary')}
        </h2>
        <span className="t-small text-muted">{to('basket.from', { house: branch.shortName })}</span>
      </div>
      <ul className="flex flex-col border-t border-line">
        {(quote?.lines ?? []).map((l) => (
          <li key={l.key} className="flex flex-col gap-1 border-b border-line py-3">
            <div className="flex items-baseline justify-between gap-3">
              <span className="t-body">
                <bdi className="tabular">{formatNumber(l.qty, locale)}</bdi> × {l.name}
              </span>
              <span className="t-small tabular">
                <bdi>{money(l.lineTotal)}</bdi>
              </span>
            </div>
            {l.options.length || l.note ? <span className="t-small text-muted">{[...l.options, l.note ? `“${l.note}”` : null].filter(Boolean).join(' · ')}</span> : null}
            {l.problem ? <span className="t-small text-danger">{to(`basket.problems.${l.problem}`)}</span> : null}
          </li>
        ))}
      </ul>
      {quote ? (
        <dl className="flex flex-col gap-2">
          {[
            { k: to('totals.subtotal'), v: money(quote.pricing.subtotal) },
            quote.pricing.discount ? { k: quote.promo?.ok ? to('totals.discountCode', { code: quote.promo.code }) : to('totals.discount'), v: `−${money(quote.pricing.discount)}` } : null,
            quote.pricing.loyaltyDiscount ? { k: to('totals.loyalty'), v: `−${money(quote.pricing.loyaltyDiscount)}` } : null,
            channel === 'delivery' && quote.zone ? { k: to('totals.delivery'), v: money(quote.pricing.deliveryFee) } : null,
            quote.pricing.serviceCharge ? { k: to('totals.service'), v: money(quote.pricing.serviceCharge) } : null,
            quote.pricing.tip ? { k: to('totals.tip'), v: money(quote.pricing.tip) } : null,
          ]
            .filter((r): r is { k: string; v: string } => r !== null)
            .map((r) => (
              <div key={r.k} className="flex items-baseline justify-between gap-4">
                <dt className="t-small text-muted">{r.k}</dt>
                <dd className="t-small tabular">
                  <bdi>{r.v}</bdi>
                </dd>
              </div>
            ))}
          <div className="flex items-baseline justify-between gap-4 border-t border-line pt-3">
            <dt className="t-label">{to('totals.total')}</dt>
            <dd className="t-heading-sm tabular">
              <bdi>{money(quote.pricing.total)}</bdi>
            </dd>
          </div>
          <div className="flex items-baseline justify-between gap-4">
            <dt className="t-small text-muted">{quote.pricing.taxIncluded ? to('totals.vatIncluded', { rate: formatNumber(Math.round(quote.taxRate * 1000) / 10, locale) }) : to('totals.vat')}</dt>
            <dd className="t-small tabular text-muted">
              <bdi>{money(quote.pricing.tax)}</bdi>
            </dd>
          </div>
          {quote.pricing.giftCardAmount ? (
            <>
              <div className="flex items-baseline justify-between gap-4">
                <dt className="t-small text-muted">{to('totals.giftCard')}</dt>
                <dd className="t-small tabular">
                  <bdi>−{money(quote.pricing.giftCardAmount)}</bdi>
                </dd>
              </div>
              <div className="flex items-baseline justify-between gap-4 border-t border-line pt-3">
                <dt className="t-label">{to('totals.due')}</dt>
                <dd className="t-heading-sm tabular">
                  <bdi>{money(quote.pricing.amountDue)}</bdi>
                </dd>
              </div>
            </>
          ) : null}
        </dl>
      ) : null}
      {quote?.timing.promisedAt ? (
        <p className="t-small flex items-center gap-2">
          <Icon name="clock" size={16} />
          {channel === 'delivery' ? t('when.deliveryAt', { time: formatClock(new Date(quote.timing.promisedAt), locale, branch.timeZone) }) : t('when.pickupAt', { time: formatClock(new Date(quote.timing.promisedAt), locale, branch.timeZone) })}
        </p>
      ) : null}
      <p className="t-small text-muted" role="status">
        {quoting ? t('updating') : quoteFailed ? problemText('quote') : ''}
      </p>
    </aside>
  );

  if (placed) {
    return (
      <div className="site-grid gap-y-12 pb-[var(--spacing-section)]">
        <section aria-labelledby={`${id}-paying`} className="col-span-full flex flex-col gap-6 border-t border-ink pt-6 lg:col-span-7">
          <h2 id={`${id}-paying`} className="t-heading-md">
            {t('steps.pay')}
          </h2>
          <PaymentForm
            {...placed.payment}
            onSucceeded={() => {
              cart.clear();
              router.push(placed.href);
            }}
          />
        </section>
        <div className="col-span-full lg:col-span-4 lg:col-start-9">
          <div className="lg:sticky lg:top-24">{summary}</div>
        </div>
      </div>
    );
  }

  return (
    <form
      noValidate
      className="site-grid gap-y-12 pb-[var(--spacing-section)]"
      onSubmit={(e) => {
        e.preventDefault();
        place(e.currentTarget);
      }}
    >
      <BotFields />
      <div className="col-span-full flex flex-col gap-12 lg:col-span-7">
        {/* How */}
        <Section id={`${id}-how`} title={t('steps.how')}>
          <div className="flex flex-wrap gap-2" role="group" aria-label={t('steps.how')}>
            {channels.map((ch) => (
              <Chip key={ch} pressed={channel === ch} onClick={() => cart.setChannel(ch)} disabled={Boolean(placed)}>
                <Icon name={ch === 'delivery' ? 'delivery' : 'bag'} size={18} />
                {to(`channels.${ch}`)}
              </Chip>
            ))}
          </div>
          {channel === 'pickup' ? <p className="t-body">{t('pickupFrom', { house: branch.name, address: branch.address })}</p> : null}
        </Section>

        {/* Where */}
        {channel === 'delivery' ? (
          <Section id={`${id}-where`} title={t('steps.where')}>
            {addresses.length ? (
              <fieldset className="flex flex-col gap-3">
                <legend className="t-label mb-3 text-muted">{t('address.saved')}</legend>
                <div className="grid gap-2 sm:grid-cols-2">
                  {[...addresses.map((a) => ({ id: a.id, title: a.label, body: [a.area, a.street].filter(Boolean).join('، ') })), { id: 'new', title: t('address.new'), body: '' }].map((o) => (
                    <button
                      key={o.id}
                      type="button"
                      aria-pressed={addressId === o.id}
                      onClick={() => {
                        setAddressId(o.id);
                        const a = addresses.find((x) => x.id === o.id);
                        if (a) {
                          setZoneId(a.zoneId);
                          setArea(a.area);
                          setStreet(a.street);
                          setBuilding(a.building ?? '');
                          setFloor(a.floor ?? '');
                          setAddressNotes(a.notes ?? '');
                          setLocation(a.lat != null && a.lng != null ? { lat: a.lat, lng: a.lng } : null);
                        }
                      }}
                      className={cn('flex min-h-16 flex-col items-start justify-center border px-4 py-3 text-start', addressId === o.id ? 'border-ink bg-ink text-bg' : 'border-line hover-capable:hover:border-ink')}
                    >
                      <span className="t-heading-sm">{o.title}</span>
                      {o.body ? <span className={cn('t-small', addressId === o.id ? 'text-bg/80' : 'text-muted')}>{o.body}</span> : null}
                    </button>
                  ))}
                </div>
              </fieldset>
            ) : null}

            <Field id={`${id}-zone`} label={t('zone')} hint={zone ? t('zoneTerms', { fee: money(zone.fee), min: money(zone.minOrder), eta: tu('mins', plural(zone.etaMinutes, locale)) }) : t('zoneHint')} required requiredLabel={tc('a11y.required')}>
              <Select
                id={`${id}-zone`}
                value={zoneId ? `${zoneId}|${zone?.kind === 'area' ? area : ''}` : ''}
                onChange={(e) => {
                  const [z, a] = e.target.value.split('|');
                  setZoneId(z || null);
                  const next = zones.find((x) => x.id === z);
                  setArea(next?.kind === 'area' ? (a ?? '') : '');
                }}
                {...describedBy(`${id}-zone`, { hint: true })}
              >
                <option value="">—</option>
                {zones
                  .filter((z) => z.kind === 'area')
                  .map((z) => (
                    <optgroup key={z.id} label={z.name}>
                      {z.areas.map((a) => (
                        <option key={a} value={`${z.id}|${a}`}>
                          {a}
                        </option>
                      ))}
                    </optgroup>
                  ))}
                {zones
                  .filter((z) => z.kind === 'radius')
                  .map((z) => (
                    <option key={z.id} value={`${z.id}|`}>
                      {z.name}
                    </option>
                  ))}
              </Select>
            </Field>
            {zone?.kind === 'radius' ? (
              <>
                <PinPicker house={{ lat: branch.lat, lng: branch.lng }} radiusKm={zone.radiusKm ?? 0} value={location} onChange={setLocation} />
                <Field id={`${id}-area`} label={t('address.area')} required requiredLabel={tc('a11y.required')} error={err('address.area')}>
                  <Input id={`${id}-area`} value={area} onChange={(e) => setArea(e.target.value)} autoComplete="address-level3" required />
                </Field>
              </>
            ) : null}
            <div className="grid gap-6 sm:grid-cols-2">
              <Field id={`${id}-street`} label={t('address.street')} required requiredLabel={tc('a11y.required')} error={err('address.street')} className="sm:col-span-2">
                <Input id={`${id}-street`} value={street} onChange={(e) => setStreet(e.target.value)} autoComplete="street-address" required {...describedBy(`${id}-street`, { error: Boolean(err('address.street')) })} />
              </Field>
              <Field id={`${id}-building`} label={`${t('address.building')} (${tf('fields.optional')})`}>
                <Input id={`${id}-building`} value={building} onChange={(e) => setBuilding(e.target.value)} />
              </Field>
              <Field id={`${id}-floor`} label={`${t('address.floor')} (${tf('fields.optional')})`}>
                <Input id={`${id}-floor`} value={floor} onChange={(e) => setFloor(e.target.value)} />
              </Field>
              <Field id={`${id}-dir`} label={`${t('address.notes')} (${tf('fields.optional')})`} hint={t('address.notesHint')} className="sm:col-span-2">
                <Input id={`${id}-dir`} value={addressNotes} onChange={(e) => setAddressNotes(e.target.value)} {...describedBy(`${id}-dir`, { hint: true })} />
              </Field>
            </div>
            {user && addressId === 'new' ? (
              <div className="flex flex-col gap-3">
                <Checkbox id={`${id}-save`} checked={saveAddress} onChange={(e) => setSaveAddress(e.target.checked)} label={t('address.save')} />
                {saveAddress ? (
                  <Field id={`${id}-label`} label={t('address.saveLabel')}>
                    <Input id={`${id}-label`} value={saveLabel} maxLength={40} placeholder={t('address.saveLabelPlaceholder')} onChange={(e) => setSaveLabel(e.target.value)} />
                  </Field>
                ) : null}
              </div>
            ) : null}
          </Section>
        ) : null}

        {/* When */}
        <Section id={`${id}-when`} title={t('steps.when')}>
          <div className="grid gap-2 sm:grid-cols-2" role="radiogroup" aria-label={t('steps.when')}>
            <button
              type="button"
              role="radio"
              aria-checked={when.asap}
              disabled={!quote?.timing.asapAvailable}
              onClick={() => setWhen({ asap: true })}
              className={cn('flex min-h-16 flex-col items-start justify-center border px-4 py-3 text-start disabled:cursor-not-allowed disabled:opacity-50', when.asap ? 'border-ink bg-ink text-bg' : 'border-line')}
            >
              <span className="t-heading-sm">{t('when.asap')}</span>
              <span className={cn('t-small', when.asap ? 'text-bg/80' : 'text-muted')}>{quote?.timing.asapAt ? t('when.asapAt', { time: formatClock(new Date(quote.timing.asapAt), locale, branch.timeZone) }) : t('when.asapOff')}</span>
            </button>
            <button
              type="button"
              role="radio"
              aria-checked={!when.asap}
              disabled={!options.some((o) => o.available)}
              onClick={() => pickFirst(chosenDay)}
              className={cn('flex min-h-16 flex-col items-start justify-center border px-4 py-3 text-start disabled:cursor-not-allowed disabled:opacity-50', !when.asap ? 'border-ink bg-ink text-bg' : 'border-line')}
            >
              <span className="t-heading-sm">{t('when.later')}</span>
            </button>
          </div>
          {!when.asap ? (
            <div className="grid gap-4 sm:grid-cols-2">
              <Field id={`${id}-day`} label={t('when.day')}>
                <Select id={`${id}-day`} value={chosenDay} onChange={(e) => pickFirst(e.target.value)}>
                  {days.map((d) => (
                    <option key={d} value={d}>
                      {dayLabel(d)}
                    </option>
                  ))}
                </Select>
              </Field>
              <Field id={`${id}-time`} label={t('when.time')}>
                <Select id={`${id}-time`} value={when.at} onChange={(e) => setWhen({ asap: false, at: e.target.value })}>
                  {options
                    .filter((o) => toDateString(new Date(o.at), branch.timeZone) === chosenDay)
                    .map((o) => (
                      <option key={o.at} value={o.at} disabled={!o.available}>
                        {formatClock(new Date(o.at), locale, branch.timeZone)}
                        {!o.available ? ` — ${t('when.full')}` : !o.servesAll ? ` — ${t('when.partial')}` : ''}
                      </option>
                    ))}
                </Select>
              </Field>
            </div>
          ) : null}
          {quote && !options.length && !quote.timing.asapAvailable ? <p className="t-small text-danger">{t('when.none')}</p> : null}
        </Section>

        {/* Who */}
        <Section id={`${id}-who`} title={t('steps.who')}>
          {user ? (
            <p className="t-small text-muted">{t('contact.signedIn', { name: user.name })}</p>
          ) : (
            <p className="t-small text-muted">
              {t('contact.signInPrompt')}{' '}
              <Link href={{ pathname: '/account/sign-in', query: { next: '/order/checkout' } }} className="underline underline-offset-4">
                {tc('nav.signIn')}
              </Link>
            </p>
          )}
          <div className="grid gap-6 sm:grid-cols-2">
            <Field id={`${id}-name`} label={tf('fields.name')} required requiredLabel={tc('a11y.required')} error={err('name')}>
              <Input id={`${id}-name`} name="name" autoComplete="name" defaultValue={user?.name ?? ''} required {...describedBy(`${id}-name`, { error: Boolean(err('name')) })} />
            </Field>
            <Field id={`${id}-email`} label={tf('fields.email')} required requiredLabel={tc('a11y.required')} error={err('email')}>
              <Input id={`${id}-email`} name="email" type="email" autoComplete="email" value={email} onChange={(e) => setEmail(e.target.value)} required {...describedBy(`${id}-email`, { error: Boolean(err('email')) })} />
            </Field>
            <Field id={`${id}-phone`} label={tf('fields.phone')} hint={tf('fields.phoneHint')} required requiredLabel={tc('a11y.required')} error={err('phone')} className="sm:col-span-2">
              <Input id={`${id}-phone`} name="phone" type="tel" inputMode="tel" autoComplete="tel" defaultValue={user?.phone ?? ''} required {...describedBy(`${id}-phone`, { hint: true, error: Boolean(err('phone')) })} />
            </Field>
            <Field id={`${id}-notes`} label={`${t('contact.notes')} (${tf('fields.optional')})`} className="sm:col-span-2">
              <Textarea id={`${id}-notes`} name="notes" rows={2} maxLength={500} className="min-h-20" />
            </Field>
          </div>
        </Section>

        {/* Codes, cards, points */}
        <Section id={`${id}-extras`} title={t('steps.extras')}>
          <div className="flex flex-col gap-2">
            <Field id={`${id}-promo`} label={t('promo.label')}>
              <div className="flex gap-2">
                <Input id={`${id}-promo`} value={promoInput} onChange={(e) => setPromoInput(e.target.value)} autoCapitalize="characters" dir="ltr" className="flex-1 uppercase" />
                <Button variant="secondary" onClick={() => setPromoCode(promoInput.trim() || null)} disabled={!promoInput.trim()}>
                  {tc('actions.apply')}
                </Button>
              </div>
            </Field>
            {quote?.promo ? (
              quote.promo.ok ? (
                <p className="t-small flex items-center gap-3" role="status">
                  <Icon name="check" size={16} />
                  <span dir="ltr">{t('promo.applied', { code: quote.promo.code, amount: money(quote.promo.discount) })}</span>
                  <button type="button" className="underline underline-offset-4" onClick={() => { setPromoCode(null); setPromoInput(''); }}>
                    {tc('actions.remove')}
                  </button>
                </p>
              ) : (
                <p className="t-small text-danger" role="alert">
                  {quote.promo.reason === 'min_order' ? t('promo.errors.min_order', { amount: money(quote.promo.minOrder ?? 0) }) : t(`promo.errors.${quote.promo.reason}`)}
                </p>
              )
            ) : null}
          </div>
          {giftCardsEnabled ? (
            <div className="flex flex-col gap-2">
              <Field id={`${id}-gift`} label={t('giftCard.label')}>
                <div className="flex gap-2">
                  <Input id={`${id}-gift`} value={giftInput} onChange={(e) => setGiftInput(e.target.value)} placeholder="ZILL-XXXX-XXXX-XXXX" dir="ltr" className="flex-1 uppercase" />
                  <Button variant="secondary" onClick={() => setGiftCode(giftInput.trim() || null)} disabled={!giftInput.trim()}>
                    {tc('actions.apply')}
                  </Button>
                </div>
              </Field>
              {quote?.giftCard ? (
                quote.giftCard.ok ? (
                  <p className="t-small flex items-center gap-3" role="status">
                    <Icon name="gift" size={16} />
                    {t('giftCard.applied', { amount: money(quote.giftCard.applied), remaining: money(quote.giftCard.balance - quote.giftCard.applied) })}
                    <button type="button" className="underline underline-offset-4" onClick={() => { setGiftCode(null); setGiftInput(''); }}>
                      {tc('actions.remove')}
                    </button>
                  </p>
                ) : (
                  <p className="t-small text-danger" role="alert">
                    {t(`giftCard.errors.${quote.giftCard.reason}`)}
                  </p>
                )
              ) : null}
            </div>
          ) : null}
          {loyaltyEnabled ? (
            <div className="flex flex-col gap-2 border-t border-line pt-5">
              <p className="t-heading-sm">{t('loyalty.title')}</p>
              {user && quote?.loyalty ? (
                <>
                  <p className="t-small text-muted">{t('loyalty.balance', { points: tu('points', plural(quote.loyalty.balance, locale)) })}</p>
                  {quote.loyalty.maxPoints > 0 || usePoints ? (
                    <Checkbox
                      id={`${id}-points`}
                      checked={usePoints}
                      onChange={(e) => setUsePoints(e.target.checked)}
                      label={t('loyalty.use', { points: tu('points', plural(usePoints ? quote.loyalty.points : quote.loyalty.maxPoints, locale)), amount: money(usePoints ? quote.loyalty.value : quote.loyalty.maxValue) })}
                    />
                  ) : (
                    <p className="t-small text-muted">{t('loyalty.none')}</p>
                  )}
                </>
              ) : !user ? (
                <p className="t-small text-muted">{t('loyalty.signIn')}</p>
              ) : null}
            </div>
          ) : null}
        </Section>

        {/* Tip */}
        {quote?.tipAllowed ? (
          <Section id={`${id}-tip`} title={t('steps.tip')}>
            <div className="flex flex-wrap gap-2" role="group" aria-label={t('steps.tip')}>
              {tipPresets.map((p) => (
                <Chip key={p} pressed={p === 0 ? !tip : tip?.kind === 'percent' && tip.value === p} onClick={() => { setTip(p === 0 ? null : { kind: 'percent', value: p }); setCustomTip(''); }}>
                  {p === 0 ? t('tip.none') : (
                    <>
                      <span className="tabular">{formatPercent(p, locale)}</span>
                      <span className="t-small opacity-80 tabular">
                        <bdi>{money(Math.round(quote.pricing.subtotal * p))}</bdi>
                      </span>
                    </>
                  )}
                </Chip>
              ))}
            </div>
            <Field id={`${id}-tipc`} label={t('tip.custom')} hint={t('tip.hint')}>
              <Input
                id={`${id}-tipc`}
                inputMode="decimal"
                value={customTip}
                onChange={(e) => {
                  setCustomTip(e.target.value);
                  const minor = parseMoneyInput(e.target.value);
                  setTip(Number.isFinite(minor) && minor > 0 ? { kind: 'amount', value: minor } : null);
                }}
                className="max-w-40 tabular"
                {...describedBy(`${id}-tipc`, { hint: true })}
              />
            </Field>
          </Section>
        ) : null}

        {/* Payment */}
        <Section id={`${id}-pay`} title={t('steps.pay')}>
          {coveredByGift ? (
            <p className="t-body flex items-center gap-3">
              <Icon name="gift" size={20} />
              {t('payment.covered')}
            </p>
          ) : (
            <div className="grid gap-2 sm:grid-cols-2" role="radiogroup" aria-label={t('steps.pay')}>
              {(['card', channel === 'delivery' ? 'cash' : 'pay_at_venue'] as const).map((m) => (
                <button
                  key={m}
                  type="button"
                  role="radio"
                  aria-checked={method === m}
                  disabled={Boolean(placed)}
                  onClick={() => setMethod(m)}
                  className={cn('flex min-h-14 items-center gap-3 border px-4 text-start', method === m ? 'border-ink bg-ink text-bg' : 'border-line hover-capable:hover:border-ink')}
                >
                  <Icon name={m === 'card' ? 'lock' : m === 'cash' ? 'receipt' : 'table'} size={18} />
                  <span className="t-body">{t(`payment.${m}`)}</span>
                </button>
              ))}
            </div>
          )}

          <div className="flex flex-col gap-4">
              {quote && !quote.canPlace ? (
                <ul className="flex flex-col gap-1" aria-live="polite">
                  {[...new Set(quote.problems)].filter((p) => p !== 'empty').map((p) => (
                    <li key={p} className="t-small text-danger">
                      {problemText(p)}
                    </li>
                  ))}
                </ul>
              ) : null}
              {formError ? (
                <p role="alert" className="t-small text-danger">
                  {formError}
                </p>
              ) : null}
              <Button type="submit" size="lg" icon="arrow" disabled={placing || !quote?.canPlace || quoting} className="w-full sm:w-auto sm:self-start">
                {placing ? t('placing') : method === 'card' && !coveredByGift ? t('placePay') : t('place')}
              </Button>
              <p className="t-small text-muted">
                {t('terms')}{' '}
                <Link href="/legal/terms" className="underline underline-offset-4">
                  {t('termsLink')}
                </Link>
              </p>
          </div>
        </Section>
      </div>

      <div className="col-span-full lg:col-span-4 lg:col-start-9">
        <div className="lg:sticky lg:top-24">{summary}</div>
      </div>
    </form>
  );
}
