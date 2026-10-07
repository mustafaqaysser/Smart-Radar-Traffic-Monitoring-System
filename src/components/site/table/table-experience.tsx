'use client';

import Image from 'next/image';
import { useLocale, useTranslations } from 'next-intl';
import { useEffect, useId, useMemo, useRef, useState, useTransition } from 'react';
import { Icon } from '@/components/brand/icon';
import { DishMarks, ProfileMark } from '@/components/site/menu/badges';
import { QuickAdd } from '@/components/site/menu/quick-add';
import { useProfileLine } from '@/components/site/menu/use-profile-line';
import { BotFields } from '@/components/site/forms/bot-fields';
import { PaymentForm } from '@/components/site/payment-form';
import { Button } from '@/components/site/ui/button';
import { Dialog } from '@/components/site/ui/dialog';
import { Chip, Field, Input, Textarea } from '@/components/site/ui/field';
import { Quantity } from '@/components/site/ui/quantity';
import { toast } from '@/components/site/ui/toast';
import { Link } from '@/i18n/navigation';
import { callFromTable, placeTableOrder, quoteTableOrder } from '@/lib/actions/table';
import { tableBasket, useTableBasket } from '@/lib/cart/table-basket';
import { formatList, formatMoney, formatNumber } from '@/lib/i18n/format';
import { plural } from '@/lib/i18n/plural';
import type { DietProfile } from '@/lib/menu/filter';
import { ALLERGENS, DIETARY_TAGS } from '@/lib/menu/tags';
import type { ItemView } from '@/lib/menu/view';
import type { OrderMenu } from '@/lib/order/menu-data';
import type { QuoteView } from '@/lib/order/types';
import type { StartedPayment } from '@/lib/server/payments';
import type { TableActivity } from '@/lib/server/table';
import { cn } from '@/lib/utils/cn';

interface TableExperienceProps {
  code: string;
  house: { slug: string; name: string };
  menus: OrderMenu[];
  items: Record<string, ItemView>;
  canOrder: boolean;
  profile: DietProfile | null;
  initialActivity: TableActivity;
}

const ACTIVE = new Set(['pending_payment', 'placed', 'accepted', 'preparing', 'ready']);

/** Live calls and orders for the table (Server-Sent Events; the connection renews itself). */
function useTableActivity(code: string, initial: TableActivity): [TableActivity, (a: TableActivity) => void] {
  const [activity, setActivity] = useState(initial);
  useEffect(() => {
    const source = new EventSource(`/api/tables/${encodeURIComponent(code)}/stream`);
    source.addEventListener('activity', (e) => setActivity(JSON.parse((e as MessageEvent<string>).data) as TableActivity));
    return () => source.close();
  }, [code]);
  return [activity, setActivity];
}

/**
 * The table's own menu: what is served now, one tap to add, a basket that goes straight to the kitchen, and
 * the waiter or the bill a tap away — every request answered live on the guest's screen.
 */
export function TableExperience({ code, house, menus, items, canOrder, profile, initialActivity }: TableExperienceProps) {
  const t = useTranslations('table');
  const ta = useTranslations('account.status.order');
  const tm = useTranslations('menu');
  const tc = useTranslations('common');
  const tf = useTranslations('forms');
  const locale = useLocale();
  const lines = useTableBasket(code);
  const [activity, setActivity] = useTableActivity(code, initialActivity);
  const [adding, setAdding] = useState<ItemView | null>(null);
  const [basketOpen, setBasketOpen] = useState(false);
  const [calling, startCalling] = useTransition();
  const [notice, setNotice] = useState<string | null>(null);
  const profileLine = useProfileLine(profile);
  const count = lines.reduce((n, l) => n + l.qty, 0);
  const estimate = lines.reduce((n, l) => {
    const item = items[l.slug];
    if (!item) return n;
    const extras = l.optionIds.reduce((sum, id) => sum + (item.modifierGroups.flatMap((g) => g.options).find((o) => o.id === id)?.priceDelta ?? 0), 0);
    return n + (item.price + extras) * l.qty;
  }, 0);

  const dietLabels = Object.fromEntries(DIETARY_TAGS.map((d) => [d, tc(`dietary.${d}`)]));
  const allergenLabels = Object.fromEntries(ALLERGENS.map((a) => [a, tc(`allergens.${a}`)]));

  // The newest call of each kind that is still being handled.
  const live = (kind: 'waiter' | 'bill') => activity.requests.find((r) => r.kind === kind && r.status !== 'done') ?? null;
  const call = (kind: 'waiter' | 'bill') =>
    startCalling(async () => {
      const res = await callFromTable({ code, kind });
      if (!res.ok) {
        toast(t.has(`errors.${res.error}`) ? t(`errors.${res.error}`) : tf.has(`errors.${res.error}`) ? tf(`errors.${res.error}`) : tf('errors.unknown'));
        return;
      }
      setActivity(res.data.activity);
      setNotice(t(`requests.${kind}.${res.data.repeated ? 'repeated' : 'open'}`));
    });
  const requestLine = (['waiter', 'bill'] as const)
    .map((kind) => {
      const r = live(kind);
      return r ? t(`requests.${kind}.${r.status === 'acknowledged' ? 'acknowledged' : 'open'}`) : null;
    })
    .filter(Boolean)
    .join(' ');

  return (
    <div className="flex flex-col gap-10 pb-32">
      <section className="flex flex-col gap-4" aria-label={t('actions.waiter')}>
        <div className="grid grid-cols-2 gap-3">
          {(['waiter', 'bill'] as const).map((kind) => {
            const r = live(kind);
            return (
              <button
                key={kind}
                type="button"
                disabled={calling}
                onClick={() => call(kind)}
                aria-pressed={Boolean(r)}
                className={cn(
                  'flex min-h-20 flex-col items-start justify-between gap-2 border p-4 text-start transition-colors',
                  r ? 'border-ink bg-ink text-bg' : 'border-ink hover-capable:hover:bg-raised',
                )}
              >
                <Icon name={kind === 'waiter' ? 'bell' : 'receipt'} size={22} />
                <span className="t-heading-sm">{t(`actions.${kind}`)}</span>
              </button>
            );
          })}
        </div>
        <p role="status" aria-live="polite" className="t-body min-h-[1.6em]">
          {requestLine || notice}
        </p>
      </section>

      {activity.orders.length ? (
        <section aria-labelledby="table-orders" className="flex flex-col gap-3">
          <h2 id="table-orders" tabIndex={-1} className="t-label text-muted outline-none">
            {t('orders.title')}
          </h2>
          <ul className="border-t border-ink">
            {activity.orders.map((o) => (
              <li key={o.number} className="flex flex-wrap items-baseline justify-between gap-3 border-b border-line py-3">
                <span className="t-heading-sm">
                  <bdi>{o.number}</bdi>
                </span>
                <span className={cn('t-label', ACTIVE.has(o.status) ? 'text-accent' : 'text-muted')}>{ta(o.status)}</span>
                <Link href={o.trackPath} className="t-small underline decoration-line underline-offset-4">
                  {t('sent.track')}
                </Link>
              </li>
            ))}
          </ul>
        </section>
      ) : null}

      {!canOrder ? (
        <p className="t-body flex items-start gap-3 border-s-2 border-warning ps-4">
          <Icon name="clock" size={20} className="mt-1 shrink-0" />
          {t('closed')}
        </p>
      ) : null}

      <section aria-labelledby="table-menu" className="flex flex-col gap-6">
        <h2 id="table-menu" className="t-display-md">
          {t('menu.title')}
        </h2>
        {menus.length > 1 ? (
          <nav aria-label={t('menu.menus')} className="-mx-[var(--spacing-margin)] flex gap-2 overflow-x-auto px-[var(--spacing-margin)] pb-1 [scrollbar-width:none]">
            {menus.map((m) => (
              <a key={m.slug} href={`#menu-${m.slug}`} className="inline-flex min-h-11 shrink-0 items-center gap-2 rounded-pill border border-line px-4 text-[0.9375rem] whitespace-nowrap hover-capable:hover:border-ink">
                {m.servingNow ? <span aria-hidden="true" className="size-2 rounded-full bg-success" /> : null}
                {m.name}
              </a>
            ))}
          </nav>
        ) : null}
        {menus.map((m) => (
          <section key={m.slug} id={`menu-${m.slug}`} aria-labelledby={`menu-${m.slug}-title`} className="flex scroll-mt-6 flex-col gap-6">
            <div className="flex flex-wrap items-baseline justify-between gap-2 border-b border-ink pb-2">
              <h3 id={`menu-${m.slug}-title`} className="t-heading-lg">
                {m.name}
              </h3>
              {m.window ? <span className="t-small tabular text-muted">{m.window}</span> : null}
            </div>
            {m.categories.map((c) => (
              <div key={c.slug} className="flex flex-col">
                <h4 className="t-label mb-1 text-muted">{c.name}</h4>
                <ul>
                  {c.items.map((slug) => {
                    const item = items[slug];
                    if (!item) return null;
                    const blocked = !item.available || item.soldOut || !item.orderable;
                    const contains = item.allergens.length ? `${tc('allergens.contains')} ${formatList(item.allergens.map((a) => allergenLabels[a] ?? a), locale)}` : tc('allergens.none');
                    return (
                      <li key={slug} className={cn('grid grid-cols-[auto_1fr_auto] gap-x-4 gap-y-1.5 border-b border-line py-4', blocked && 'opacity-70')}>
                        {item.image ? (
                          <div className="arch-3x4 relative row-span-4 aspect-[3/4] w-16 overflow-hidden bg-surface">
                            <Image src={item.image.src} alt="" fill sizes="64px" quality={55} className="object-cover" style={{ objectPosition: `${item.image.focalX * 100}% ${item.image.focalY * 100}%` }} />
                          </div>
                        ) : (
                          <span className="row-span-4 w-16" />
                        )}
                        <p className="t-heading-sm col-start-2">{item.name}</p>
                        <p className="t-small col-start-2 line-clamp-2 text-muted">{item.description}</p>
                        <DishMarks className="col-start-2" dietary={item.dietary} allergens={item.allergens} spice={item.spice} labels={{ diet: dietLabels, allergen: allergenLabels, spice: tc(`spice.${item.spice}`), contains }} />
                        <ProfileMark line={profileLine(item)} className="col-start-2" />
                        <div className="col-start-3 row-span-4 row-start-1 flex flex-col items-end gap-2">
                          <span className="t-body tabular">
                            <bdi>{formatMoney(item.price, locale)}</bdi>
                          </span>
                          {item.soldOut ? (
                            <span className="t-label text-danger">{tm('item.soldOut')}</span>
                          ) : canOrder && !blocked ? (
                            <button
                              type="button"
                              onClick={() => (item.modifierGroups.length ? setAdding(item) : (tableBasket.add(code, { slug, qty: 1, optionIds: [], note: '' }), toast(`${item.name} — ${tm('item.added')}`)))}
                              className="inline-flex size-11 items-center justify-center rounded-full border border-ink transition-colors hover-capable:hover:bg-ink hover-capable:hover:text-bg"
                              aria-label={t('menu.add', { name: item.name })}
                            >
                              <Icon name="plus" size={20} />
                            </button>
                          ) : null}
                        </div>
                      </li>
                    );
                  })}
                </ul>
              </div>
            ))}
          </section>
        ))}
      </section>

      {canOrder && count > 0 ? (
        <div className="fixed inset-x-0 bottom-0 z-30 border-t border-line bg-bg/95 px-[var(--spacing-margin)] pt-3 pb-[max(0.75rem,env(safe-area-inset-bottom))] backdrop-blur">
          <Button size="lg" className="w-full" icon="arrow" onClick={() => setBasketOpen(true)}>
            {`${t('basket.open', { count: formatNumber(count, locale) })} · ${formatMoney(estimate, locale)}`}
          </Button>
        </div>
      ) : null}

      <QuickAdd item={adding} branchSlug={house.slug} branchName={house.name} onClose={() => setAdding(null)} onAdd={(line) => tableBasket.add(code, line)} />
      <BasketSheet
        open={basketOpen}
        onClose={() => setBasketOpen(false)}
        code={code}
        items={items}
        onPlaced={(a) => {
          setActivity(a);
          tableBasket.clear(code);
        }}
      />
    </div>
  );
}

function BasketSheet({ open, onClose, code, items, onPlaced }: { open: boolean; onClose: () => void; code: string; items: Record<string, ItemView>; onPlaced: (a: TableActivity) => void }) {
  const t = useTranslations('table');
  const to = useTranslations('order');
  const tf = useTranslations('forms');
  const tc = useTranslations('common');
  const locale = useLocale();
  const id = useId();
  const lines = useTableBasket(code);
  const [quote, setQuote] = useState<QuoteView | null>(null);
  const [pay, setPay] = useState<'pay_at_venue' | 'card'>('pay_at_venue');
  const [name, setName] = useState('');
  const [notes, setNotes] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [placed, setPlaced] = useState<{ number: string; trackPath: string; payment: StartedPayment | null } | null>(null);
  const [pending, start] = useTransition();
  const formRef = useRef<HTMLFormElement>(null);
  const payload = useMemo(() => lines.map((l) => ({ slug: l.slug, qty: l.qty, optionIds: l.optionIds, note: l.note })), [lines]);
  const money = (v: number) => formatMoney(v, locale);

  // Re-price whenever the basket changes while the sheet is open.
  useEffect(() => {
    if (!open || !payload.length) return undefined;
    let live = true;
    const timer = window.setTimeout(async () => {
      const res = await quoteTableOrder({ code, lines: payload, locale: locale as 'ar' | 'en' });
      if (live && res.ok) setQuote(res.data);
    }, 250);
    return () => {
      live = false;
      window.clearTimeout(timer);
    };
  }, [open, payload, code, locale]);

  // Once an order is sent the basket button is gone, so focus moves to the table's orders (and their status).
  const focusOrders = () => window.setTimeout(() => document.getElementById('table-orders')?.focus(), 60);
  const close = () => {
    onClose();
    if (placed && !placed.payment) {
      setPlaced(null);
      focusOrders();
    }
  };

  const problems = quote?.problems.filter((p) => p !== 'empty') ?? [];
  const lineProblem = quote?.lines.find((l) => l.problem);

  return (
    <Dialog open={open} onClose={close} title={placed ? t('sent.title') : t('basket.title')} closeLabel={tc('a11y.close')} variant="sheet">
      {placed ? (
        <div className="flex flex-col gap-6">
          {placed.payment ? (
            <>
              <p className="t-body">{t('sent.pay', { number: placed.number })}</p>
              <PaymentForm
                {...placed.payment}
                onSucceeded={() => {
                  setPlaced({ ...placed, payment: null });
                  toast(t('sent.body', { number: placed.number }));
                }}
              />
            </>
          ) : (
            <>
              <p className="t-body-lg flex items-start gap-3">
                <Icon name="check" size={24} className="mt-1 shrink-0" />
                {t('sent.body', { number: placed.number })}
              </p>
              <div className="flex flex-wrap gap-3">
                <Button
                  icon={null}
                  onClick={() => {
                    setPlaced(null);
                    onClose();
                    focusOrders();
                  }}
                >
                  {t('sent.again')}
                </Button>
                <Link href={placed.trackPath} className="t-small inline-flex min-h-11 items-center underline decoration-line underline-offset-4">
                  {t('sent.track')}
                </Link>
              </div>
            </>
          )}
        </div>
      ) : !lines.length ? (
        <p className="t-body text-muted">{t('basket.empty')}</p>
      ) : (
        <form
          ref={formRef}
          noValidate
          className="flex flex-col gap-6"
          onSubmit={(e) => {
            e.preventDefault();
            const data = new FormData(e.currentTarget);
            setError(null);
            start(async () => {
              const res = await placeTableOrder({
                code,
                lines: payload,
                name,
                notes,
                paymentMethod: pay,
                locale: locale as 'ar' | 'en',
                company_website: String(data.get('company_website') ?? ''),
                rendered_at: String(data.get('rendered_at') ?? ''),
              });
              if (!res.ok) {
                setError(t.has(`errors.${res.error}`) ? t(`errors.${res.error}`) : tf.has(`errors.${res.error}`) ? tf(`errors.${res.error}`) : tf('errors.unknown'));
                return;
              }
              onPlaced(res.data.activity);
              setPlaced({ number: res.data.number, trackPath: res.data.trackPath, payment: res.data.payment });
            });
          }}
        >
          <BotFields />
          <ul className="border-t border-ink">
            {lines.map((l) => {
              const item = items[l.slug];
              const quoted = quote?.lines.find((q) => q.key === l.key);
              const name = item?.name ?? l.slug;
              return (
                <li key={l.key} className="flex flex-wrap items-center justify-between gap-3 border-b border-line py-3">
                  <div className="flex min-w-0 flex-1 flex-col">
                    <span className="t-heading-sm">{name}</span>
                    {quoted?.options.length || l.note ? <span className="t-small text-muted">{[...(quoted?.options ?? []), l.note ? `“${l.note}”` : null].filter(Boolean).join(' · ')}</span> : null}
                    {quoted?.problem ? <span className="t-small text-danger">{to(`basket.problems.${quoted.problem}`)}</span> : null}
                  </div>
                  <Quantity size="sm" value={l.qty} min={0} onChange={(n) => tableBasket.setQty(code, l.key, n)} label={t('basket.remove', { name })} />
                  <span className="t-body w-20 text-end tabular">
                    <bdi>{quoted ? money(quoted.lineTotal) : ''}</bdi>
                  </span>
                </li>
              );
            })}
          </ul>

          <Field id={`${id}-name`} label={t('basket.name')} hint={t('basket.nameHint')}>
            <Input id={`${id}-name`} value={name} maxLength={80} autoComplete="given-name" onChange={(e) => setName(e.target.value)} />
          </Field>
          <Field id={`${id}-notes`} label={t('basket.notes')}>
            <Textarea id={`${id}-notes`} value={notes} rows={2} maxLength={300} className="min-h-20" onChange={(e) => setNotes(e.target.value)} />
          </Field>

          <fieldset className="flex flex-col gap-3">
            <legend className="t-label mb-3 text-muted">{t('basket.pay')}</legend>
            <div className="flex flex-wrap gap-2">
              <Chip pressed={pay === 'pay_at_venue'} onClick={() => setPay('pay_at_venue')}>
                <Icon name="receipt" size={16} />
                {t('basket.payAtTable')}
              </Chip>
              <Chip pressed={pay === 'card'} onClick={() => setPay('card')}>
                <Icon name="lock" size={16} />
                {t('basket.payNow')}
              </Chip>
            </div>
          </fieldset>

          {quote ? (
            <dl className="flex flex-col gap-2 border-t border-line pt-4">
              {[
                { k: to('totals.subtotal'), v: money(quote.pricing.subtotal) },
                quote.pricing.serviceCharge ? { k: to('totals.service'), v: money(quote.pricing.serviceCharge) } : null,
                quote.pricing.tip ? { k: to('totals.tip'), v: money(quote.pricing.tip) } : null,
              ]
                .filter((r): r is { k: string; v: string } => r !== null)
                .map((r) => (
                  <div key={r.k} className="flex justify-between gap-4">
                    <dt className="t-small text-muted">{r.k}</dt>
                    <dd className="t-small tabular">
                      <bdi>{r.v}</bdi>
                    </dd>
                  </div>
                ))}
              <div className="flex justify-between gap-4 border-t border-line pt-2">
                <dt className="t-label">{to('totals.total')}</dt>
                <dd className="t-heading-sm tabular">
                  <bdi>{money(quote.pricing.total)}</bdi>
                </dd>
              </div>
              <div className="flex justify-between gap-4">
                <dt className="t-small text-muted">{quote.pricing.taxIncluded ? to('totals.vatIncluded', { rate: formatNumber(Math.round(quote.taxRate * 1000) / 10, locale) }) : to('totals.vat')}</dt>
                <dd className="t-small tabular text-muted">
                  <bdi>{money(quote.pricing.tax)}</bdi>
                </dd>
              </div>
            </dl>
          ) : null}

          {problems.length && !lineProblem ? (
            <p role="alert" className="t-small text-danger">
              {t('basket.problem', { reason: problems.map((p) => (to.has(`checkout.problems.${p}`) ? to(`checkout.problems.${p}`) : p)).join(' ') })}
            </p>
          ) : null}
          {error ? (
            <p role="alert" className="t-small text-danger">
              {error}
            </p>
          ) : null}
          <Button type="submit" size="lg" icon="arrow" disabled={pending || !quote?.canPlace} className="w-full">
            {pending ? t('basket.sending') : t('basket.send', { total: quote ? money(quote.pricing.total) : '…' })}
          </Button>
          <p className="t-small text-muted">{tc('units.items', plural(lines.reduce((n, l) => n + l.qty, 0), locale))}</p>
        </form>
      )}
    </Dialog>
  );
}
