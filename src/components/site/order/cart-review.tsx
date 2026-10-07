'use client';

import Image from 'next/image';
import { useRouter } from 'next/navigation';
import { useLocale, useTranslations } from 'next-intl';
import { useState, useTransition } from 'react';
import { Icon } from '@/components/brand/icon';
import { QuickAdd } from '@/components/site/menu/quick-add';
import { Button, ButtonLink } from '@/components/site/ui/button';
import { Quantity } from '@/components/site/ui/quantity';
import { toast } from '@/components/site/ui/toast';
import { selectBranch } from '@/lib/actions/site';
import { cart, useCart } from '@/lib/cart/store';
import { formatMoney } from '@/lib/i18n/format';
import { plural } from '@/lib/i18n/plural';
import type { ItemView } from '@/lib/menu/view';
import { basketLines } from './basket-lines';

interface CartReviewProps {
  items: Record<string, ItemView>;
  upsell: string[];
  branch: { slug: string; name: string };
  houses: Record<string, string>;
}

/** The basket in full: quantities, choices and notes, a few things to complete the meal, then checkout. */
export function CartReview({ items, upsell, branch, houses }: CartReviewProps) {
  const t = useTranslations('order.basket');
  const tm = useTranslations('menu.item');
  const tu = useTranslations('common.units');
  const locale = useLocale();
  const router = useRouter();
  const [pending, start] = useTransition();
  const [adding, setAdding] = useState<ItemView | null>(null);
  const c = useCart();
  const lines = basketLines(c.lines, items);
  const count = c.lines.reduce((n, l) => n + l.qty, 0);
  const subtotal = lines.reduce((n, l) => n + l.total, 0);
  const elsewhere = c.lines.length > 0 && c.branchSlug !== null && c.branchSlug !== branch.slug;
  const problems = lines.filter((l) => !l.item || l.item.soldOut || !l.item.available);
  const inBasket = new Set(c.lines.map((l) => l.slug));
  const suggestions = upsell.filter((slug) => !inBasket.has(slug) && items[slug] && !items[slug].soldOut && items[slug].available).slice(0, 4);

  if (!c.lines.length) {
    return (
      <div className="site-grid pb-[var(--spacing-section)]">
        <div className="col-span-full flex flex-col items-start gap-5 border-t border-ink pt-8 lg:col-span-7">
          <p className="t-heading-lg">{t('empty')}</p>
          <p className="t-body text-muted">{t('emptyBody')}</p>
          <ButtonLink href="/order">{t('browse')}</ButtonLink>
        </div>
      </div>
    );
  }

  const quickAdd = (item: ItemView) => {
    if (item.modifierGroups.length) {
      setAdding(item);
      return;
    }
    cart.add(item.slug, 1);
    toast(t('added', { dish: item.name }));
  };

  return (
    <div className="site-grid gap-y-14 pb-[var(--spacing-section)]">
      <section aria-labelledby="cart-lines" className="col-span-full flex flex-col gap-6 lg:col-span-7">
        <h2 id="cart-lines" className="sr-only">
          {t('title')}
        </h2>
        {elsewhere ? (
          <div className="flex flex-col gap-3 border-s-2 border-warning ps-4">
            <p className="t-body">{t('elsewhere', { house: houses[c.branchSlug as string] ?? '' })}</p>
            <div className="flex flex-wrap gap-2">
              <Button
                size="sm"
                variant="secondary"
                disabled={pending}
                onClick={() =>
                  start(async () => {
                    const res = await selectBranch(c.branchSlug as string);
                    if (res.ok) router.refresh();
                  })
                }
              >
                {t('goTo', { house: houses[c.branchSlug as string] ?? '' })}
              </Button>
            </div>
          </div>
        ) : null}
        <ul className="border-t border-ink">
          {lines.map(({ line, item, total, options }) => (
            <li key={line.key} className="grid grid-cols-[auto_1fr_auto] items-start gap-x-4 gap-y-3 border-b border-line py-6">
              {item?.image ? (
                <div className="arch-3x4 relative row-span-2 aspect-[3/4] w-16 overflow-hidden bg-surface sm:w-20">
                  <Image src={item.image.src} alt="" fill sizes="80px" quality={55} className="object-cover" style={{ objectPosition: `${item.image.focalX * 100}% ${item.image.focalY * 100}%` }} />
                </div>
              ) : (
                <span className="row-span-2" />
              )}
              <div className="flex min-w-0 flex-col gap-1">
                <p className="t-heading-sm">{item?.name ?? line.slug}</p>
                {options.length ? <p className="t-small text-muted">{options.join(' · ')}</p> : null}
                {line.note ? <p className="t-small text-muted">“{line.note}”</p> : null}
                {!item ? <p className="t-small text-danger">{t('problems.unknown')}</p> : item.soldOut ? <p className="t-small text-danger">{t('problems.sold_out')}</p> : !item.available ? <p className="t-small text-danger">{t('problems.unavailable')}</p> : null}
              </div>
              <span className="t-body tabular text-end">
                <bdi>{formatMoney(total, locale)}</bdi>
              </span>
              <div className="col-start-2 col-end-4 flex items-center justify-between gap-4">
                <Quantity size="sm" value={line.qty} onChange={(n) => cart.setQty(line.key, n)} label={t('quantity', { dish: item?.name ?? line.slug })} />
                <button type="button" onClick={() => cart.remove(line.key)} className="t-small inline-flex min-h-10 items-center gap-2 text-muted hover-capable:hover:text-ink">
                  <Icon name="trash" size={16} />
                  <span aria-hidden="true">{t('removeShort')}</span>
                  <span className="sr-only">{t('remove', { dish: item?.name ?? line.slug })}</span>
                </button>
              </div>
            </li>
          ))}
        </ul>
        {problems.length ? (
          <Button variant="secondary" size="sm" className="self-start" onClick={() => problems.forEach((p) => cart.remove(p.line.key))}>
            {t('removeProblems')}
          </Button>
        ) : null}
      </section>

      <aside className="col-span-full lg:col-span-4 lg:col-start-9 lg:row-span-2">
        <div className="flex flex-col gap-5 bg-raised p-6 lg:sticky lg:top-28">
          <p className="t-label text-muted">{t('from', { house: houses[c.branchSlug ?? branch.slug] ?? branch.name })}</p>
          <div className="flex items-baseline justify-between gap-4 border-t border-line pt-4">
            <span className="t-body">{tu('items', plural(count, locale))}</span>
            <span className="t-heading-md tabular">
              <bdi>{formatMoney(subtotal, locale)}</bdi>
            </span>
          </div>
          <p className="t-small text-muted">{t('feesNote')}</p>
          {elsewhere ? (
            <Button size="lg" className="w-full" disabled>
              {t('checkout')}
            </Button>
          ) : (
            <ButtonLink href="/order/checkout" size="lg" className="w-full">
              {t('checkout')}
            </ButtonLink>
          )}
          <ButtonLink href="/order" variant="quiet" icon={null} className="self-center">
            {t('addMore')}
          </ButtonLink>
        </div>
      </aside>

      {suggestions.length ? (
        <section aria-labelledby="cart-upsell" className="col-span-full flex flex-col gap-6 lg:col-span-7">
          <div className="flex flex-col gap-2">
            <h2 id="cart-upsell" className="t-heading-lg">
              {t('upsellTitle')}
            </h2>
            <p className="t-body text-muted">{t('upsellBody')}</p>
          </div>
          <ul className="grid grid-cols-2 gap-[var(--spacing-gutter)] sm:grid-cols-4">
            {suggestions.map((slug) => {
              const item = items[slug] as ItemView;
              return (
                <li key={slug} className="flex flex-col gap-3">
                  <div className="arch-3x4 relative aspect-[3/4] overflow-hidden bg-surface">
                    {item.image ? <Image src={item.image.src} alt="" fill sizes="(min-width: 640px) 15vw, 45vw" quality={55} className="object-cover" style={{ objectPosition: `${item.image.focalX * 100}% ${item.image.focalY * 100}%` }} /> : null}
                  </div>
                  <p className="t-body">{item.name}</p>
                  <div className="flex items-center justify-between gap-2">
                    <span className="t-small tabular">
                      <bdi>{formatMoney(item.price, locale)}</bdi>
                    </span>
                    <button type="button" onClick={() => quickAdd(item)} className="inline-flex size-10 items-center justify-center rounded-full border border-ink hover-capable:hover:bg-ink hover-capable:hover:text-bg" aria-label={tm('addNamed', { name: item.name })}>
                      <Icon name="plus" size={18} />
                    </button>
                  </div>
                </li>
              );
            })}
          </ul>
        </section>
      ) : null}

      <QuickAdd item={adding} branchSlug={c.branchSlug ?? branch.slug} branchName={houses[c.branchSlug ?? branch.slug] ?? branch.name} onClose={() => setAdding(null)} />
    </div>
  );
}
