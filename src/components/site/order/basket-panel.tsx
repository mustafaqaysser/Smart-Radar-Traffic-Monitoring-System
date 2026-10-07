'use client';

import { useRouter } from 'next/navigation';
import { useLocale, useTranslations } from 'next-intl';
import { useTransition } from 'react';
import { Icon } from '@/components/brand/icon';
import { ButtonLink, Button } from '@/components/site/ui/button';
import { Quantity } from '@/components/site/ui/quantity';
import { selectBranch } from '@/lib/actions/site';
import { cart, useCart } from '@/lib/cart/store';
import { formatMoney } from '@/lib/i18n/format';
import { plural } from '@/lib/i18n/plural';
import type { ItemView } from '@/lib/menu/view';
import { cn } from '@/lib/utils/cn';
import { basketLines } from './basket-lines';

interface BasketPanelProps {
  items: Record<string, ItemView>;
  branch: { slug: string; name: string };
  /** Names of the other houses, for a basket started elsewhere. */
  houses: Record<string, string>;
  className?: string;
}

/** The order taking shape beside the menu: lines with quantities, subtotal and the way to checkout. */
export function BasketPanel({ items, branch, houses, className }: BasketPanelProps) {
  const t = useTranslations('order.basket');
  const tu = useTranslations('common.units');
  const locale = useLocale();
  const router = useRouter();
  const [pending, start] = useTransition();
  const c = useCart();
  const lines = basketLines(c.lines, items);
  const count = c.lines.reduce((n, l) => n + l.qty, 0);
  const subtotal = lines.reduce((n, l) => n + l.total, 0);
  const elsewhere = c.lines.length > 0 && c.branchSlug !== null && c.branchSlug !== branch.slug;

  return (
    <section aria-labelledby="basket-title" className={cn('flex flex-col gap-5 bg-raised p-6', className)}>
      <header className="flex items-baseline justify-between gap-4">
        <h2 id="basket-title" className="t-heading-md">
          {t('title')}
        </h2>
        {count ? <span className="t-small text-muted">{tu('items', plural(count, locale))}</span> : null}
      </header>

      {elsewhere ? (
        <div className="flex flex-col gap-3 border-s-2 border-warning ps-4">
          <p className="t-small">{t('elsewhere', { house: houses[c.branchSlug as string] ?? c.branchSlug ?? '' })}</p>
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
              {t('goTo', { house: houses[c.branchSlug as string] ?? c.branchSlug ?? '' })}
            </Button>
            <Button size="sm" variant="quiet" onClick={() => cart.replace([], branch.slug, c.channel)}>
              {t('startHere', { house: branch.name })}
            </Button>
          </div>
        </div>
      ) : null}

      {lines.length === 0 ? (
        <div className="flex flex-col gap-2 border-t border-line pt-5">
          <p className="t-body">{t('empty')}</p>
          <p className="t-small text-muted">{t('emptyBody')}</p>
        </div>
      ) : (
        <ul className="flex flex-col border-t border-line">
          {lines.map(({ line, item, total, options }) => (
            <li key={line.key} className="flex flex-col gap-2 border-b border-line py-4">
              <div className="flex items-baseline justify-between gap-3">
                <p className="t-body min-w-0">{item?.name ?? line.slug}</p>
                <span className="t-small tabular shrink-0">
                  <bdi>{formatMoney(total, locale)}</bdi>
                </span>
              </div>
              {options.length || line.note ? <p className="t-small text-muted">{[...options, line.note ? `“${line.note}”` : null].filter(Boolean).join(' · ')}</p> : null}
              {!item ? <p className="t-small text-danger">{t('problems.unknown')}</p> : item.soldOut ? <p className="t-small text-danger">{t('problems.sold_out')}</p> : null}
              <div className="flex items-center justify-between gap-3">
                <Quantity size="sm" value={line.qty} min={1} onChange={(n) => cart.setQty(line.key, n)} label={t('quantity', { dish: item?.name ?? line.slug })} />
                <button type="button" onClick={() => cart.remove(line.key)} className="inline-flex size-9 items-center justify-center text-muted hover-capable:hover:text-ink" aria-label={t('remove', { dish: item?.name ?? line.slug })}>
                  <Icon name="trash" size={18} />
                </button>
              </div>
            </li>
          ))}
        </ul>
      )}

      {lines.length ? (
        <div className="flex flex-col gap-4">
          <div className="flex items-baseline justify-between">
            <span className="t-label text-muted">{t('subtotal')}</span>
            <span className="t-heading-sm tabular">
              <bdi>{formatMoney(subtotal, locale)}</bdi>
            </span>
          </div>
          <p className="t-small text-muted">{t('feesNote')}</p>
          <ButtonLink href="/order/checkout" size="lg" className="w-full">
            {t('checkout')}
          </ButtonLink>
          <ButtonLink href="/order/cart" variant="quiet" icon={null} className="self-center">
            {t('review')}
          </ButtonLink>
        </div>
      ) : null}
    </section>
  );
}
