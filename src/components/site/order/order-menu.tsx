'use client';

import Image from 'next/image';
import { useLocale, useTranslations } from 'next-intl';
import { useState } from 'react';
import { Icon } from '@/components/brand/icon';
import { DishMarks } from '@/components/site/menu/badges';
import { QuickAdd } from '@/components/site/menu/quick-add';
import { toast } from '@/components/site/ui/toast';
import { Link } from '@/i18n/navigation';
import { cart, useCart } from '@/lib/cart/store';
import { ALLERGENS, DIETARY_TAGS } from '@/lib/menu/tags';
import { formatList, formatMoney } from '@/lib/i18n/format';
import { plural } from '@/lib/i18n/plural';
import type { ItemView } from '@/lib/menu/view';
import type { OrderMenu as OrderMenuData } from '@/lib/order/menu-data';
import { cn } from '@/lib/utils/cn';
import { basketLines } from './basket-lines';
import { BasketPanel } from './basket-panel';

interface OrderMenuProps {
  menus: OrderMenuData[];
  items: Record<string, ItemView>;
  branch: { slug: string; name: string };
  houses: Record<string, string>;
  canOrder: boolean;
}

/**
 * The menu to order from: what is being served now first, every dish one tap from the basket (dishes with
 * choices open a sheet). The basket stays beside the menu on wide screens and in a bar on phones.
 */
export function OrderMenu({ menus, items, branch, houses, canOrder }: OrderMenuProps) {
  const t = useTranslations('order');
  const tm = useTranslations('menu');
  const tc = useTranslations('common');
  const tu = useTranslations('common.units');
  const locale = useLocale();
  const c = useCart();
  const [adding, setAdding] = useState<ItemView | null>(null);
  const dietLabels = Object.fromEntries(DIETARY_TAGS.map((d) => [d, tc(`dietary.${d}`)]));
  const allergenLabels = Object.fromEntries(ALLERGENS.map((a) => [a, tc(`allergens.${a}`)]));
  const count = c.lines.reduce((n, l) => n + l.qty, 0);
  const subtotal = basketLines(c.lines, items).reduce((n, l) => n + l.total, 0);

  const add = (item: ItemView) => {
    if (item.modifierGroups.length) {
      setAdding(item);
      return;
    }
    if (c.lines.length && c.branchSlug && c.branchSlug !== branch.slug) cart.replace([], branch.slug, c.channel);
    else cart.setBranch(branch.slug);
    cart.add(item.slug, 1);
    toast(t('basket.added', { dish: item.name }), { label: t('basket.view'), href: '/order/cart' });
  };

  return (
    <div className="site-grid gap-y-10 pb-28 lg:pb-[var(--spacing-section)]">
      <div className="col-span-full flex min-w-0 flex-col gap-12 lg:col-span-8">
        <nav aria-label={t('menu.sections')} className="sticky top-0 z-20 -mx-[var(--spacing-gutter)] border-b border-line bg-bg px-[var(--spacing-gutter)] py-3">
          <ul className="no-scrollbar flex gap-2 overflow-x-auto" data-lenis-prevent>
            {menus.flatMap((m) =>
              m.categories.map((cat) => (
                <li key={cat.slug} className="shrink-0">
                  <a href={`#${cat.slug}`} className="t-small inline-flex min-h-10 items-center rounded-pill border border-line px-4 whitespace-nowrap hover-capable:hover:border-ink">
                    {cat.name}
                  </a>
                </li>
              )),
            )}
          </ul>
        </nav>

        {menus.map((menu) => (
          <section key={menu.slug} aria-labelledby={`menu-${menu.slug}`} className="flex flex-col gap-8">
            <header className="flex flex-wrap items-baseline justify-between gap-x-6 gap-y-2 border-t border-ink pt-6">
              <h2 id={`menu-${menu.slug}`} className="t-heading-lg">
                {menu.name}
              </h2>
              <p className="t-small flex items-center gap-2 text-muted">
                {menu.servingNow ? <span aria-hidden="true" className="size-2 rounded-full bg-success" /> : null}
                {menu.window ? t('menu.servedAt', { window: menu.window }) : t('menu.allDay')}
              </p>
            </header>
            {!menu.servingNow ? <p className="t-small -mt-4 text-muted">{t('menu.notNow')}</p> : null}
            {menu.categories.map((cat) => (
              <div key={cat.slug} id={cat.slug} className="scroll-mt-24">
                <h3 className="t-label mb-2 text-muted">{cat.name}</h3>
                <ul className="border-t border-line">
                  {cat.items.map((slug) => {
                    const item = items[slug];
                    if (!item) return null;
                    const blocked = !item.available || item.soldOut;
                    const contains = item.allergens.length ? `${tc('allergens.contains')} ${formatList(item.allergens.map((a) => allergenLabels[a] ?? a), locale)}` : tc('allergens.none');
                    return (
                      <li key={slug} className={cn('grid grid-cols-[1fr_auto] gap-x-4 gap-y-2 border-b border-line py-5', blocked && 'opacity-70')}>
                        <div className="flex min-w-0 flex-col gap-2">
                          <div className="flex flex-wrap items-baseline gap-x-3">
                            <h4 className="t-heading-sm">
                              <Link href={`/menu/dish/${item.slug}`} className="underline decoration-transparent underline-offset-[0.2em] hover-capable:hover:decoration-current">
                                {item.name}
                              </Link>
                            </h4>
                            {item.signature ? <span className="t-label text-accent">{tc('status.signature')}</span> : null}
                            {item.soldOut ? <span className="t-label text-danger">{tm('item.soldOut')}</span> : null}
                            {!item.available ? <span className="t-label text-muted">{tm('item.unavailable', { house: branch.name })}</span> : null}
                          </div>
                          <p className="t-small text-muted line-clamp-2">{item.description}</p>
                          <DishMarks dietary={item.dietary} allergens={item.allergens} spice={item.spice} labels={{ diet: dietLabels, allergen: allergenLabels, spice: tc(`spice.${item.spice}`), contains }} />
                          <span className="t-body tabular">
                            <bdi>{formatMoney(item.price, locale)}</bdi>
                          </span>
                        </div>
                        <div className="relative self-start">
                          {item.image ? (
                            <div className="arch-3x4 relative aspect-[3/4] w-20 overflow-hidden bg-surface sm:w-24">
                              <Image src={item.image.src} alt="" fill sizes="96px" quality={55} className="object-cover" style={{ objectPosition: `${item.image.focalX * 100}% ${item.image.focalY * 100}%` }} />
                            </div>
                          ) : null}
                          {canOrder && !blocked ? (
                            <button
                              type="button"
                              onClick={() => add(item)}
                              className={cn(
                                'inline-flex size-11 items-center justify-center rounded-full border border-ink bg-bg transition-colors hover-capable:hover:bg-ink hover-capable:hover:text-bg',
                                item.image && 'absolute -bottom-3 -start-3',
                              )}
                              aria-label={tm('item.addNamed', { name: item.name })}
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
      </div>

      <aside className="hidden lg:col-span-4 lg:col-start-9 lg:block">
        <div className="sticky top-24 max-h-[calc(100dvh-7rem)] overflow-y-auto" data-lenis-prevent>
          <BasketPanel items={items} branch={branch} houses={houses} />
        </div>
      </aside>

      {count ? (
        <div className="fixed inset-x-0 bottom-0 z-30 border-t border-line bg-bg px-4 pt-3 pb-[calc(env(safe-area-inset-bottom)+0.75rem)] lg:hidden">
          <Link href="/order/cart" className="flex min-h-12 items-center justify-between gap-4 bg-accent px-5 text-on-accent">
            <span className="t-small">{t('basket.bar', { items: tu('items', plural(count, locale)), total: formatMoney(subtotal, locale) })}</span>
            <Icon name="arrow" size={18} />
          </Link>
        </div>
      ) : null}

      <QuickAdd item={adding} branchSlug={branch.slug} branchName={branch.name} onClose={() => setAdding(null)} />
    </div>
  );
}
