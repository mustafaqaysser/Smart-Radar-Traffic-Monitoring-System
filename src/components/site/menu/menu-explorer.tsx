'use client';

import { useLocale, useTranslations } from 'next-intl';
import { useEffect, useId, useMemo, useState } from 'react';
import Image from 'next/image';
import { Icon } from '@/components/brand/icon';
import { Button } from '@/components/site/ui/button';
import { Chip, Input, Label } from '@/components/site/ui/field';
import { Dialog } from '@/components/site/ui/dialog';
import { Link } from '@/i18n/navigation';
import { ALLERGENS, DIETARY_TAGS, type Allergen, type DietaryTag } from '@/lib/menu/tags';
import { isServing, minutesUntilServing } from '@/lib/domain/menus';
import { formatClock, formatList, formatMoney } from '@/lib/i18n/format';
import { plural } from '@/lib/i18n/plural';
import { activeFilterCount, EMPTY_FILTERS, passesFilters, type MenuFilters } from '@/lib/menu/filter';
import type { ItemView, MenuView } from '@/lib/menu/view';
import { localToUtc } from '@/lib/time/zoned';
import { cn } from '@/lib/utils/cn';
import { DishMarks } from './badges';
import { MatchmakerDialog } from './matchmaker-dialog';
import { QuickAdd } from './quick-add';
import { minutesToStep, stepToMinutes, SundialScrubber } from './sundial-scrubber';
import { TrailingWindow, useTrailingWindow } from './trailing-window';

export interface MenuExplorerProps {
  menus: MenuView[];
  items: Record<string, ItemView>;
  initialMenu: string;
  branch: { slug: string; name: string; timeZone: string };
  today: string;
  nowIso: string;
  nowMinutes: number;
  canOrder: boolean;
}

const SPICE_LEVELS = [0, 1, 2] as const;

export function MenuExplorer({ menus, items, initialMenu, branch, today, nowIso, nowMinutes, canOrder }: MenuExplorerProps) {
  const t = useTranslations('menu');
  const tc = useTranslations('common');
  const locale = useLocale();
  const id = useId();
  const nowStep = minutesToStep(nowMinutes);
  const [menuSlug, setMenuSlug] = useState(initialMenu);
  const [step, setStep] = useState(nowStep);
  const [filters, setFilters] = useState<MenuFilters>(EMPTY_FILTERS);
  const [filtersOpen, setFiltersOpen] = useState(false);
  const [matchOpen, setMatchOpen] = useState(false);
  const [adding, setAdding] = useState<ItemView | null>(null);
  const trailing = useTrailingWindow();

  // Keep the chosen menu in the URL so it can be shared (?m=slug), without a navigation.
  useEffect(() => {
    const url = new URL(window.location.href);
    url.searchParams.set('m', menuSlug);
    window.history.replaceState(window.history.state, '', url);
  }, [menuSlug]);

  const instantAt = (s: number) => localToUtc(today, stepToMinutes(s), branch.timeZone, true) ?? new Date(nowIso);
  const servedAt = (s: number) => menus.filter((m) => m.schedule.length > 0 && isServing(m, instantAt(s), branch.timeZone));

  const onScrub = (s: number) => {
    setStep(s);
    const served = servedAt(s);
    if (served[0] && !served.some((m) => m.slug === menuSlug)) setMenuSlug(served[0].slug);
  };

  const pickMenu = (m: MenuView) => {
    setMenuSlug(m.slug);
    const first = m.schedule[0];
    if (first && !servedAt(step).some((x) => x.slug === m.slug)) {
      const [h, mm] = first.start.split(':').map(Number) as [number, number];
      setStep(minutesToStep(h * 60 + mm));
    }
  };

  const menu = menus.find((m) => m.slug === menuSlug) ?? menus[0];
  const servedNames = servedAt(step).map((m) => m.name);
  const readout = servedNames.length ? formatList(servedNames, locale) : t('now.resting');

  const statusOf = (m: MenuView): string => {
    if (m.schedule.length === 0) return t('now.allDay');
    const now = new Date(nowIso);
    if (isServing(m, now, branch.timeZone)) return t('now.serving');
    const wait = minutesUntilServing(m, now, branch.timeZone);
    if (wait !== null && wait < 24 * 60) return t('now.later', { when: formatClock(new Date(now.getTime() + wait * 60000), locale, branch.timeZone) });
    return t('now.notToday');
  };

  const visible = useMemo(() => {
    if (!menu) return [];
    return menu.categories
      .map((c) => ({ ...c, items: c.items.map((slug) => items[slug]).filter((i): i is ItemView => Boolean(i) && passesFilters(i as ItemView, filters)) }))
      .filter((c) => c.items.length > 0);
  }, [menu, items, filters]);
  const count = visible.reduce((n, c) => n + c.items.length, 0);
  const menuItems = useMemo(() => visible.flatMap((c) => c.items), [visible]);
  const nActive = activeFilterCount(filters);

  const dietLabels = Object.fromEntries(DIETARY_TAGS.map((d) => [d, tc(`dietary.${d}`)]));
  const allergenLabels = Object.fromEntries(ALLERGENS.map((a) => [a, tc(`allergens.${a}`)]));

  const toggleDiet = (d: DietaryTag) => setFilters((f) => ({ ...f, diets: f.diets.includes(d) ? f.diets.filter((x) => x !== d) : [...f.diets, d] }));
  const toggleAvoid = (a: Allergen) => setFilters((f) => ({ ...f, avoid: f.avoid.includes(a) ? f.avoid.filter((x) => x !== a) : [...f.avoid, a] }));

  const filterPanel = (
    <div className="flex flex-col gap-8">
      <div>
        <Label htmlFor={`${id}-q`}>{t('filters.search')}</Label>
        <div className="relative">
          <Icon name="search" size={18} className="pointer-events-none absolute inset-y-0 start-4 my-auto text-muted" />
          <Input id={`${id}-q`} type="search" value={filters.query} placeholder={t('filters.searchPlaceholder')} onChange={(e) => setFilters((f) => ({ ...f, query: e.target.value }))} className="ps-11" enterKeyHint="search" />
        </div>
      </div>
      <fieldset>
        <legend className="t-label mb-3 text-muted">{t('filters.diet')}</legend>
        <div className="flex flex-wrap gap-2">
          {DIETARY_TAGS.map((d) => (
            <Chip key={d} pressed={filters.diets.includes(d)} onClick={() => toggleDiet(d)}>
              {dietLabels[d]}
            </Chip>
          ))}
        </div>
      </fieldset>
      <fieldset>
        <legend className="t-label mb-1 text-muted">{t('filters.avoid')}</legend>
        <p className="t-small mb-3 text-muted">{t('filters.avoidHint')}</p>
        <div className="flex flex-wrap gap-2">
          {ALLERGENS.map((a) => (
            <Chip key={a} pressed={filters.avoid.includes(a)} onClick={() => toggleAvoid(a)} className="px-3">
              <Icon name={a} size={16} struck={filters.avoid.includes(a)} />
              {allergenLabels[a]}
            </Chip>
          ))}
        </div>
      </fieldset>
      <fieldset>
        <legend className="t-label mb-3 text-muted">{t('filters.spice')}</legend>
        <div className="flex flex-wrap gap-2">
          <Chip pressed={filters.maxSpice === null} onClick={() => setFilters((f) => ({ ...f, maxSpice: null }))}>
            {t('filters.spiceAny')}
          </Chip>
          {SPICE_LEVELS.map((s) => (
            <Chip key={s} pressed={filters.maxSpice === s} onClick={() => setFilters((f) => ({ ...f, maxSpice: s }))}>
              {s === 0 ? tc('spice.0') : t('filters.spiceUpTo', { level: tc(`spice.${s}`) })}
            </Chip>
          ))}
        </div>
      </fieldset>
      {nActive ? (
        <button type="button" className="t-label self-start underline underline-offset-4" onClick={() => setFilters(EMPTY_FILTERS)}>
          {t('filters.clear')}
        </button>
      ) : null}
    </div>
  );

  return (
    <div className="site-grid gap-y-10">
      {/* Dial and menus */}
      <div className="col-span-full grid gap-10 border-y border-line py-10 lg:grid-cols-12">
        <SundialScrubber value={step} nowStep={nowStep} onChange={onScrub} readout={readout} className="mx-auto w-full max-w-md lg:col-span-5" />
        <nav aria-label={t('menus')} className="lg:col-span-7 lg:self-center">
          <ul className="grid gap-x-6 sm:grid-cols-2">
            {menus.map((m) => {
              const active = m.slug === menu?.slug;
              return (
                <li key={m.slug}>
                  <button
                    type="button"
                    aria-pressed={active}
                    onClick={() => pickMenu(m)}
                    className={cn('group flex w-full items-baseline justify-between gap-4 border-b py-3 text-start transition-colors', active ? 'border-ink' : 'border-line hover-capable:hover:border-ink')}
                  >
                    <span className={cn('t-heading-sm', active && 'text-accent')}>{m.name}</span>
                    <span className="t-small shrink-0 text-muted">{statusOf(m)}</span>
                  </button>
                </li>
              );
            })}
          </ul>
        </nav>
      </div>

      {/* Filters (sidebar on wide screens, a sheet on small ones) */}
      <aside className="col-span-full hidden lg:col-span-3 lg:block" aria-label={t('filters.title')}>
        <div className="sticky top-24 flex flex-col gap-10">
          {filterPanel}
          <div className="flex flex-col gap-3 border-t border-line pt-6">
            <Button variant="secondary" icon={null} leadingIcon="concierge" onClick={() => setMatchOpen(true)}>
              {t('matchmaker.open')}
            </Button>
            <a href={`/${locale}/menu/print?m=${menu?.slug ?? ''}&branch=${branch.slug}`} className="t-small inline-flex min-h-11 items-center gap-2 underline decoration-line underline-offset-4">
              <Icon name="print" size={18} />
              {t('actions.print')}
            </a>
            <a href={`/menu-pdf/${branch.slug}-${menu?.slug ?? ''}-${locale}.pdf`} className="t-small inline-flex min-h-11 items-center gap-2 underline decoration-line underline-offset-4">
              <Icon name="download" size={18} />
              {t('actions.pdf')}
            </a>
          </div>
        </div>
      </aside>

      <div className="col-span-full flex flex-wrap items-center gap-3 lg:hidden">
        <Button variant="secondary" size="sm" icon={null} leadingIcon="filter" onClick={() => setFiltersOpen(true)}>
          {nActive ? `${t('filters.show')} · ${t('filters.active', plural(nActive, locale))}` : t('filters.show')}
        </Button>
        <Button variant="secondary" size="sm" icon={null} leadingIcon="concierge" onClick={() => setMatchOpen(true)}>
          {t('matchmaker.open')}
        </Button>
      </div>
      <Dialog open={filtersOpen} onClose={() => setFiltersOpen(false)} title={t('filters.title')} closeLabel={tc('a11y.close')} variant="sheet" footer={<Button className="w-full" icon={null} onClick={() => setFiltersOpen(false)}>{`${t('filters.done')} · ${t('filters.results', plural(count, locale))}`}</Button>}>
        {filterPanel}
      </Dialog>

      {/* The menu */}
      {menu ? (
        <section className="col-span-full lg:col-span-9" aria-labelledby={`${id}-menu`}>
          <header className="mb-10 flex flex-col gap-4">
            <p className="t-label text-muted">{menu.hour}</p>
            <h2 id={`${id}-menu`} className="t-display-md">
              {menu.name}
            </h2>
            {menu.description ? <p className="t-body-lg measure">{menu.description}</p> : null}
            {menu.isTasting && menu.tastingPrice ? <p className="t-heading-sm">{t('notes.tasting', { price: formatMoney(menu.tastingPrice, locale) })}</p> : null}
            <p className="t-small text-muted" aria-live="polite">
              {t('filters.results', plural(count, locale))}
            </p>
          </header>

          {count === 0 ? <p className="t-body border-t border-line py-10 text-muted">{t('filters.none')}</p> : null}

          {visible.map((c) => (
            <section key={c.slug} aria-labelledby={`${id}-${c.slug}`} className="mb-14">
              <h3 id={`${id}-${c.slug}`} className="t-heading-lg mb-2">
                {c.name}
              </h3>
              {c.description ? <p className="t-small mb-4 text-muted">{c.description}</p> : null}
              <ul className="border-t border-ink">
                {c.items.map((item) => {
                  const contains = item.allergens.length ? `${tc('allergens.contains')} ${formatList(item.allergens.map((a) => allergenLabels[a] ?? a), locale)}` : tc('allergens.none');
                  const blocked = !item.available || item.soldOut;
                  return (
                    <li
                      key={item.slug}
                      className={cn('group relative grid grid-cols-[auto_1fr_auto] gap-x-4 gap-y-2 border-b border-line py-6', blocked && 'opacity-70')}
                      onPointerEnter={() => trailing.show(item.image)}
                      onPointerLeave={trailing.hide}
                    >
                      {item.image ? (
                        <div className="arch-3x4 relative row-span-3 aspect-[3/4] w-16 overflow-hidden bg-surface sm:w-20 lg:hidden">
                          <Image src={item.image.src} alt="" fill sizes="80px" quality={55} className="object-cover" style={{ objectPosition: `${item.image.focalX * 100}% ${item.image.focalY * 100}%` }} />
                        </div>
                      ) : (
                        <span className="row-span-3 lg:hidden" />
                      )}
                      <div className="col-start-2 flex flex-wrap items-baseline gap-x-3 lg:col-start-1 lg:col-end-3">
                        <h4 className="t-heading-sm">
                          <Link href={`/menu/dish/${item.slug}`} className="underline decoration-transparent underline-offset-[0.2em] transition-[text-decoration-color] hover-capable:hover:decoration-current">
                            {item.name}
                          </Link>
                        </h4>
                        {item.signature ? <span className="t-label text-accent">{tc('status.signature')}</span> : null}
                        {item.soldOut ? <span className="t-label text-danger">{t('item.soldOut')}</span> : null}
                        {!item.available ? <span className="t-label text-muted">{t('item.unavailable', { house: branch.name })}</span> : null}
                      </div>
                      <p className="t-small col-start-2 text-muted lg:col-start-1 lg:col-end-3">{item.description}</p>
                      <DishMarks
                        className="col-start-2 lg:col-start-1 lg:col-end-3"
                        dietary={item.dietary}
                        allergens={item.allergens}
                        spice={item.spice}
                        labels={{ diet: dietLabels, allergen: allergenLabels, spice: tc(`spice.${item.spice}`), contains }}
                      />
                      <div className="col-start-3 row-span-3 row-start-1 flex flex-col items-end gap-3">
                        {!menu.isTasting ? (
                          <span className="t-body tabular">
                            <bdi>{formatMoney(item.price, locale)}</bdi>
                          </span>
                        ) : null}
                        {canOrder && item.orderable && !blocked ? (
                          <button type="button" onClick={() => setAdding(item)} className="inline-flex size-11 items-center justify-center rounded-full border border-ink transition-colors hover-capable:hover:bg-ink hover-capable:hover:text-bg" aria-label={t('item.addNamed', { name: item.name })}>
                            <Icon name="plus" size={20} />
                          </button>
                        ) : canOrder && !item.orderable && !menu.isTasting ? (
                          <span className="t-small text-muted">{t('item.notOrderable')}</span>
                        ) : null}
                      </div>
                    </li>
                  );
                })}
              </ul>
            </section>
          ))}

          <div className="flex flex-col gap-3 border-t border-line pt-8">
            <p className="t-small measure text-muted">{t('notes.allergens')}</p>
            <p className="t-small measure text-muted">{t('notes.prices')}</p>
            <div className="mt-4 flex flex-wrap gap-x-6 gap-y-2 lg:hidden">
              <a href={`/${locale}/menu/print?m=${menu.slug}&branch=${branch.slug}`} className="t-small inline-flex min-h-11 items-center gap-2 underline decoration-line underline-offset-4">
                <Icon name="print" size={18} />
                {t('actions.print')}
              </a>
              <a href={`/menu-pdf/${branch.slug}-${menu.slug}-${locale}.pdf`} className="t-small inline-flex min-h-11 items-center gap-2 underline decoration-line underline-offset-4">
                <Icon name="download" size={18} />
                {t('actions.pdf')}
              </a>
            </div>
          </div>
        </section>
      ) : null}

      <TrailingWindow image={trailing.image} />
      <MatchmakerDialog
        open={matchOpen}
        onClose={() => setMatchOpen(false)}
        items={menuItems}
        onAdd={(item) => {
          setMatchOpen(false);
          if (canOrder) setAdding(item);
        }}
      />
      {canOrder ? <QuickAdd item={adding} branchSlug={branch.slug} branchName={branch.name} onClose={() => setAdding(null)} /> : null}
    </div>
  );
}
