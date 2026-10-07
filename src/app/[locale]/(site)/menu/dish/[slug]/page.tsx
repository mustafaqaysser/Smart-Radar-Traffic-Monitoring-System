import type { Metadata } from 'next';
import { notFound } from 'next/navigation';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import { and, eq } from 'drizzle-orm';
import { Icon, type IconName } from '@/components/brand/icon';
import { SplitWords } from '@/components/motion/split-words';
import { Reveal } from '@/components/motion/reveal';
import { JsonLd } from '@/components/site/json-ld';
import { DishActions } from '@/components/site/menu/dish-actions';
import { DishCard } from '@/components/site/menu/dish-card';
import { BackLink } from '@/components/site/ui/button';
import { MediaImage } from '@/components/site/ui/media-image';
import { Rail } from '@/components/site/ui/rail';
import { getCurrentUser } from '@/lib/auth/session';
import { db } from '@/lib/db/client';
import { favorites } from '@/lib/db/schema';
import { formatList, formatMoney, formatNumber } from '@/lib/i18n/format';
import { tr } from '@/lib/i18n/localized';
import { describeSchedule } from '@/lib/menu/schedule';
import { itemView } from '@/lib/menu/view';
import { getBranches } from '@/lib/queries/branches';
import { getMenuCatalog } from '@/lib/queries/catalog';
import type { MenuItemDTO } from '@/lib/queries/types';
import { getSettings } from '@/lib/server/settings';
import { getSelectedBranch } from '@/lib/site/selection';
import { pageMetadata } from '@/lib/site/metadata';
import { absoluteUrl } from '@/lib/site/url';

type Params = Promise<{ locale: string; slug: string }>;

async function loadItem(slug: string): Promise<MenuItemDTO | null> {
  const [catalog, settings] = await Promise.all([getMenuCatalog(), getSettings()]);
  const item = catalog.items[slug];
  if (!item || !item.isActive || (item.isAlcoholic && !settings.features.alcohol)) return null;
  return item;
}

export async function generateMetadata({ params }: { params: Params }): Promise<Metadata> {
  const { locale, slug } = await params;
  const item = await loadItem(slug);
  if (!item) return {};
  return pageMetadata({ locale, path: `/menu/dish/${slug}`, title: tr(item.name, locale), description: tr(item.description, locale), image: item.image ? item.image.src : 'menu' });
}

const DIET_ICON: Record<string, { icon: IconName; struck?: boolean }> = {
  vegetarian: { icon: 'leaf' },
  vegan: { icon: 'sprout' },
  'gluten-free': { icon: 'gluten', struck: true },
  'dairy-free': { icon: 'milk', struck: true },
  'nut-free': { icon: 'nuts', struck: true },
};

export default async function DishPage({ params }: { params: Params }) {
  const { locale, slug } = await params;
  setRequestLocale(locale);
  const item = await loadItem(slug);
  if (!item) notFound();
  const [t, tc, catalog, branches, selected, settings, user] = await Promise.all([
    getTranslations('menu'),
    getTranslations('common'),
    getMenuCatalog(),
    getBranches(),
    getSelectedBranch(),
    getSettings(),
    getCurrentUser(),
  ]);
  const branch = selected ?? branches[0];
  if (!branch) notFound();
  const view = itemView(item, catalog, branch.id, locale);
  const favourite = user ? Boolean(await db.query.favorites.findFirst({ where: and(eq(favorites.userId, user.id), eq(favorites.itemId, item.id)) })) : false;
  const canOrder = settings.features.ordering && !branch.orderingPaused && (branch.pickupEnabled || branch.deliveryEnabled);

  const menus = catalog.menus.filter((m) => m.categories.some((c) => c.items.includes(slug)) && !m.seasonalModeId);
  const served = menus.map((m) => ({ menu: tr(m.name, locale), lines: m.schedule.length ? describeSchedule(m.schedule, locale, t('dish.everyDay')) : [t('now.allDay')] }));
  const pairings = item.pairings.map((p) => catalog.items[p]).filter((p): p is MenuItemDTO => Boolean(p && p.isActive));
  const firstMenu = menus[0];
  const siblings = firstMenu
    ? firstMenu.categories
        .flatMap((c) => c.items)
        .filter((s, i, arr) => s !== slug && arr.indexOf(s) === i)
        .map((s) => catalog.items[s])
        .filter((i): i is MenuItemDTO => Boolean(i && i.isActive && i.image))
        .slice(0, 8)
    : [];
  const allergenNames = item.allergens.map((a) => tc(`allergens.${a}`));

  const jsonLd = {
    '@context': 'https://schema.org',
    '@type': 'MenuItem',
    name: tr(item.name, locale),
    description: tr(item.description, locale),
    url: absoluteUrl(`/${locale}/menu/dish/${slug}`),
    image: item.image ? absoluteUrl(item.image.src) : undefined,
    offers: { '@type': 'Offer', price: (view.price / 100).toFixed(2), priceCurrency: 'SAR', availability: view.soldOut ? 'https://schema.org/SoldOut' : 'https://schema.org/InStock' },
    nutrition: item.calories ? { '@type': 'NutritionInformation', calories: `${item.calories} calories` } : undefined,
    suitableForDiet: item.dietary.includes('vegan') ? 'https://schema.org/VeganDiet' : item.dietary.includes('vegetarian') ? 'https://schema.org/VegetarianDiet' : undefined,
  };

  return (
    <article className="site-grid gap-y-12 pt-8 lg:pt-12" aria-labelledby="dish-title">
      <JsonLd data={jsonLd} />
      <nav aria-label={tc('a11y.breadcrumbs')} className="col-span-full">
        <BackLink href={`/menu${firstMenu ? `?m=${firstMenu.slug}` : ''}`}>{t('dish.back')}</BackLink>
      </nav>

      <div className="col-span-full flex flex-col gap-8 md:col-span-4 lg:col-span-6 lg:self-end">
        {item.isSignature ? <p className="t-label text-accent">{tc('status.signature')}</p> : null}
        <h1 id="dish-title" className="t-display-lg">
          <SplitWords text={tr(item.name, locale)} />
        </h1>
        <p className="t-body-lg measure">{tr(item.description, locale)}</p>
        <p className="t-heading-md tabular">
          <bdi>{formatMoney(view.price, locale)}</bdi>
          {view.soldOut ? <span className="t-label ms-4 text-danger">{t('dish.soldOut')}</span> : null}
        </p>
        <DishActions item={view} canOrder={canOrder} branch={{ slug: branch.slug, name: tr(branch.shortName, locale) }} signedIn={Boolean(user)} initialFavourite={favourite} />
      </div>

      {item.image ? (
        <Reveal variant="shade" className="col-span-full md:col-span-4 lg:col-span-5 lg:col-start-8">
          <MediaImage media={item.image} locale={locale} sizes="(min-width: 1024px) 40vw, (min-width: 768px) 50vw, 100vw" ratio="4/5" shape="arch-4x5" preload className="cast-shade" />
        </Reveal>
      ) : null}

      <div className="col-span-full grid gap-x-[var(--spacing-gutter)] gap-y-10 border-t border-ink pt-10 md:grid-cols-2 lg:grid-cols-3">
        {item.story ? (
          <section className="md:col-span-2 lg:col-span-1" aria-labelledby="dish-story">
            <h2 id="dish-story" className="t-label mb-3 text-muted">
              {t('dish.story')}
            </h2>
            <p className="t-body-lg">{tr(item.story, locale)}</p>
          </section>
        ) : null}
        {item.ingredients ? (
          <section aria-labelledby="dish-ingredients">
            <h2 id="dish-ingredients" className="t-label mb-3 text-muted">
              {t('dish.ingredients')}
            </h2>
            <p className="t-body">{tr(item.ingredients, locale)}</p>
          </section>
        ) : null}
        <section aria-labelledby="dish-allergens">
          <h2 id="dish-allergens" className="t-label mb-3 text-muted">
            {t('dish.allergens')}
          </h2>
          {item.allergens.length ? (
            <ul className="flex flex-col gap-2">
              {item.allergens.map((a, i) => (
                <li key={a} className="t-body flex items-center gap-3">
                  <Icon name={a as IconName} size={20} />
                  {allergenNames[i]}
                </li>
              ))}
            </ul>
          ) : (
            <p className="t-body">{tc('allergens.none')}</p>
          )}
          <p className="t-small mt-4 text-muted">{t('notes.allergens')}</p>
        </section>
        {item.dietary.length ? (
          <section aria-labelledby="dish-diet">
            <h2 id="dish-diet" className="t-label mb-3 text-muted">
              {t('dish.dietary')}
            </h2>
            <ul className="flex flex-col gap-2">
              {item.dietary.map((d) => (
                <li key={d} className="t-body flex items-center gap-3">
                  <Icon name={DIET_ICON[d]?.icon ?? 'leaf'} struck={DIET_ICON[d]?.struck} size={20} />
                  {tc(`dietary.${d}`)}
                </li>
              ))}
            </ul>
          </section>
        ) : null}
        <section aria-labelledby="dish-facts">
          <h2 id="dish-facts" className="t-label mb-3 text-muted">
            {t('dish.facts')}
          </h2>
          <dl className="t-body grid grid-cols-[auto_1fr] gap-x-6 gap-y-2">
            {item.calories ? (
              <>
                <dt className="text-muted">{t('dish.nutrition')}</dt>
                <dd>{t('item.kcal', { n: formatNumber(item.calories, locale) })}</dd>
              </>
            ) : null}
            <dt className="text-muted">{t('dish.spice')}</dt>
            <dd className="flex items-center gap-2">
              {Array.from({ length: item.spiceLevel }, (_, i) => (
                <Icon key={i} name="chili" size={16} />
              ))}
              {tc(`spice.${Math.min(3, item.spiceLevel)}`)}
            </dd>
          </dl>
        </section>
        {served.length ? (
          <section aria-labelledby="dish-served">
            <h2 id="dish-served" className="t-label mb-3 text-muted">
              {t('dish.served')}
            </h2>
            <ul className="flex flex-col gap-3">
              {served.map((s) => (
                <li key={s.menu}>
                  <p className="t-body font-semibold">{s.menu}</p>
                  {s.lines.map((l) => (
                    <p key={l} className="t-small text-muted">
                      {l}
                    </p>
                  ))}
                </li>
              ))}
            </ul>
          </section>
        ) : null}
        <section aria-labelledby="dish-houses">
          <h2 id="dish-houses" className="t-label mb-3 text-muted">
            {t('dish.houses')}
          </h2>
          <ul className="flex flex-col gap-2">
            {branches.map((b) => {
              const s = item.branches[b.id];
              const status = !s || !s.available ? t('dish.notHere') : s.soldOut ? t('dish.soldOut') : t('dish.available');
              return (
                <li key={b.id} className="t-body flex flex-wrap items-baseline justify-between gap-x-4">
                  <span>{tr(b.shortName, locale)}</span>
                  <span className="t-small text-muted">
                    {status}
                    {s && s.available ? (
                      <>
                        {' · '}
                        <bdi className="tabular">{formatMoney(s.price, locale)}</bdi>
                      </>
                    ) : null}
                  </span>
                </li>
              );
            })}
          </ul>
        </section>
        {item.modifierGroups.length ? (
          <section aria-labelledby="dish-mods">
            <h2 id="dish-mods" className="t-label mb-3 text-muted">
              {t('dish.modifiers')}
            </h2>
            <ul className="flex flex-col gap-2">
              {item.modifierGroups.map((g) => (
                <li key={g.id} className="t-body">
                  <span className="font-semibold">{tr(g.name, locale)}</span>
                  <span className="text-muted"> — {formatList(g.options.map((o) => tr(o.name, locale)), locale)}</span>
                </li>
              ))}
            </ul>
          </section>
        ) : null}
      </div>

      {pairings.length ? (
        <section className="col-span-full" aria-labelledby="dish-pairings">
          <h2 id="dish-pairings" className="t-heading-lg mb-8">
            {t('dish.pairings')}
          </h2>
          <ul className="grid grid-cols-2 gap-[var(--spacing-gutter)] md:grid-cols-4">
            {pairings.map((p) => (
              <li key={p.id}>
                <DishCard item={p} locale={locale} branchId={branch.id} labels={{ soldOut: tc('status.soldOut'), signature: tc('status.signature') }} sizes="(min-width: 768px) 22vw, 45vw" />
              </li>
            ))}
          </ul>
        </section>
      ) : null}

      {siblings.length && firstMenu ? (
        <section className="col-span-full -mx-[var(--spacing-margin)]" aria-labelledby="dish-more">
          <h2 id="dish-more" className="site-wrap t-heading-lg mb-8">
            {t('dish.moreFrom', { menu: tr(firstMenu.name, locale) })}
          </h2>
          <Rail label={t('dish.moreFrom', { menu: tr(firstMenu.name, locale) })}>
            {siblings.map((s) => (
              <li key={s.id} className="w-[60vw] shrink-0 snap-start md:w-[30vw] lg:w-[20vw]">
                <DishCard item={s} locale={locale} branchId={branch.id} labels={{ soldOut: tc('status.soldOut'), signature: tc('status.signature') }} sizes="(min-width: 1024px) 20vw, 60vw" />
              </li>
            ))}
          </Rail>
        </section>
      ) : null}
    </article>
  );
}
