import type { Metadata } from 'next';
import restaurantConfig from '@config';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import { MenuExplorer } from '@/components/site/menu/menu-explorer';
import { InstrumentLine } from '@/components/site/instrument-line';
import { SplitWords } from '@/components/motion/split-words';
import { BranchSwitch } from '@/components/site/chrome/branch-switch';
import { JsonLd } from '@/components/site/json-ld';
import { getCurrentUser } from '@/lib/auth/session';
import { minutesUntilServing } from '@/lib/domain/menus';
import { tr } from '@/lib/i18n/localized';
import { getBranches } from '@/lib/queries/branches';
import { getMenuCatalog } from '@/lib/queries/catalog';
import { toDietProfile } from '@/lib/menu/filter';
import { itemView, menuView, visibleItems, type ItemView } from '@/lib/menu/view';
import { getSettings } from '@/lib/server/settings';
import { getSelectedBranch } from '@/lib/site/selection';
import { getServingContext, servingNames } from '@/lib/site/serving';
import { pageMetadata } from '@/lib/site/metadata';
import { absoluteUrl } from '@/lib/site/url';
import { toLocalMinutes } from '@/lib/time/zoned';

export async function generateMetadata({ params }: { params: Promise<{ locale: string }> }): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: 'menu.meta' });
  return pageMetadata({ locale, path: '/menu', title: t('title'), description: t('description'), image: 'menu' });
}

export default async function MenuPage({ params, searchParams }: { params: Promise<{ locale: string }>; searchParams: Promise<{ m?: string }> }) {
  const { locale } = await params;
  const { m } = await searchParams;
  setRequestLocale(locale);
  const [t, branch, branches, catalog, settings, user] = await Promise.all([getTranslations('menu'), getSelectedBranch(), getBranches(), getMenuCatalog(), getSettings(), getCurrentUser()]);
  const serving = await getServingContext(branch);
  if (!branch) return null;

  const menus = serving.menus;
  const timed = serving.serving.filter((x) => x.schedule.length > 0);
  const upcoming = [...menus]
    .filter((x) => x.schedule.length > 0)
    .map((x) => ({ x, wait: minutesUntilServing(x, serving.now, branch.timeZone) ?? Infinity }))
    .sort((a, b) => a.wait - b.wait)[0]?.x;
  const initial = (m && menus.find((x) => x.slug === m)) || timed[0] || upcoming || menus[0];

  const allowed = new Set(visibleItems(catalog, settings.features.alcohol).map((i) => i.slug));
  const slugs = new Set(menus.flatMap((x) => x.categories.flatMap((c) => c.items)).filter((s) => allowed.has(s)));
  const items: Record<string, ItemView> = {};
  for (const slug of slugs) {
    const item = catalog.items[slug];
    if (item) items[slug] = itemView(item, catalog, branch.id, locale);
  }
  const canOrder = settings.features.ordering && !branch.orderingPaused && (branch.pickupEnabled || branch.deliveryEnabled);

  const jsonLd = {
    '@context': 'https://schema.org',
    '@type': 'Menu',
    name: t('title'),
    url: absoluteUrl(`/${locale}/menu`),
    inLanguage: locale,
    hasMenuSection: menus.map((menu) => ({
      '@type': 'MenuSection',
      name: tr(menu.name, locale),
      description: menu.description ? tr(menu.description, locale) : undefined,
      hasMenuItem: menu.categories.flatMap((c) =>
        c.items
          .map((s) => items[s])
          .filter((i): i is ItemView => Boolean(i))
          .map((i) => ({
            '@type': 'MenuItem',
            name: i.name,
            description: i.description,
            url: absoluteUrl(`/${locale}/menu/dish/${i.slug}`),
            offers: { '@type': 'Offer', price: (i.price / 100).toFixed(2), priceCurrency: restaurantConfig.currency },
            suitableForDiet: i.dietary.includes('vegan') ? 'https://schema.org/VeganDiet' : i.dietary.includes('vegetarian') ? 'https://schema.org/VegetarianDiet' : i.dietary.includes('gluten-free') ? 'https://schema.org/GlutenFreeDiet' : undefined,
          })),
      ),
    })),
  };

  return (
    <>
      <JsonLd data={jsonLd} />
      <header className="site-grid gap-y-8 pt-10 pb-12 lg:pt-16">
        <InstrumentLine servingNow={servingNames(serving, locale)} className="col-span-full text-muted" />
        <h1 className="t-display-lg col-span-full">
          <SplitWords text={t('title')} />
        </h1>
        <p className="t-body-lg measure col-span-full lg:col-span-7">{t('intro', { house: tr(branch.shortName, locale) })}</p>
        <BranchSwitch
          className="col-span-full lg:col-span-5 lg:justify-self-end"
          branches={branches.map((b) => ({ slug: b.slug, name: tr(b.shortName, locale), city: tr(b.city, locale) }))}
          selected={branch.slug}
        />
      </header>
      {initial ? (
        <MenuExplorer
          key={`${branch.slug}-${initial.slug}`}
          menus={menus.map((x) => menuView(x, locale))}
          items={items}
          initialMenu={initial.slug}
          branch={{ slug: branch.slug, name: tr(branch.shortName, locale), timeZone: branch.timeZone }}
          profile={toDietProfile(user?.dietary)}
          signedIn={Boolean(user)}
          today={serving.today}
          nowIso={serving.now.toISOString()}
          nowMinutes={toLocalMinutes(serving.now, branch.timeZone)}
          canOrder={canOrder}
        />
      ) : null}
    </>
  );
}
