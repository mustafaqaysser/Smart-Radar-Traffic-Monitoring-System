import type { Metadata } from 'next';
import { notFound } from 'next/navigation';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import { Icon, type IconName } from '@/components/brand/icon';
import { Lockup } from '@/components/brand/logo';
import { PrintButton } from '@/components/site/menu/print-button';
import { formatDate, formatMoney } from '@/lib/i18n/format';
import { tr } from '@/lib/i18n/localized';
import { describeSchedule } from '@/lib/menu/schedule';
import { visibleItems } from '@/lib/menu/view';
import { getBranch } from '@/lib/queries/branches';
import { getMenuCatalog } from '@/lib/queries/catalog';
import type { MenuItemDTO } from '@/lib/queries/types';
import { getSettings } from '@/lib/server/settings';
import { getSelectedBranch } from '@/lib/site/selection';
import { pageMetadata } from '@/lib/site/metadata';

type Search = Promise<{ m?: string; branch?: string; auto?: string }>;

export async function generateMetadata({ params }: { params: Promise<{ locale: string }> }): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: 'menu.meta' });
  return pageMetadata({ locale, path: '/menu/print', title: t('title'), noindex: true });
}

/** A typographic, A4 menu for printing and for the headless-Chromium PDF render. */
export default async function PrintMenuPage({ params, searchParams }: { params: Promise<{ locale: string }>; searchParams: Search }) {
  const { locale } = await params;
  const { m, branch: branchSlug, auto } = await searchParams;
  setRequestLocale(locale);
  const [t, tc, catalog, settings] = await Promise.all([getTranslations('menu'), getTranslations('common'), getMenuCatalog(), getSettings()]);
  const branch = (branchSlug ? await getBranch(branchSlug) : null) ?? (await getSelectedBranch());
  if (!branch) notFound();
  const menu = catalog.menus.find((x) => x.slug === m) ?? catalog.menus.find((x) => !x.seasonalModeId && (!x.branchIds || x.branchIds.includes(branch.id)));
  if (!menu || (menu.branchIds && !menu.branchIds.includes(branch.id))) notFound();
  const allowed = new Set(visibleItems(catalog, settings.features.alcohol).map((i) => i.slug));
  const categories = menu.categories.map((c) => ({
    ...c,
    items: c.items
      .filter((s) => allowed.has(s))
      .map((s) => catalog.items[s])
      .filter((i): i is MenuItemDTO => Boolean(i) && (i as MenuItemDTO).branches[branch.id]?.available !== false),
  }));
  const used = [...new Set(categories.flatMap((c) => c.items.flatMap((i) => i.allergens)))];
  const schedule = menu.schedule.length ? describeSchedule(menu.schedule, locale, t('dish.everyDay')) : [t('now.allDay')];

  return (
    <main id="main" className="print-sheet mx-auto max-w-[210mm] bg-bg px-[14mm] py-[12mm] text-ink" data-phase="morning">
      <div className="print-hidden mb-8 flex flex-wrap items-center justify-between gap-4 border-b border-line pb-6">
        <p className="t-small text-muted">{t('print.footer')}</p>
        <PrintButton label={t('print.button')} auto={auto === '1'} />
      </div>
      <header className="flex items-start justify-between gap-8 border-b border-ink pb-6">
        <div>
          <p className="t-label text-muted">{tr(branch.name, locale)}</p>
          <h1 className="t-display-md mt-2">{tr(menu.name, locale)}</h1>
          {schedule.map((line) => (
            <p key={line} className="t-small mt-1 text-muted">
              {line}
            </p>
          ))}
        </div>
        <Lockup locale={locale === 'ar' ? 'ar' : 'en'} decorative className="h-12 w-auto shrink-0" />
      </header>
      {menu.description ? <p className="t-body mt-6 max-w-[42em]">{tr(menu.description, locale)}</p> : null}
      {menu.isTasting && menu.tastingPrice ? <p className="t-heading-sm mt-4">{t('notes.tasting', { price: formatMoney(menu.tastingPrice, locale) })}</p> : null}

      <div className="mt-8 columns-1 gap-[10mm] print:columns-2 md:columns-2">
        {categories
          .filter((c) => c.items.length)
          .map((c) => (
            <section key={c.id} className="mb-8 break-inside-avoid-column">
              <h2 className="t-heading-sm mb-3 border-b border-line pb-2">{tr(c.name, locale)}</h2>
              <ul className="flex flex-col gap-4">
                {c.items.map((i) => (
                  <li key={i.id} className="break-inside-avoid">
                    <div className="flex items-baseline justify-between gap-4">
                      <h3 className="text-[1.05rem] font-semibold">{tr(i.name, locale)}</h3>
                      {!menu.isTasting ? (
                        <span className="tabular shrink-0 text-[0.95rem]">
                          <bdi>{formatMoney(i.branches[branch.id]?.price ?? i.price, locale)}</bdi>
                        </span>
                      ) : null}
                    </div>
                    <p className="text-[0.9rem] leading-snug text-muted">{tr(i.description, locale)}</p>
                    {i.allergens.length ? (
                      <p className="mt-1 flex flex-wrap items-center gap-1.5 text-muted" aria-label={`${tc('allergens.contains')}: ${i.allergens.map((a) => tc(`allergens.${a}`)).join(', ')}`}>
                        {i.allergens.map((a) => (
                          <Icon key={a} name={a as IconName} size={13} />
                        ))}
                      </p>
                    ) : null}
                  </li>
                ))}
              </ul>
            </section>
          ))}
      </div>

      <footer className="mt-6 border-t border-ink pt-4 text-[0.8rem] text-muted">
        {used.length ? (
          <ul className="mb-3 flex flex-wrap gap-x-4 gap-y-1" aria-label={tc('allergens.title')}>
            {used.map((a) => (
              <li key={a} className="inline-flex items-center gap-1.5">
                <Icon name={a as IconName} size={13} />
                {tc(`allergens.${a}`)}
              </li>
            ))}
          </ul>
        ) : null}
        <p>{t('notes.allergens')}</p>
        <p className="mt-1">
          {t('print.footer')} · {t('print.printedAt', { date: formatDate(new Date(), locale, branch.timeZone, { day: 'numeric', month: 'long', year: 'numeric' }) })}
        </p>
      </footer>
    </main>
  );
}
