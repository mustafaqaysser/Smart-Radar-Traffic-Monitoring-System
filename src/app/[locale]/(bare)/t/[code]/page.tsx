import type { Metadata } from 'next';
import { notFound } from 'next/navigation';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import { Monogram } from '@/components/brand/logo';
import { LocaleSwitch } from '@/components/site/chrome/locale-switch';
import { TableExperience } from '@/components/site/table/table-experience';
import { Link } from '@/i18n/navigation';
import { getCurrentUser } from '@/lib/auth/session';
import { formatList } from '@/lib/i18n/format';
import { tr } from '@/lib/i18n/localized';
import { toDietProfile } from '@/lib/menu/filter';
import { orderMenuData } from '@/lib/order/menu-data';
import { kitchenStatus } from '@/lib/server/order-quote';
import { getSettings } from '@/lib/server/settings';
import { tableActivity, tableByCode } from '@/lib/server/table';
import { pageMetadata } from '@/lib/site/metadata';

type Params = Promise<{ locale: string; code: string }>;

export async function generateMetadata({ params }: { params: Params }): Promise<Metadata> {
  const { locale, code } = await params;
  const found = await tableByCode(code);
  if (!found) return {};
  const t = await getTranslations({ locale, namespace: 'table' });
  return pageMetadata({ locale, path: `/t/${found.table.code}`, title: t('meta', { table: found.table.label, house: tr(found.branch.shortName, locale) }), noindex: true });
}

/** The page a table's QR code opens: no site chrome, just this table's menu, orders and calls. */
export default async function TablePage({ params }: { params: Params }) {
  const { locale, code } = await params;
  setRequestLocale(locale);
  const settings = await getSettings();
  if (!settings.features.dineInQr) notFound();
  const found = await tableByCode(code);
  if (!found) notFound();
  const { table, branch } = found;
  const now = new Date();
  const [t, data, kitchen, activity, user] = await Promise.all([getTranslations('table'), orderMenuData(branch, locale), kitchenStatus(branch, now), tableActivity(table, now), getCurrentUser()]);
  // At the table only what is served now (and the all-day lists) is offered.
  const menus = data.menus.filter((m) => m.servingNow || m.window === null);
  const serving = data.menus.filter((m) => m.servingNow && m.window !== null).map((m) => m.name);
  const canOrder = settings.features.ordering && branch.dineInEnabled && kitchen.open && !kitchen.paused;
  const house = tr(branch.shortName, locale);

  return (
    <main id="main" className="mx-auto flex min-h-dvh w-full max-w-2xl flex-col gap-8 px-[var(--spacing-margin)] pt-6">
      <header className="flex items-center justify-between gap-4 border-b border-line pb-4">
        <Link href="/" aria-label={tr(branch.name, locale)} className="inline-flex">
          <Monogram decorative shade className="h-10 w-auto" />
        </Link>
        <p className="t-label text-muted">
          {t('eyebrow', { table: table.label, house })}
        </p>
        <LocaleSwitch />
      </header>
      <section className="flex flex-col gap-3">
        <h1 className="t-display-md">{t('title')}</h1>
        <p className="t-body measure text-muted">{t('intro')}</p>
        <p className="t-small flex items-center gap-2">
          <span aria-hidden="true" className={serving.length ? 'size-2 rounded-full bg-success' : 'size-2 rounded-full bg-muted'} />
          {serving.length ? t('servingNow', { menus: formatList(serving, locale) }) : t('resting')}
        </p>
      </section>
      <TableExperience code={table.code} house={{ slug: branch.slug, name: house }} menus={menus} items={data.items} canOrder={canOrder} profile={toDietProfile(user?.dietary)} initialActivity={activity} />
    </main>
  );
}
