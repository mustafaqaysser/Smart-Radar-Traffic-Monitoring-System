import type { Metadata } from 'next';
import { notFound } from 'next/navigation';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import { CartReview } from '@/components/site/order/cart-review';
import { PageHeader } from '@/components/site/ui/page-header';
import { tr } from '@/lib/i18n/localized';
import { orderMenuData } from '@/lib/order/menu-data';
import { getBranches } from '@/lib/queries/branches';
import { getSettings } from '@/lib/server/settings';
import { pageMetadata } from '@/lib/site/metadata';
import { getSelectedBranch } from '@/lib/site/selection';

type Params = Promise<{ locale: string }>;

export async function generateMetadata({ params }: { params: Params }): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: 'order.basket' });
  return pageMetadata({ locale, path: '/order/cart', title: t('title'), noindex: true });
}

export default async function CartPage({ params }: { params: Params }) {
  const { locale } = await params;
  setRequestLocale(locale);
  if (!(await getSettings()).features.ordering) notFound();
  const [t, tb, branches, selected] = await Promise.all([getTranslations('order'), getTranslations('order.basket'), getBranches(), getSelectedBranch()]);
  const branch = selected ?? branches[0];
  if (!branch) notFound();
  const data = await orderMenuData(branch, locale);
  return (
    <>
      <PageHeader eyebrow={t('eyebrow')} title={tb('title')} />
      <CartReview items={data.items} upsell={data.upsell} branch={{ slug: branch.slug, name: tr(branch.shortName, locale) }} houses={Object.fromEntries(branches.map((b) => [b.slug, tr(b.shortName, locale)]))} />
    </>
  );
}
