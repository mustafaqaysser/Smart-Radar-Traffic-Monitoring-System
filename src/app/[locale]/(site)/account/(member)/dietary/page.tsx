import type { Metadata } from 'next';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import { AccountHeader } from '@/components/site/account/account-header';
import { DietaryForm } from '@/components/site/account/dietary-form';
import { getCurrentUser } from '@/lib/auth/session';
import { toDietProfile } from '@/lib/menu/filter';
import { visibleItems } from '@/lib/menu/view';
import { getMenuCatalog } from '@/lib/queries/catalog';
import { getSettings } from '@/lib/server/settings';
import { pageMetadata } from '@/lib/site/metadata';
import { getSelectedBranch } from '@/lib/site/selection';
import { getServingContext } from '@/lib/site/serving';

type Params = Promise<{ locale: string }>;

export async function generateMetadata({ params }: { params: Params }): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: 'account.meta' });
  return pageMetadata({ locale, path: '/account/dietary', title: t('dietary'), noindex: true });
}

export default async function DietaryPage({ params }: { params: Params }) {
  const { locale } = await params;
  setRequestLocale(locale);
  const user = await getCurrentUser();
  if (!user) return null;
  const [t, catalog, settings, branch] = await Promise.all([getTranslations('account.dietary'), getMenuCatalog(), getSettings(), getSelectedBranch()]);
  // The live count covers the dishes on the menus served today at the guest's house.
  const serving = await getServingContext(branch);
  const onToday = new Set(serving.menus.flatMap((m) => m.categories.flatMap((c) => c.items)));
  const dishes = visibleItems(catalog, settings.features.alcohol)
    .filter((i) => onToday.has(i.slug))
    .map((i) => ({ dietary: i.dietary, allergens: i.allergens, spice: i.spiceLevel }));
  return (
    <>
      <AccountHeader title={t('title')} intro={t('intro')} />
      <DietaryForm initial={toDietProfile(user.dietary)} dishes={dishes} />
    </>
  );
}
