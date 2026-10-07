import type { Metadata } from 'next';
import { notFound } from 'next/navigation';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import { desc, eq } from 'drizzle-orm';
import { Checkout } from '@/components/site/order/checkout';
import { PageHeader } from '@/components/site/ui/page-header';
import { getCurrentUser } from '@/lib/auth/session';
import { db } from '@/lib/db/client';
import { addresses } from '@/lib/db/schema';
import { tr } from '@/lib/i18n/localized';
import { checkoutBranch } from '@/lib/order/menu-data';
import { getBranches } from '@/lib/queries/branches';
import { getSettings } from '@/lib/server/settings';
import { pageMetadata } from '@/lib/site/metadata';
import { getSelectedBranch } from '@/lib/site/selection';

type Params = Promise<{ locale: string }>;

export async function generateMetadata({ params }: { params: Params }): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: 'order.checkout' });
  return pageMetadata({ locale, path: '/order/checkout', title: t('meta'), noindex: true });
}

export default async function CheckoutPage({ params }: { params: Params }) {
  const { locale } = await params;
  setRequestLocale(locale);
  const settings = await getSettings();
  if (!settings.features.ordering) notFound();
  const [t, branches, selected, user] = await Promise.all([getTranslations('order.checkout'), getBranches(), getSelectedBranch(), getCurrentUser()]);
  const branch = selected ?? branches[0];
  if (!branch) notFound();
  const [info, saved] = await Promise.all([
    checkoutBranch(branch, locale),
    user ? db.select().from(addresses).where(eq(addresses.userId, user.id)).orderBy(desc(addresses.isDefault), desc(addresses.createdAt)) : Promise.resolve([]),
  ]);
  const zoneIds = new Set(info.zones.map((z) => z.id));
  return (
    <>
      <PageHeader eyebrow={t('eyebrow')} title={t('title')} />
      <Checkout
        branch={info}
        houses={Object.fromEntries(branches.map((b) => [b.slug, tr(b.shortName, locale)]))}
        user={user ? { name: user.name, email: user.email, phone: user.phone ?? '' } : null}
        addresses={saved
          .filter((a) => !a.zoneId || zoneIds.has(a.zoneId))
          .map((a) => ({ id: a.id, label: a.label, zoneId: a.zoneId, area: a.area, street: a.street, building: a.building, floor: a.floor, notes: a.notes, lat: a.lat, lng: a.lng, isDefault: a.isDefault }))}
        tipPresets={settings.tips.presets}
        loyaltyEnabled={settings.features.loyalty}
        giftCardsEnabled={settings.features.giftCards}
      />
    </>
  );
}
