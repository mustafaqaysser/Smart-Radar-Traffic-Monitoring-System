import type { Metadata } from 'next';
import { notFound } from 'next/navigation';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import { AccountHeader } from '@/components/site/account/account-header';
import { AddressBook } from '@/components/site/account/address-book';
import { getCurrentUser } from '@/lib/auth/session';
import { accountAddresses } from '@/lib/server/account';
import { featureEnabled } from '@/lib/server/settings';
import { pageMetadata } from '@/lib/site/metadata';

type Params = Promise<{ locale: string }>;

export async function generateMetadata({ params }: { params: Params }): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: 'account.meta' });
  return pageMetadata({ locale, path: '/account/addresses', title: t('addresses'), noindex: true });
}

export default async function AddressesPage({ params }: { params: Params }) {
  const { locale } = await params;
  setRequestLocale(locale);
  if (!(await featureEnabled('ordering'))) notFound();
  const user = await getCurrentUser();
  if (!user) return null;
  const [t, rows] = await Promise.all([getTranslations('account.addresses'), accountAddresses(user.id)]);
  return (
    <>
      <AccountHeader title={t('title')} intro={t('intro')} />
      <AddressBook addresses={rows.map(({ id, label, area, street, building, floor, notes, isDefault }) => ({ id, label, area, street, building, floor, notes, isDefault }))} />
    </>
  );
}
