import type { Metadata } from 'next';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import { AccountHeader } from '@/components/site/account/account-header';
import { PreferencesForm } from '@/components/site/account/preferences-form';
import { getCurrentUser } from '@/lib/auth/session';
import { pageMetadata } from '@/lib/site/metadata';

type Params = Promise<{ locale: string }>;

export async function generateMetadata({ params }: { params: Params }): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: 'account.meta' });
  return pageMetadata({ locale, path: '/account/preferences', title: t('preferences'), noindex: true });
}

export default async function PreferencesPage({ params }: { params: Params }) {
  const { locale } = await params;
  setRequestLocale(locale);
  const user = await getCurrentUser();
  if (!user) return null;
  const t = await getTranslations('account.preferences');
  return (
    <>
      <AccountHeader title={t('title')} intro={t('intro')} />
      <div className="border-t border-ink pt-6">
        <PreferencesForm initial={{ marketingEmail: user.marketingEmail, marketingSms: user.marketingSms }} />
      </div>
    </>
  );
}
