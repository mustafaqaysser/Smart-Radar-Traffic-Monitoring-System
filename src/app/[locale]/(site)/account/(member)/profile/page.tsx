import type { Metadata } from 'next';
import { and, eq } from 'drizzle-orm';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import { AccountHeader, LedgerBlock } from '@/components/site/account/account-header';
import { PasswordPanel, ProfileForm, RevokeSessions } from '@/components/site/account/profile-forms';
import { Link } from '@/i18n/navigation';
import { getCurrentUser } from '@/lib/auth/session';
import { db } from '@/lib/db/client';
import { accounts } from '@/lib/db/schema';
import { pageMetadata } from '@/lib/site/metadata';

type Params = Promise<{ locale: string }>;

export async function generateMetadata({ params }: { params: Params }): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: 'account.meta' });
  return pageMetadata({ locale, path: '/account/profile', title: t('profile'), noindex: true });
}

export default async function ProfilePage({ params }: { params: Params }) {
  const { locale } = await params;
  setRequestLocale(locale);
  const user = await getCurrentUser();
  if (!user) return null;
  const [t, credential] = await Promise.all([getTranslations('account.profile'), db.query.accounts.findFirst({ where: and(eq(accounts.userId, user.id), eq(accounts.providerId, 'credential')) })]);
  return (
    <>
      <AccountHeader title={t('title')} intro={t('intro')} />
      <LedgerBlock title={t('contact')}>
        <div className="flex flex-col gap-8">
          <div className="flex flex-col gap-1">
            <p className="t-label text-muted">{t('email')}</p>
            <p className="t-body-lg">
              <bdi dir="ltr">{user.email}</bdi>
            </p>
            <p className="t-small text-muted">
              {t('emailNote')}{' '}
              <Link href={{ pathname: '/contact', query: { subject: 'general' } }} className="underline underline-offset-4">
                {t('contact')}
              </Link>
            </p>
          </div>
          <ProfileForm initial={{ name: user.name, phone: user.phone ?? '', locale: user.locale }} />
        </div>
      </LedgerBlock>
      <LedgerBlock title={t('password.title')}>
        <PasswordPanel hasPassword={Boolean(credential?.password)} />
      </LedgerBlock>
      <LedgerBlock title={t('sessions.title')}>
        <RevokeSessions />
      </LedgerBlock>
    </>
  );
}
