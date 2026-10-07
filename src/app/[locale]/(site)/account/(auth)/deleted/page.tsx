import type { Metadata } from 'next';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import { AuthShell } from '@/components/site/account/auth-shell';
import { ButtonLink } from '@/components/site/ui/button';
import { getMediaIndex } from '@/lib/queries/catalog';
import { pageMetadata } from '@/lib/site/metadata';

type Params = Promise<{ locale: string }>;

export async function generateMetadata({ params }: { params: Params }): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: 'account.meta' });
  return pageMetadata({ locale, path: '/account/deleted', title: t('deleted'), noindex: true });
}

/** Where a guest lands after deleting their account (already signed out). */
export default async function AccountDeletedPage({ params }: { params: Params }) {
  const { locale } = await params;
  setRequestLocale(locale);
  const [t, media] = await Promise.all([getTranslations('account'), getMediaIndex()]);
  return (
    <AuthShell eyebrow={t('auth.eyebrow')} title={t('privacy.delete.doneTitle')} body={t('privacy.delete.done')} image={media['place-palm-shadow-2'] ?? null} locale={locale}>
      <ButtonLink href="/" variant="secondary">
        {t('privacy.delete.home')}
      </ButtonLink>
    </AuthShell>
  );
}
