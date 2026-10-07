import type { Metadata } from 'next';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import { ErrorView } from '@/components/site/error-view';
import { ButtonLink } from '@/components/site/ui/button';
import { pageMetadata } from '@/lib/site/metadata';

export async function generateMetadata({ params }: { params: Promise<{ locale: string }> }): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: 'common.errors' });
  return pageMetadata({ locale, path: '/offline', title: t('offlineTitle'), noindex: true });
}

/** Shown by the service worker when a page is requested without a connection and is not cached. */
export default async function OfflinePage({ params }: { params: Promise<{ locale: string }> }) {
  const { locale } = await params;
  setRequestLocale(locale);
  const t = await getTranslations('common');
  return (
    <main id="main">
      <ErrorView title={t('errors.offlineTitle')} body={t('errors.offlineBody')}>
        <ButtonLink href="/menu">{t('actions.viewMenu')}</ButtonLink>
      </ErrorView>
    </main>
  );
}
