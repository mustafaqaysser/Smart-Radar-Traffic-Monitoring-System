import type { Metadata } from 'next';
import { notFound } from 'next/navigation';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import { ConciergeChat } from '@/components/site/concierge/concierge-chat';
import { PageHeader } from '@/components/site/ui/page-header';
import { conciergeEnabled } from '@/lib/server/concierge';
import { pageMetadata } from '@/lib/site/metadata';
import { siteUrl } from '@/lib/site/url';

type Params = Promise<{ locale: string }>;

export async function generateMetadata({ params }: { params: Params }): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: 'pages.concierge.meta' });
  return pageMetadata({ locale, path: '/concierge', title: t('title'), description: t('description'), noindex: true });
}

/** The AI concierge (only when the owner has turned it on and an Anthropic API key is set). */
export default async function ConciergePage({ params }: { params: Params }) {
  const { locale } = await params;
  setRequestLocale(locale);
  if (!(await conciergeEnabled())) notFound();
  const t = await getTranslations('pages.concierge');
  return (
    <>
      <PageHeader eyebrow={t('eyebrow')} title={t('title')} intro={<p>{t('intro')}</p>} />
      <div className="site-grid pb-[var(--spacing-section)]">
        <div className="col-span-full lg:col-span-8">
          <ConciergeChat origin={new URL(siteUrl()).origin} />
        </div>
      </div>
    </>
  );
}
