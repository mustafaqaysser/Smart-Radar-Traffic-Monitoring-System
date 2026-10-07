import type { Metadata } from 'next';
import { notFound } from 'next/navigation';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import { Icon } from '@/components/brand/icon';
import { NewsletterForm } from '@/components/site/chrome/newsletter-form';
import { PageHeader } from '@/components/site/ui/page-header';
import { getMediaIndex } from '@/lib/queries/catalog';
import { featureEnabled } from '@/lib/server/settings';
import { pageMetadata } from '@/lib/site/metadata';
import { MediaImage } from '@/components/site/ui/media-image';

export async function generateMetadata({ params }: { params: Promise<{ locale: string }> }): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: 'visit.newsletter.meta' });
  return pageMetadata({ locale, path: '/newsletter', title: t('title'), description: t('description') });
}

export default async function NewsletterPage({ params, searchParams }: { params: Promise<{ locale: string }>; searchParams: Promise<{ status?: string }> }) {
  const { locale } = await params;
  const { status } = await searchParams;
  setRequestLocale(locale);
  if (!(await featureEnabled('newsletter'))) notFound();
  const [t, tf, media] = await Promise.all([getTranslations('visit.newsletter'), getTranslations('forms.newsletter'), getMediaIndex()]);
  const notice =
    status === 'confirmed'
      ? { title: tf('confirmedTitle'), body: tf('confirmedBody') }
      : status === 'unsubscribed'
        ? { title: tf('unsubscribedTitle'), body: tf('unsubscribedBody') }
        : status === 'invalid'
          ? { title: tf('invalidTitle'), body: tf('invalidBody') }
          : null;
  const photo = media['ing-dates'] ?? null;
  return (
    <>
      <PageHeader eyebrow={t('eyebrow')} title={notice?.title ?? t('title')} intro={<p>{notice?.body ?? t('intro')}</p>} aside={photo ? <MediaImage media={photo} locale={locale} sizes="(min-width: 1024px) 30vw, 100vw" ratio="3/4" shape="arch-3x4" preload /> : null}>
        {notice ? (
          <p className="t-small flex items-center gap-2 text-muted" role="status">
            <Icon name={status === 'invalid' ? 'info' : 'check'} size={18} />
            {status === 'confirmed' ? t('promise') : t('intro')}
          </p>
        ) : null}
      </PageHeader>
      {status !== 'confirmed' ? (
        <section className="site-grid" aria-label={t('title')}>
          <div className="col-span-full flex flex-col gap-6 md:col-span-6 lg:col-span-6">
            <NewsletterForm source="page" />
            <p className="t-small text-muted">{t('promise')}</p>
          </div>
        </section>
      ) : null}
    </>
  );
}
