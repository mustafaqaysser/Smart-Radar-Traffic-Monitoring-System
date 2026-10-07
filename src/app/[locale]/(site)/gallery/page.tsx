import type { Metadata } from 'next';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import { DialGallery } from '@/components/site/dial-gallery';
import { PageHeader } from '@/components/site/ui/page-header';
import { tr } from '@/lib/i18n/localized';
import { imageView } from '@/lib/menu/view';
import { getGallery } from '@/lib/queries/content';
import { pageMetadata } from '@/lib/site/metadata';

export async function generateMetadata({ params }: { params: Promise<{ locale: string }> }): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: 'pages.gallery.meta' });
  return pageMetadata({ locale, path: '/gallery', title: t('title'), description: t('description'), image: 'gallery' });
}

export default async function GalleryPage({ params }: { params: Promise<{ locale: string }> }) {
  const { locale } = await params;
  setRequestLocale(locale);
  const [t, tp, gallery] = await Promise.all([getTranslations('pages.gallery'), getTranslations('common.phase'), getGallery()]);
  const items = gallery.flatMap((g) => {
    const image = imageView(g.media, locale);
    return image ? [{ id: g.id, image, caption: g.caption ? tr(g.caption, locale) : image.alt, hour: g.hour ? tp(g.hour) : '' }] : [];
  });
  return (
    <>
      <PageHeader eyebrow={t('eyebrow')} title={t('title')} intro={<p>{t('intro')}</p>} />
      <div className="site-wrap">
        <DialGallery items={items} />
      </div>
    </>
  );
}
