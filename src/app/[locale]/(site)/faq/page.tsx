import type { Metadata } from 'next';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import { FaqList } from '@/components/site/faq-list';
import { JsonLd } from '@/components/site/json-ld';
import { ButtonLink } from '@/components/site/ui/button';
import { PageHeader } from '@/components/site/ui/page-header';
import { normalizeForSearch } from '@/lib/i18n/arabic';
import { tr } from '@/lib/i18n/localized';
import { getFaqs } from '@/lib/queries/content';
import { pageMetadata } from '@/lib/site/metadata';

const ORDER = ['visiting', 'reservations', 'menu', 'ordering', 'events', 'gift-cards', 'loyalty', 'accessibility'];

export async function generateMetadata({ params }: { params: Promise<{ locale: string }> }): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: 'pages.faq.meta' });
  return pageMetadata({ locale, path: '/faq', title: t('title'), description: t('description'), image: 'faq' });
}

export default async function FaqPage({ params }: { params: Promise<{ locale: string }> }) {
  const { locale } = await params;
  setRequestLocale(locale);
  const [t, faqs] = await Promise.all([getTranslations('pages.faq'), getFaqs()]);
  const items = faqs.map((f) => ({
    id: f.id,
    category: f.category,
    question: tr(f.question, locale),
    answer: tr(f.answer, locale),
    search: normalizeForSearch([...Object.values(f.question), ...Object.values(f.answer)].join(' ')),
  }));
  const categories = ORDER.filter((c) => items.some((i) => i.category === c));
  return (
    <>
      <JsonLd
        data={{
          '@context': 'https://schema.org',
          '@type': 'FAQPage',
          mainEntity: items.map((i) => ({ '@type': 'Question', name: i.question, acceptedAnswer: { '@type': 'Answer', text: i.answer } })),
        }}
      />
      <PageHeader eyebrow={t('eyebrow')} title={t('title')} intro={<p>{t('intro')}</p>} />
      <div className="site-grid">
        <div className="col-span-full lg:col-span-9">
          <FaqList items={items} categories={categories} />
        </div>
        <aside className="col-span-full mt-16 flex flex-col gap-4 lg:col-span-3 lg:mt-0" aria-labelledby="faq-still">
          <h2 id="faq-still" className="t-heading-md">
            {t('still')}
          </h2>
          <ButtonLink href="/contact" variant="secondary">
            {t('contact')}
          </ButtonLink>
        </aside>
      </div>
    </>
  );
}
