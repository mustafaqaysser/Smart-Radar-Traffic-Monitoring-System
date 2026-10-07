import type { Metadata } from 'next';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import manifest from '@/content/media.json';
import { PageHeader } from '@/components/site/ui/page-header';
import { tr } from '@/lib/i18n/localized';
import { pageMetadata } from '@/lib/site/metadata';

interface ManifestEntry {
  kind?: string;
  alt?: { ar?: string; en?: string };
  credit?: { author: string; authorUrl?: string | null; source: string; sourceUrl?: string | null; license: string; licenseUrl?: string | null } | null;
}

export async function generateMetadata({ params }: { params: Promise<{ locale: string }> }): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: 'pages.credits.meta' });
  return pageMetadata({ locale, path: '/credits', title: t('title'), description: t('description') });
}

export default async function CreditsPage({ params }: { params: Promise<{ locale: string }> }) {
  const { locale } = await params;
  setRequestLocale(locale);
  const t = await getTranslations('pages.credits');
  const tc = await getTranslations('common.a11y');
  const rows = Object.entries(manifest as Record<string, ManifestEntry>)
    .filter(([, m]) => m.credit)
    .map(([name, m]) => ({ name, alt: m.alt ? tr(m.alt, locale) : name, credit: m.credit as NonNullable<ManifestEntry['credit']> }));
  return (
    <>
      <PageHeader eyebrow={t('eyebrow')} title={t('title')} intro={<p>{t('intro')}</p>} />
      <section className="site-grid gap-y-6" aria-labelledby="credits-photos">
        <h2 id="credits-photos" className="t-heading-lg col-span-full">
          {t('photos')}
        </h2>
        <div className="col-span-full overflow-x-auto" data-lenis-prevent-horizontal>
          <table className="t-small w-full min-w-[40rem] border-t border-ink text-start">
            <thead>
              <tr className="border-b border-line text-muted">
                <th scope="col" className="py-3 pe-4 text-start font-normal">{t('photo')}</th>
                <th scope="col" className="py-3 pe-4 text-start font-normal">{t('author')}</th>
                <th scope="col" className="py-3 pe-4 text-start font-normal">{t('source')}</th>
                <th scope="col" className="py-3 text-start font-normal">{t('license')}</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => (
                <tr key={r.name} className="border-b border-line align-top">
                  <td className="py-3 pe-4">{r.alt}</td>
                  <td className="py-3 pe-4">
                    {r.credit.authorUrl ? (
                      <a href={r.credit.authorUrl} target="_blank" rel="noopener noreferrer" className="underline decoration-line underline-offset-4">
                        <bdi>{r.credit.author}</bdi>
                        <span className="sr-only"> ({tc('newWindow')})</span>
                      </a>
                    ) : (
                      <bdi>{r.credit.author}</bdi>
                    )}
                  </td>
                  <td className="py-3 pe-4">
                    {r.credit.sourceUrl ? (
                      <a href={r.credit.sourceUrl} target="_blank" rel="noopener noreferrer" className="underline decoration-line underline-offset-4">
                        <bdi>{r.credit.source}</bdi>
                        <span className="sr-only"> ({tc('newWindow')})</span>
                      </a>
                    ) : (
                      <bdi>{r.credit.source}</bdi>
                    )}
                  </td>
                  <td className="py-3">
                    {r.credit.licenseUrl ? (
                      <a href={r.credit.licenseUrl} target="_blank" rel="noopener noreferrer" className="underline decoration-line underline-offset-4">
                        <bdi>{r.credit.license}</bdi>
                        <span className="sr-only"> ({tc('newWindow')})</span>
                      </a>
                    ) : (
                      <bdi>{r.credit.license}</bdi>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
      <section className="site-grid gap-y-4 pt-[var(--spacing-stack)]" aria-labelledby="credits-fonts">
        <h2 id="credits-fonts" className="t-heading-lg col-span-full">
          {t('fonts')}
        </h2>
        <p className="t-body measure col-span-full">{t('fontsBody')}</p>
        <p className="t-body measure col-span-full text-muted">{t('original')}</p>
      </section>
    </>
  );
}
