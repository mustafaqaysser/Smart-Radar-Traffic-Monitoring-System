import type { Metadata } from 'next';
import { notFound } from 'next/navigation';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import { PageHeader } from '@/components/site/ui/page-header';
import { Link } from '@/i18n/navigation';
import { tr } from '@/lib/i18n/localized';
import { getBranches } from '@/lib/queries/branches';
import { getMediaIndex } from '@/lib/queries/catalog';
import { getOpenJobs } from '@/lib/queries/content';
import { featureEnabled, getSettings } from '@/lib/server/settings';
import { pageMetadata } from '@/lib/site/metadata';
import { MediaImage } from '@/components/site/ui/media-image';

export async function generateMetadata({ params }: { params: Promise<{ locale: string }> }): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: 'visit.careers.meta' });
  return pageMetadata({ locale, path: '/careers', title: t('title'), description: t('description'), image: 'careers' });
}

export default async function CareersPage({ params }: { params: Promise<{ locale: string }> }) {
  const { locale } = await params;
  setRequestLocale(locale);
  if (!(await featureEnabled('careers'))) notFound();
  const [t, jobs, branches, settings, media] = await Promise.all([getTranslations('visit.careers'), getOpenJobs(), getBranches(), getSettings(), getMediaIndex()]);
  const photo = media['craft-kneading'] ?? null;
  return (
    <>
      <PageHeader eyebrow={t('eyebrow')} title={t('title')} intro={<p>{t('intro')}</p>} aside={photo ? <MediaImage media={photo} locale={locale} sizes="(min-width: 1024px) 30vw, 100vw" ratio="3/4" shape="arch-3x4" preload /> : null} />
      <section className="site-grid gap-y-8" aria-labelledby="jobs-open">
        <h2 id="jobs-open" className="t-heading-lg col-span-full">
          {t('open')}
        </h2>
        {jobs.length === 0 ? (
          <p className="t-body-lg measure col-span-full">
            {t('none')}{' '}
            <a href={`mailto:${settings.contact.careers}`} dir="ltr" className="underline underline-offset-4">
              {settings.contact.careers}
            </a>
          </p>
        ) : (
          <ul className="col-span-full border-t border-ink">
            {jobs.map((j) => {
              const house = branches.find((b) => b.id === j.branchId);
              return (
                <li key={j.id} data-card className="relative grid gap-3 border-b border-line py-8 md:grid-cols-12 md:items-baseline">
                  <h3 className="t-heading-md md:col-span-6">
                    <Link href={`/careers/${j.slug}`} className="card-link">
                      {tr(j.title, locale)}
                    </Link>
                  </h3>
                  <p className="t-small text-muted md:col-span-3">
                    {house ? tr(house.shortName, locale) : t('both')} · {t(`employment.${j.employment}`)}
                  </p>
                  <p className="t-small md:col-span-3 md:text-end">{t('view')}</p>
                  <p className="t-body text-muted md:col-span-9">{tr(j.summary, locale)}</p>
                </li>
              );
            })}
          </ul>
        )}
      </section>
    </>
  );
}
