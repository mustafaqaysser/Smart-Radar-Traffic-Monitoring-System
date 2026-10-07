import type { Metadata } from 'next';
import { notFound } from 'next/navigation';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import restaurantConfig from '@config';
import { SplitWords } from '@/components/motion/split-words';
import { ApplyForm } from '@/components/site/apply-form';
import { JsonLd } from '@/components/site/json-ld';
import { BackLink } from '@/components/site/ui/button';
import { tr } from '@/lib/i18n/localized';
import { getBranches } from '@/lib/queries/branches';
import { getOpenJobs } from '@/lib/queries/content';
import { sanitizeRichText } from '@/lib/server/sanitize';
import { pageMetadata } from '@/lib/site/metadata';
import { absoluteUrl } from '@/lib/site/url';

type Params = Promise<{ locale: string; slug: string }>;

export async function generateMetadata({ params }: { params: Params }): Promise<Metadata> {
  const { locale, slug } = await params;
  const job = (await getOpenJobs()).find((j) => j.slug === slug);
  if (!job) return {};
  return pageMetadata({ locale, path: `/careers/${slug}`, title: tr(job.title, locale), description: tr(job.summary, locale), image: 'careers' });
}

const EMPLOYMENT: Record<string, string> = { full_time: 'FULL_TIME', part_time: 'PART_TIME', seasonal: 'TEMPORARY' };

export default async function CareerPage({ params }: { params: Params }) {
  const { locale, slug } = await params;
  setRequestLocale(locale);
  const [t, jobs, branches] = await Promise.all([getTranslations('visit.careers'), getOpenJobs(), getBranches()]);
  const job = jobs.find((j) => j.slug === slug);
  if (!job) notFound();
  const house = branches.find((b) => b.id === job.branchId);
  const place = house ?? branches[0];
  return (
    <article className="site-grid gap-y-10 pt-8 lg:pt-12" aria-labelledby="job-title">
      <JsonLd
        data={{
          '@context': 'https://schema.org',
          '@type': 'JobPosting',
          title: tr(job.title, locale),
          description: sanitizeRichText(tr(job.description, locale)),
          employmentType: EMPLOYMENT[job.employment] ?? 'FULL_TIME',
          hiringOrganization: { '@type': 'Organization', name: tr(restaurantConfig.legalName, locale), sameAs: absoluteUrl('/') },
          jobLocation: place ? { '@type': 'Place', address: { '@type': 'PostalAddress', streetAddress: tr(place.address, locale), addressLocality: tr(place.city, locale), addressCountry: restaurantConfig.country } } : undefined,
          datePosted: new Date().toISOString().slice(0, 10),
          url: absoluteUrl(`/${locale}/careers/${slug}`),
        }}
      />
      <nav className="col-span-full">
        <BackLink href="/careers">{t('back')}</BackLink>
      </nav>
      <header className="col-span-full flex flex-col gap-4 lg:col-span-8">
        <p className="t-label text-muted">
          {house ? tr(house.shortName, locale) : t('both')} · {t(`employment.${job.employment}`)}
        </p>
        <h1 id="job-title" className="t-display-lg">
          <SplitWords text={tr(job.title, locale)} />
        </h1>
        <p className="t-body-lg measure">{tr(job.summary, locale)}</p>
      </header>
      <div className="prose-zill col-span-full lg:col-span-6" dangerouslySetInnerHTML={{ __html: sanitizeRichText(tr(job.description, locale)) }} />
      <section className="col-span-full flex flex-col gap-6 lg:col-span-5 lg:col-start-8" aria-labelledby="job-apply">
        <h2 id="job-apply" className="t-display-md">
          {t('apply')}
        </h2>
        <p className="t-body text-muted">{t('applyIntro')}</p>
        <ApplyForm posting={job.slug} />
      </section>
    </article>
  );
}
