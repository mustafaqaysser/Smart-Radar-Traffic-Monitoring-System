import type { Metadata } from 'next';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import { Reveal } from '@/components/motion/reveal';
import { ArrowLink } from '@/components/site/ui/button';
import { MediaImage } from '@/components/site/ui/media-image';
import { PageHeader } from '@/components/site/ui/page-header';
import { SectionHeading } from '@/components/site/ui/section-heading';
import { tr } from '@/lib/i18n/localized';
import { getBranches } from '@/lib/queries/branches';
import { getTeam } from '@/lib/queries/content';
import { getSettings } from '@/lib/server/settings';
import { pageMetadata } from '@/lib/site/metadata';

export async function generateMetadata({ params }: { params: Promise<{ locale: string }> }): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: 'pages.team.meta' });
  return pageMetadata({ locale, path: '/team', title: t('title'), description: t('description'), image: 'team' });
}

export default async function TeamPage({ params }: { params: Promise<{ locale: string }> }) {
  const { locale } = await params;
  setRequestLocale(locale);
  const [t, team, branches, settings] = await Promise.all([getTranslations('pages.team'), getTeam(), getBranches(), getSettings()]);
  const [lead, ...rest] = team;
  return (
    <>
      <PageHeader eyebrow={t('eyebrow')} title={t('title')} intro={<p>{t('intro')}</p>} />
      {lead ? (
        <section className="site-grid items-end gap-y-10" aria-labelledby={`tm-${lead.id}`}>
          {lead.image ? (
            <Reveal variant="shade" className="col-span-full md:col-span-4 lg:col-span-5">
              <MediaImage media={lead.image} locale={locale} sizes="(min-width: 1024px) 40vw, (min-width: 768px) 50vw, 100vw" ratio="4/5" shape="arch-4x5" preload className="cast-shade" />
            </Reveal>
          ) : null}
          <div className="col-span-full flex flex-col gap-4 md:col-span-4 lg:col-span-6 lg:col-start-7">
            <p className="t-label text-accent">{tr(lead.role, locale)}</p>
            <h2 id={`tm-${lead.id}`} className="t-display-md">
              {tr(lead.name, locale)}
            </h2>
            <p className="t-body-lg">{tr(lead.bio, locale)}</p>
          </div>
        </section>
      ) : null}
      <ul className="site-grid gap-y-16 pt-[var(--spacing-section)]">
        {rest.map((m, i) => {
          const house = branches.find((b) => b.id === m.branchId);
          return (
            <Reveal as="li" key={m.id} delay={(i % 3) * 90} className="col-span-full flex flex-col gap-4 md:col-span-4 lg:col-span-4">
              {m.image ? <MediaImage media={m.image} locale={locale} sizes="(min-width: 1024px) 30vw, (min-width: 768px) 45vw, 100vw" ratio="3/4" shape="arch-3x4" /> : null}
              <p className="t-label text-muted">{house ? tr(house.shortName, locale) : t('both')}</p>
              <h2 className="t-heading-md">{tr(m.name, locale)}</h2>
              <p className="t-small text-accent">{tr(m.role, locale)}</p>
              <p className="t-body">{tr(m.bio, locale)}</p>
            </Reveal>
          );
        })}
      </ul>
      {settings.features.careers ? (
        <section className="site-grid pt-[var(--spacing-section)]" aria-labelledby="team-join">
          <SectionHeading id="team-join" className="col-span-full border-t border-ink pt-10 lg:col-span-8" title={t('join.title')} intro={<p>{t('join.body')}</p>} action={<ArrowLink href="/careers">{t('join.cta')}</ArrowLink>} />
        </section>
      ) : null}
    </>
  );
}
