import type { Metadata } from 'next';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import { Reveal } from '@/components/motion/reveal';
import { SplitWords } from '@/components/motion/split-words';
import { ArrowLink, ButtonLink } from '@/components/site/ui/button';
import { MediaImage } from '@/components/site/ui/media-image';
import { PageHeader } from '@/components/site/ui/page-header';
import { formatNumber } from '@/lib/i18n/format';
import { getMediaIndex } from '@/lib/queries/catalog';
import type { MediaDTO } from '@/lib/queries/types';
import { pageMetadata } from '@/lib/site/metadata';
import { cn } from '@/lib/utils/cn';

export async function generateMetadata({ params }: { params: Promise<{ locale: string }> }): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: 'pages.story.meta' });
  return pageMetadata({ locale, path: '/story', title: t('title'), description: t('description'), image: 'story' });
}

const CHAPTERS: { key: string; photos: string[] }[] = [
  { key: 'courtesy', photos: ['place-arch-shadow'] },
  { key: 'houses', photos: ['place-coral-house', 'place-mudbrick'] },
  { key: 'hours', photos: ['place-palm-shadow-2'] },
  { key: 'sources', photos: ['ing-dates', 'ing-coffee-beans', 'ing-honey'] },
  { key: 'keep', photos: ['craft-server-hands'] },
];

const TIMELINE: { key: string; year: number }[] = [
  { key: 'alBalad', year: 2021 },
  { key: 'sundial', year: 2022 },
  { key: 'bread', year: 2023 },
  { key: 'wadiHanifah', year: 2024 },
  { key: 'harvest', year: 2025 },
];

export default async function StoryPage({ params }: { params: Promise<{ locale: string }> }) {
  const { locale } = await params;
  setRequestLocale(locale);
  const [t, media] = await Promise.all([getTranslations('pages.story'), getMediaIndex()]);
  const photo = (name: string): MediaDTO | null => media[name] ?? null;
  const hero = photo('place-courtyard-sun');

  return (
    <>
      <PageHeader
        eyebrow={t('eyebrow')}
        title={t('title')}
        intro={<p>{t('intro')}</p>}
        aside={hero ? <MediaImage media={hero} locale={locale} sizes="(min-width: 1024px) 30vw, (min-width: 768px) 38vw, 100vw" ratio="3/4" shape="arch-3x4" preload className="cast-shade" /> : null}
      />

      {CHAPTERS.map((c, i) => {
        const photos = c.photos.map(photo).filter((m): m is MediaDTO => m !== null);
        const flip = i % 2 === 1;
        return (
          <section key={c.key} className="site-grid items-center gap-y-10 pt-[var(--spacing-section)]" aria-labelledby={`story-${c.key}`}>
            <div className={cn('col-span-full flex flex-col gap-6 md:col-span-4 lg:col-span-5', flip ? 'md:col-start-5 lg:col-start-8' : 'lg:col-start-1')}>
              <p className="t-instrument text-muted">
                {formatNumber(i + 1, locale)} / {formatNumber(CHAPTERS.length, locale)}
              </p>
              <h2 id={`story-${c.key}`} className="t-display-md">
                <SplitWords text={t(`chapters.${c.key}.title`)} />
              </h2>
              <Reveal as="p" delay={120} className="t-body-lg">
                {t(`chapters.${c.key}.body`)}
              </Reveal>
            </div>
            <div
              className={cn(
                'col-span-full grid gap-[var(--spacing-gutter)] md:col-span-4 lg:col-span-6',
                flip ? 'md:col-start-1 md:row-start-1 lg:col-start-1' : 'lg:col-start-7',
                photos.length === 3 ? 'grid-cols-3' : photos.length === 2 ? 'grid-cols-2' : 'grid-cols-1',
              )}
            >
              {photos.map((m, pi) => (
                <Reveal key={m.id} variant="shade" delay={pi * 120} className={photos.length === 1 ? 'mx-auto w-4/5' : undefined}>
                  <MediaImage media={m} locale={locale} sizes={photos.length === 1 ? '(min-width: 1024px) 38vw, 80vw' : '(min-width: 1024px) 18vw, 40vw'} ratio={photos.length === 1 ? '4/5' : '2/3'} shape={photos.length === 1 ? 'arch-4x5' : 'arch-2x3'} />
                </Reveal>
              ))}
            </div>
          </section>
        );
      })}

      <section className="site-grid pt-[var(--spacing-section)]" aria-labelledby="story-timeline">
        <h2 id="story-timeline" className="t-heading-lg col-span-full mb-10">
          {t('timelineTitle')}
        </h2>
        <ol className="col-span-full grid gap-8 border-t border-ink pt-8 md:grid-cols-5">
          {TIMELINE.map((e, i) => (
            <Reveal as="li" key={e.key} delay={i * 80} className="flex flex-col gap-3">
              <span className="t-display-md tabular text-accent">{formatNumber(e.year, locale, { useGrouping: false })}</span>
              <span className="t-body">{t(`timeline.${e.key}`)}</span>
            </Reveal>
          ))}
        </ol>
      </section>

      <section className="site-grid gap-y-8 pt-[var(--spacing-section)]" aria-labelledby="story-cta">
        <h2 id="story-cta" className="t-display-md col-span-full lg:col-span-8">
          <SplitWords text={t('cta.title')} />
        </h2>
        <div className="col-span-full flex flex-wrap items-center gap-x-8 gap-y-4">
          <ButtonLink href="/reserve" size="lg">
            {t('cta.reserve')}
          </ButtonLink>
          <ArrowLink href="/team">{t('cta.team')}</ArrowLink>
        </div>
      </section>
    </>
  );
}
