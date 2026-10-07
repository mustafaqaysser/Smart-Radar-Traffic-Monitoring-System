import type { Metadata } from 'next';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import restaurantConfig from '@config';
import { Reveal } from '@/components/motion/reveal';
import { JournalCard } from '@/components/site/journal-card';
import { PageHeader } from '@/components/site/ui/page-header';
import { Link } from '@/i18n/navigation';
import { getJournal } from '@/lib/queries/content';
import { pageMetadata } from '@/lib/site/metadata';
import { cn } from '@/lib/utils/cn';

export async function generateMetadata({ params }: { params: Promise<{ locale: string }> }): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: 'pages.journal.meta' });
  return pageMetadata({ locale, path: '/journal', title: t('title'), description: t('description'), image: 'journal' });
}

const KINDS = ['all', 'article', 'recipe'] as const;

export default async function JournalPage({ params, searchParams }: { params: Promise<{ locale: string }>; searchParams: Promise<{ kind?: string }> }) {
  const { locale } = await params;
  const { kind } = await searchParams;
  setRequestLocale(locale);
  const [t, posts] = await Promise.all([getTranslations('pages.journal'), getJournal()]);
  const active = KINDS.find((k) => k === kind) ?? 'all';
  const list = active === 'all' ? posts : posts.filter((p) => p.kind === active);
  const [first, ...rest] = list;
  const labels = { recipe: t('recipe'), article: t('articles'), readingTime: (a: { count: number; n: string }) => t('readingTime', a) };
  return (
    <>
      <PageHeader eyebrow={t('eyebrow')} title={t('title')} intro={<p>{t('intro')}</p>}>
        <nav aria-label={t('eyebrow')} className="flex flex-wrap gap-2">
          {KINDS.map((k) => (
            <Link
              key={k}
              href={k === 'all' ? '/journal' : { pathname: '/journal', query: { kind: k } }}
              aria-current={active === k ? 'page' : undefined}
              className={cn('inline-flex min-h-11 items-center rounded-pill border px-4 text-[0.9375rem]', active === k ? 'border-ink bg-ink text-bg' : 'border-line hover-capable:hover:border-ink')}
            >
              {k === 'all' ? t('all') : k === 'article' ? t('articles') : t('recipes')}
            </Link>
          ))}
        </nav>
      </PageHeader>
      <div className="site-grid gap-y-16">
        {first ? (
          <div className="col-span-full">
            <JournalCard post={first} locale={locale} labels={labels} large timeZone={restaurantConfig.defaultTimeZone} />
          </div>
        ) : null}
        <ul className="col-span-full grid gap-x-[var(--spacing-gutter)] gap-y-16 md:grid-cols-2 lg:grid-cols-3">
          {rest.map((p, i) => (
            <Reveal as="li" key={p.id} delay={(i % 3) * 90}>
              <JournalCard post={p} locale={locale} labels={labels} timeZone={restaurantConfig.defaultTimeZone} />
            </Reveal>
          ))}
        </ul>
      </div>
    </>
  );
}
