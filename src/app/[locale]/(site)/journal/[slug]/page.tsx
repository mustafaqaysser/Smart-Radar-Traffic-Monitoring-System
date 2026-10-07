import type { Metadata } from 'next';
import { notFound } from 'next/navigation';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import restaurantConfig from '@config';
import { SplitWords } from '@/components/motion/split-words';
import { JsonLd } from '@/components/site/json-ld';
import { JournalCard } from '@/components/site/journal-card';
import { ShareButton } from '@/components/site/share-button';
import { BackLink } from '@/components/site/ui/button';
import { MediaImage } from '@/components/site/ui/media-image';
import { formatDate, formatNumber } from '@/lib/i18n/format';
import { tr } from '@/lib/i18n/localized';
import { plural } from '@/lib/i18n/plural';
import { getJournal, getJournalPost } from '@/lib/queries/content';
import { sanitizeRichText } from '@/lib/server/sanitize';
import { pageMetadata } from '@/lib/site/metadata';
import { absoluteUrl } from '@/lib/site/url';

type Params = Promise<{ locale: string; slug: string }>;

export async function generateMetadata({ params }: { params: Params }): Promise<Metadata> {
  const { locale, slug } = await params;
  const post = await getJournalPost(slug);
  if (!post) return {};
  return pageMetadata({ locale, path: `/journal/${slug}`, title: tr(post.title, locale), description: tr(post.excerpt, locale), image: post.cover?.src });
}

export default async function JournalPostPage({ params }: { params: Params }) {
  const { locale, slug } = await params;
  setRequestLocale(locale);
  const [t, post, all] = await Promise.all([getTranslations('pages.journal'), getJournalPost(slug), getJournal()]);
  if (!post) notFound();
  const tz = restaurantConfig.defaultTimeZone;
  const title = tr(post.title, locale);
  const url = absoluteUrl(`/${locale}/journal/${slug}`);
  const more = all.filter((p) => p.slug !== slug).slice(0, 3);
  const labels = { recipe: t('recipe'), article: t('articles'), readingTime: (a: { count: number; n: string }) => t('readingTime', a) };
  const published = post.publishedAt ? new Date(post.publishedAt) : null;

  const jsonLd =
    post.kind === 'recipe' && post.recipe
      ? {
          '@context': 'https://schema.org',
          '@type': 'Recipe',
          name: title,
          description: tr(post.excerpt, locale),
          image: post.cover ? absoluteUrl(post.cover.src) : undefined,
          author: { '@type': 'Person', name: tr(post.author, locale) },
          datePublished: post.publishedAt ?? undefined,
          recipeYield: String(post.recipe.serves),
          totalTime: `PT${post.recipe.minutes}M`,
          recipeIngredient: post.recipe.ingredients.map((i) => tr(i, locale)),
          recipeInstructions: post.recipe.steps.map((s) => ({ '@type': 'HowToStep', text: tr(s, locale) })),
          inLanguage: locale,
        }
      : {
          '@context': 'https://schema.org',
          '@type': 'Article',
          headline: title,
          description: tr(post.excerpt, locale),
          image: post.cover ? absoluteUrl(post.cover.src) : undefined,
          author: { '@type': 'Person', name: tr(post.author, locale) },
          datePublished: post.publishedAt ?? undefined,
          dateModified: post.updatedAt,
          inLanguage: locale,
          mainEntityOfPage: url,
        };

  return (
    <article className="pt-8 lg:pt-12" aria-labelledby="post-title">
      <JsonLd data={jsonLd} />
      <div className="site-grid gap-y-10">
        <nav className="col-span-full">
          <BackLink href="/journal">{t('back')}</BackLink>
        </nav>
        <header className="col-span-full flex flex-col gap-6 lg:col-span-9">
          <p className="t-instrument text-muted">
            {post.kind === 'recipe' ? t('recipe') : t('articles')}
            {published ? ` · ${formatDate(published, locale, tz, { day: 'numeric', month: 'long', year: 'numeric' })}` : ''} · {t('readingTime', plural(post.readingMinutes, locale))}
          </p>
          <h1 id="post-title" className="t-display-lg">
            <SplitWords text={title} />
          </h1>
          <p className="t-body-lg measure">{tr(post.excerpt, locale)}</p>
          <p className="t-small text-muted">{t('by', { author: tr(post.author, locale) })}</p>
        </header>
        {post.cover ? (
          <div className="col-span-full">
            <MediaImage media={post.cover} locale={locale} sizes="100vw" ratio="16/9" preload />
          </div>
        ) : null}
        <div className="prose-zill col-span-full md:col-span-6 md:col-start-2 lg:col-span-7 lg:col-start-3" dangerouslySetInnerHTML={{ __html: sanitizeRichText(tr(post.body, locale)) }} />
        {post.recipe ? (
          <aside className="col-span-full bg-surface p-6 md:col-span-6 md:col-start-2 lg:col-span-7 lg:col-start-3 lg:p-10" aria-labelledby="recipe-card">
            <h2 id="recipe-card" className="t-heading-lg">
              {t('recipe')}
            </h2>
            <p className="t-instrument mt-3 text-muted">
              {t('serves', plural(post.recipe.serves, locale))} · {t('time', plural(post.recipe.minutes, locale))}
            </p>
            <h3 className="t-heading-sm mt-8 mb-4">{t('ingredients')}</h3>
            <ul className="t-body flex flex-col gap-2">
              {post.recipe.ingredients.map((i, idx) => (
                <li key={idx} className="border-b border-line pb-2">
                  {tr(i, locale)}
                </li>
              ))}
            </ul>
            <h3 className="t-heading-sm mt-8 mb-4">{t('method')}</h3>
            <ol className="t-body flex flex-col gap-4">
              {post.recipe.steps.map((s, idx) => (
                <li key={idx} className="grid grid-cols-[2.5rem_1fr] gap-2">
                  <span className="t-instrument pt-1 text-accent">{formatNumber(idx + 1, locale)}</span>
                  <span>{tr(s, locale)}</span>
                </li>
              ))}
            </ol>
          </aside>
        ) : null}
        <div className="col-span-full md:col-span-6 md:col-start-2 lg:col-span-7 lg:col-start-3">
          <ShareButton url={url} title={title} label={t('share')} copiedLabel={t('copied')} />
        </div>
      </div>
      {more.length ? (
        <section className="site-grid gap-y-10 pt-[var(--spacing-section)]" aria-labelledby="post-more">
          <h2 id="post-more" className="t-heading-lg col-span-full">
            {t('more')}
          </h2>
          <ul className="col-span-full grid gap-[var(--spacing-gutter)] md:grid-cols-3">
            {more.map((p) => (
              <li key={p.id}>
                <JournalCard post={p} locale={locale} labels={labels} timeZone={tz} />
              </li>
            ))}
          </ul>
        </section>
      ) : null}
    </article>
  );
}
