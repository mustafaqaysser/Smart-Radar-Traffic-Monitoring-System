import { Link } from '@/i18n/navigation';
import { formatDate } from '@/lib/i18n/format';
import { tr } from '@/lib/i18n/localized';
import { plural } from '@/lib/i18n/plural';
import type { JournalSummaryDTO } from '@/lib/queries/content';
import { MediaImage } from '@/components/site/ui/media-image';
import { cn } from '@/lib/utils/cn';

export function JournalCard({ post, locale, labels, large, timeZone }: { post: JournalSummaryDTO; locale: string; labels: { recipe: string; article: string; readingTime: (args: { count: number; n: string }) => string }; large?: boolean; timeZone: string }) {
  return (
    <article data-card className={cn('group relative flex flex-col gap-4', large && 'lg:grid lg:grid-cols-2 lg:items-end lg:gap-[var(--spacing-gutter)]')}>
      {post.cover ? (
        <MediaImage media={post.cover} locale={locale} sizes={large ? '(min-width: 1024px) 50vw, 100vw' : '(min-width: 1024px) 30vw, (min-width: 768px) 45vw, 100vw'} ratio={large ? '4/3' : '3/2'} imgClassName="transition-transform duration-[var(--dur-sun)] ease-[var(--ease-shade)] group-hover:scale-[1.03]" />
      ) : null}
      <div className="flex flex-col gap-3">
        <p className="t-instrument text-muted">
          {post.kind === 'recipe' ? labels.recipe : labels.article}
          {post.publishedAt ? ` · ${formatDate(new Date(post.publishedAt), locale, timeZone, { day: 'numeric', month: 'long', year: 'numeric' })}` : ''} · {labels.readingTime(plural(post.readingMinutes, locale))}
        </p>
        <h3 className={large ? 't-display-md' : 't-heading-md'}>
          <Link href={`/journal/${post.slug}`} className="card-link">
            {tr(post.title, locale)}
          </Link>
        </h3>
        <p className="t-body text-muted">{tr(post.excerpt, locale)}</p>
      </div>
    </article>
  );
}
