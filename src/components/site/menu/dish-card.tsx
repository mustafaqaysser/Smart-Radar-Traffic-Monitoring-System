import { Link } from '@/i18n/navigation';
import { tr } from '@/lib/i18n/localized';
import type { MenuItemDTO } from '@/lib/queries/types';
import { MediaImage } from '@/components/site/ui/media-image';
import { Price } from '@/components/site/ui/price';
import { cn } from '@/lib/utils/cn';

interface DishCardProps {
  item: MenuItemDTO;
  locale: string;
  branchId: string | null;
  labels: { soldOut: string; signature: string };
  sizes?: string;
  className?: string;
}

/** A dish in an arched window: photograph, name, a line of description and the house's price. */
export function DishCard({ item, locale, branchId, labels, sizes = '(min-width: 1024px) 24vw, (min-width: 768px) 40vw, 72vw', className }: DishCardProps) {
  const state = branchId ? item.branches[branchId] : undefined;
  const price = state?.price ?? item.price;
  return (
    <article data-card className={cn('group relative flex flex-col gap-4', className)}>
      <div className="relative">
        {item.image ? (
          <MediaImage media={item.image} locale={locale} sizes={sizes} ratio="4/5" shape="arch-4x5" imgClassName="transition-transform duration-[var(--dur-sun)] ease-[var(--ease-shade)] group-hover:scale-[1.03]" />
        ) : (
          <div className="arch-4x5 aspect-[4/5] bg-surface" aria-hidden="true" />
        )}
        {state?.soldOut ? <span className="t-label absolute inset-x-0 bottom-0 bg-ink/90 px-4 py-2 text-center text-bg">{labels.soldOut}</span> : null}
      </div>
      <div className="flex items-baseline justify-between gap-4">
        <h3 className="t-heading-sm">
          <Link href={`/menu/dish/${item.slug}`} className="card-link">
            {tr(item.name, locale)}
          </Link>
        </h3>
        <Price amount={price} locale={locale} className="t-small shrink-0 text-muted" />
      </div>
      <p className="t-small line-clamp-2 text-muted">{tr(item.description, locale)}</p>
      {item.isSignature ? <p className="t-label text-accent">{labels.signature}</p> : null}
    </article>
  );
}
