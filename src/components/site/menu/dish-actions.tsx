'use client';

import { useTranslations } from 'next-intl';
import { useState, useTransition } from 'react';
import { Icon } from '@/components/brand/icon';
import { Button } from '@/components/site/ui/button';
import { toast } from '@/components/site/ui/toast';
import { Link } from '@/i18n/navigation';
import { toggleFavourite } from '@/lib/actions/site';
import type { ItemView } from '@/lib/menu/view';
import { QuickAdd } from './quick-add';

interface DishActionsProps {
  item: ItemView;
  canOrder: boolean;
  branch: { slug: string; name: string };
  signedIn: boolean;
  initialFavourite: boolean;
}

/** Add to order (with modifiers) and save to favourites, on the dish page. */
export function DishActions({ item, canOrder, branch, signedIn, initialFavourite }: DishActionsProps) {
  const t = useTranslations('menu.dish');
  const [adding, setAdding] = useState(false);
  const [favourite, setFavourite] = useState(initialFavourite);
  const [pending, start] = useTransition();
  const blocked = !item.available || item.soldOut || !item.orderable;

  return (
    <div className="flex flex-wrap items-center gap-4">
      {canOrder && !blocked ? (
        <Button size="lg" icon={null} leadingIcon="plus" onClick={() => setAdding(true)}>
          {t('addToOrder')}
        </Button>
      ) : null}
      {signedIn ? (
        <Button
          variant="secondary"
          size="lg"
          icon={null}
          aria-pressed={favourite}
          disabled={pending}
          onClick={() =>
            start(async () => {
              const res = await toggleFavourite(item.slug);
              if (res.ok) {
                setFavourite(res.data.favourite);
                toast(res.data.favourite ? t('saved') : t('removed'));
              }
            })
          }
        >
          <span className="inline-flex items-center gap-2">
            <Icon name="heart" size={18} className={favourite ? 'fill-current' : undefined} />
            {favourite ? t('unfavourite') : t('favourite')}
          </span>
        </Button>
      ) : (
        <Link href={{ pathname: '/account/sign-in', query: { next: `/menu/dish/${item.slug}` } }} className="t-small inline-flex min-h-11 items-center gap-2 underline decoration-line underline-offset-4">
          <Icon name="heart" size={18} />
          {t('favouriteSignIn')}
        </Link>
      )}
      {canOrder ? <QuickAdd item={adding ? item : null} branchSlug={branch.slug} branchName={branch.name} onClose={() => setAdding(false)} /> : null}
    </div>
  );
}
