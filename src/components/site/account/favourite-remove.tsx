'use client';

import { useTranslations } from 'next-intl';
import { useTransition } from 'react';
import { Icon } from '@/components/brand/icon';
import { toast } from '@/components/site/ui/toast';
import { useRouter } from '@/i18n/navigation';
import { toggleFavourite } from '@/lib/actions/site';

/** The filled heart on a saved dish: tap to let it go. */
export function FavouriteRemove({ slug, name }: { slug: string; name: string }) {
  const t = useTranslations('account.favourites');
  const router = useRouter();
  const [pending, start] = useTransition();
  return (
    <button
      type="button"
      disabled={pending}
      aria-label={t('remove', { name })}
      className="relative z-[1] inline-flex size-11 shrink-0 items-center justify-center rounded-full border border-line text-accent transition-colors hover-capable:hover:border-ink"
      onClick={() =>
        start(async () => {
          const res = await toggleFavourite(slug);
          if (res.ok) {
            toast(t('removed'));
            router.refresh();
          }
        })
      }
    >
      <Icon name="heart" size={18} className="fill-current" />
    </button>
  );
}
