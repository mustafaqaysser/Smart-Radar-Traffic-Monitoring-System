'use client';

import { useLocale, useTranslations } from 'next-intl';
import { useRouter } from 'next/navigation';
import { useTransition } from 'react';
import { toast } from '@/components/site/ui/toast';
import { reorderAction } from '@/lib/actions/orders';
import { cart } from '@/lib/cart/store';
import { lineKey } from '@/lib/cart/types';
import { plural } from '@/lib/i18n/plural';

/** "Order this again": puts the order's dishes that can still be ordered back in the basket and opens it. */
export function useReorder(number: string, token: string): [boolean, () => void] {
  const t = useTranslations('order.track');
  const tf = useTranslations('forms');
  const locale = useLocale();
  const router = useRouter();
  const [pending, start] = useTransition();
  const reorder = () =>
    start(async () => {
      const res = await reorderAction({ number, token });
      if (!res.ok) {
        toast(tf.has(`errors.${res.error}`) ? tf(`errors.${res.error}`) : tf('errors.unknown'));
        return;
      }
      cart.replace(
        res.data.lines.map((l) => ({ ...l, key: lineKey(l.slug, l.optionIds, l.note) })),
        res.data.branch,
        res.data.channel,
      );
      if (res.data.skipped) toast(t('reorderSkipped', plural(res.data.skipped, locale)));
      router.push(`/${locale}/order/cart`);
    });
  return [pending, reorder];
}
