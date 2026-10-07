'use client';

import { useTranslations } from 'next-intl';
import { Icon } from '@/components/brand/icon';
import { useReorder } from '@/components/site/order/use-reorder';

export function ReorderButton({ number, token }: { number: string; token: string }) {
  const t = useTranslations('account.orders');
  const [pending, reorder] = useReorder(number, token);
  return (
    <button type="button" onClick={reorder} disabled={pending} className="t-small inline-flex min-h-11 items-center gap-2 underline decoration-line underline-offset-4">
      <Icon name="refresh" size={16} />
      {pending ? t('reordering') : t('reorder')}
    </button>
  );
}
