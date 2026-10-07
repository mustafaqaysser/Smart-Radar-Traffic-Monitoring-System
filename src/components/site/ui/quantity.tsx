'use client';

import { useLocale, useTranslations } from 'next-intl';
import { Icon } from '@/components/brand/icon';
import { formatNumber } from '@/lib/i18n/format';
import { cn } from '@/lib/utils/cn';

/** − n + stepper with an accessible name and live value. */
export function Quantity({ value, onChange, min = 1, max = 20, label, className, size = 'md' }: { value: number; onChange: (n: number) => void; min?: number; max?: number; label: string; className?: string; size?: 'sm' | 'md' }) {
  const t = useTranslations('common.a11y');
  const locale = useLocale();
  const box = size === 'sm' ? 'size-9' : 'size-11';
  return (
    <div role="group" aria-label={label} className={cn('inline-flex items-center border border-field', className)}>
      <button type="button" className={cn(box, 'inline-flex items-center justify-center disabled:opacity-40')} onClick={() => onChange(Math.max(min, value - 1))} disabled={value <= min} aria-label={t('decrease')}>
        <Icon name="minus" size={16} />
      </button>
      <output aria-live="polite" className="tabular min-w-8 text-center">
        {formatNumber(value, locale)}
      </output>
      <button type="button" className={cn(box, 'inline-flex items-center justify-center disabled:opacity-40')} onClick={() => onChange(Math.min(max, value + 1))} disabled={value >= max} aria-label={t('increase')}>
        <Icon name="plus" size={16} />
      </button>
    </div>
  );
}
