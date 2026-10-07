'use client';

import { useTranslations } from 'next-intl';
import { Icon } from '@/components/brand/icon';
import { cart, useCart } from '@/lib/cart/store';
import { cn } from '@/lib/utils/cn';

/** Delivery or pickup — remembered with the basket. Only the ways the house offers right now are shown. */
export function ChannelToggle({ channels, className }: { channels: { delivery: boolean; pickup: boolean }; className?: string }) {
  const t = useTranslations('order');
  const c = useCart();
  const options = (['delivery', 'pickup'] as const).filter((ch) => channels[ch]);
  const current = options.includes(c.channel as 'delivery' | 'pickup') ? c.channel : options[0];
  if (!options.length) return null;
  return (
    <fieldset className={cn('min-w-0', className)}>
      <legend className="t-label mb-3 text-muted">{t('menu.how')}</legend>
      <div className="flex flex-wrap gap-2">
        {options.map((ch) => (
          <button
            key={ch}
            type="button"
            aria-pressed={current === ch}
            onClick={() => cart.setChannel(ch)}
            className={cn('inline-flex min-h-11 items-center gap-2 rounded-pill border px-5 text-[0.9375rem] transition-colors duration-[var(--dur-quick)]', current === ch ? 'border-ink bg-ink text-bg' : 'border-line hover-capable:hover:border-ink')}
          >
            <Icon name={ch === 'delivery' ? 'delivery' : 'bag'} size={18} />
            {t(`channels.${ch}`)}
          </button>
        ))}
      </div>
    </fieldset>
  );
}
