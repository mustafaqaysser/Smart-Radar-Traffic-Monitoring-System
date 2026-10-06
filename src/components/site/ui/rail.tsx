'use client';

import { useTranslations } from 'next-intl';
import { useRef, type ReactNode } from 'react';
import { Icon } from '@/components/brand/icon';
import { cn } from '@/lib/utils/cn';

/**
 * A horizontal, snap-scrolling rail. Native scrolling (touch, trackpad, keyboard); previous/next buttons on
 * wide screens. Mirrors in RTL.
 */
export function Rail({ children, label, className }: { children: ReactNode; label: string; className?: string }) {
  const t = useTranslations('common.a11y');
  const ref = useRef<HTMLUListElement>(null);
  const page = (direction: 1 | -1) => {
    const el = ref.current;
    if (!el) return;
    const rtl = getComputedStyle(el).direction === 'rtl';
    const reduced = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
    el.scrollBy({ left: direction * (rtl ? -1 : 1) * el.clientWidth * 0.8, behavior: reduced ? 'auto' : 'smooth' });
  };
  return (
    <div className={cn('relative', className)}>
      <ul ref={ref} aria-label={label} className="no-scrollbar flex snap-x snap-mandatory gap-[var(--spacing-gutter)] overflow-x-auto scroll-px-[var(--spacing-margin)] px-[var(--spacing-margin)] pb-4" data-lenis-prevent-horizontal>
        {children}
      </ul>
      <div className="site-wrap mt-6 hidden justify-end gap-2 md:flex">
        <button type="button" onClick={() => page(-1)} className="inline-flex size-12 items-center justify-center border border-line hover-capable:hover:border-ink" aria-label={t('previous')}>
          <Icon name="arrowBack" size={20} />
        </button>
        <button type="button" onClick={() => page(1)} className="inline-flex size-12 items-center justify-center border border-line hover-capable:hover:border-ink" aria-label={t('next')}>
          <Icon name="arrow" size={20} />
        </button>
      </div>
    </div>
  );
}
