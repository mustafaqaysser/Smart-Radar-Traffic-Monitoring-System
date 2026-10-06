'use client';

import { useTranslations } from 'next-intl';
import { useRouter } from 'next/navigation';
import { useTransition } from 'react';
import { selectBranch } from '@/lib/actions/site';
import { cart } from '@/lib/cart/store';
import { cn } from '@/lib/utils/cn';

export interface BranchOption {
  slug: string;
  name: string;
  city: string;
}

/** Segmented choice between houses. Changing it re-renders the page for that house's sun and menu. */
export function BranchSwitch({ branches, selected, className, tone = 'default' }: { branches: BranchOption[]; selected: string | null; className?: string; tone?: 'default' | 'large' }) {
  const t = useTranslations('common.branch');
  const router = useRouter();
  const [pending, start] = useTransition();
  if (branches.length < 2) return null;
  return (
    <fieldset className={cn('min-w-0', className)} aria-busy={pending || undefined}>
      <legend className="t-label mb-3 text-muted">{t('choose')}</legend>
      <div className="flex flex-wrap gap-2">
        {branches.map((b) => {
          const active = b.slug === selected;
          return (
            <button
              key={b.slug}
              type="button"
              aria-pressed={active}
              disabled={pending}
              onClick={() =>
                start(async () => {
                  const res = await selectBranch(b.slug);
                  if (res.ok) {
                    cart.setBranch(b.slug);
                    router.refresh();
                  }
                })
              }
              className={cn(
                'flex min-h-11 flex-col items-start justify-center rounded-pill border px-5 py-2 text-start transition-colors duration-[var(--dur-quick)]',
                active ? 'border-ink bg-ink text-bg' : 'border-line hover-capable:hover:border-ink',
                tone === 'large' && 'rounded-none px-6 py-4',
              )}
            >
              <span className={tone === 'large' ? 't-heading-sm' : 'text-[0.9375rem] leading-tight'}>{b.name}</span>
              {tone === 'large' ? <span className="t-small opacity-80">{b.city}</span> : null}
            </button>
          );
        })}
      </div>
    </fieldset>
  );
}
