'use client';

import Link from 'next/link';
import { usePathname, useSearchParams } from 'next/navigation';
import { cn } from '@/lib/utils/cn';

/**
 * Tabs that are links (the view lives in the URL, so it survives refresh and can be shared). The current tab is
 * matched on the query parameter `param`, or on the path when `param` is omitted.
 */
export function LinkTabs({ items, param, label, className }: { items: { href: string; label: string; value?: string; count?: string }[]; param?: string; label: string; className?: string }) {
  const pathname = usePathname();
  const search = useSearchParams();
  const current = param ? (search.get(param) ?? items[0]?.value) : pathname;
  return (
    <nav aria-label={label} className={cn('flex gap-1 overflow-x-auto border-b border-line scrollbar-thin', className)}>
      {items.map((item) => {
        const active = param ? item.value === current : item.href === current;
        return (
          <Link
            key={item.href}
            href={item.href}
            aria-current={active ? 'page' : undefined}
            className={cn(
              '-mb-px inline-flex h-9 shrink-0 items-center gap-2 border-b-2 border-transparent px-3 text-[0.875rem] text-muted no-underline transition-colors hover-capable:hover:text-ink pointer-coarse:h-11',
              active && 'border-accent font-medium text-ink',
            )}
          >
            {item.label}
            {item.count ? <span className="rounded-hair bg-surface px-1.5 text-xs text-muted tabular">{item.count}</span> : null}
          </Link>
        );
      })}
    </nav>
  );
}
