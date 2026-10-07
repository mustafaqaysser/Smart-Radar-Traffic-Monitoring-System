import type { ComponentProps } from 'react';
import { cn } from '@/lib/utils/cn';

export function Kbd({ className, ...props }: ComponentProps<'kbd'>) {
  return <kbd dir="ltr" className={cn('inline-flex h-5 min-w-5 items-center justify-center rounded-hair border border-line bg-surface px-1 font-mono text-[0.6875rem] text-muted', className)} {...props} />;
}
