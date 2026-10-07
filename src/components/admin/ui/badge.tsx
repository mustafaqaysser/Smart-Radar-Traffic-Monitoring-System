import { cva, type VariantProps } from 'class-variance-authority';
import type { ComponentProps } from 'react';
import { cn } from '@/lib/utils/cn';

export const badgeVariants = cva('inline-flex items-center gap-1.5 whitespace-nowrap rounded-hair border px-2 py-0.5 text-xs font-medium leading-5 [&_svg]:size-3.5', {
  variants: {
    tone: {
      neutral: 'border-line bg-surface text-ink',
      accent: 'border-transparent bg-accent text-on-accent',
      success: 'border-success/40 bg-success/10 text-success',
      warning: 'border-warning/40 bg-warning/10 text-warning',
      danger: 'border-danger/40 bg-danger/10 text-danger',
      sun: 'border-sun/50 bg-sun/15 text-ink',
      outline: 'border-field text-muted',
    },
  },
  defaultVariants: { tone: 'neutral' },
});

export function Badge({ className, tone, ...props }: ComponentProps<'span'> & VariantProps<typeof badgeVariants>) {
  return <span className={cn(badgeVariants({ tone }), className)} {...props} />;
}

/** A small round status light (decorative; always paired with text). */
export function Dot({ tone = 'neutral', pulse = false, className }: { tone?: 'neutral' | 'accent' | 'success' | 'warning' | 'danger' | 'sun'; pulse?: boolean; className?: string }) {
  const color = { neutral: 'bg-muted', accent: 'bg-accent', success: 'bg-success', warning: 'bg-warning', danger: 'bg-danger', sun: 'bg-sun' }[tone];
  return <span aria-hidden="true" className={cn('inline-block size-2 shrink-0 rounded-full', color, pulse && 'animate-pulse reduced:animate-none', className)} />;
}
