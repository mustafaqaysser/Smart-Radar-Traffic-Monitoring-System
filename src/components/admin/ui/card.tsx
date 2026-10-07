import type { ComponentProps } from 'react';
import { cn } from '@/lib/utils/cn';

export function Card({ className, ...props }: ComponentProps<'section'>) {
  return <section className={cn('rounded-soft border border-line bg-raised', className)} {...props} />;
}

export function CardHeader({ className, ...props }: ComponentProps<'div'>) {
  return <div className={cn('flex flex-wrap items-start justify-between gap-x-4 gap-y-2 border-b border-line px-5 py-4', className)} {...props} />;
}

export function CardTitle({ className, as: Tag = 'h2', ...props }: ComponentProps<'h2'> & { as?: 'h2' | 'h3' }) {
  return <Tag className={cn('text-[0.9375rem] font-semibold', className)} {...props} />;
}

export function CardDescription({ className, ...props }: ComponentProps<'p'>) {
  return <p className={cn('text-[0.8125rem] text-muted', className)} {...props} />;
}

export function CardContent({ className, ...props }: ComponentProps<'div'>) {
  return <div className={cn('px-5 py-4', className)} {...props} />;
}

export function CardFooter({ className, ...props }: ComponentProps<'div'>) {
  return <div className={cn('flex flex-wrap items-center justify-end gap-2 border-t border-line px-5 py-3', className)} {...props} />;
}
