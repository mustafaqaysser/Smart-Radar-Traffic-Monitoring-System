import type { ComponentProps } from 'react';
import { cn } from '@/lib/utils/cn';

/** Plain data table styles; wrap wide tables so they scroll inside their card, never the page. */
export function TableWrap({ className, ...props }: ComponentProps<'div'>) {
  return <div className={cn('w-full overflow-x-auto scrollbar-thin', className)} {...props} />;
}

export function Table({ className, ...props }: ComponentProps<'table'>) {
  return <table className={cn('w-full border-collapse text-start text-[0.875rem]', className)} {...props} />;
}

export function THead({ className, ...props }: ComponentProps<'thead'>) {
  return <thead className={cn('border-b border-line', className)} {...props} />;
}

export function TBody({ className, ...props }: ComponentProps<'tbody'>) {
  return <tbody className={cn('[&>tr:last-child]:border-0', className)} {...props} />;
}

export function TR({ className, ...props }: ComponentProps<'tr'>) {
  return <tr className={cn('border-b border-line transition-colors data-[clickable=true]:cursor-pointer hover-capable:data-[clickable=true]:hover:bg-surface/60', className)} {...props} />;
}

export function TH({ className, ...props }: ComponentProps<'th'>) {
  return <th className={cn('h-9 whitespace-nowrap px-3 text-start align-middle text-xs font-medium text-muted first:ps-5 last:pe-5', className)} {...props} />;
}

export function TD({ className, ...props }: ComponentProps<'td'>) {
  return <td className={cn('px-3 py-2.5 align-middle first:ps-5 last:pe-5', className)} {...props} />;
}
