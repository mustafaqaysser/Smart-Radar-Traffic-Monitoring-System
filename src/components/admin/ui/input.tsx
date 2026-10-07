import type { ComponentProps } from 'react';
import { cn } from '@/lib/utils/cn';

export const controlClass =
  'block w-full rounded-hair border border-field bg-raised px-3 text-ink placeholder:text-muted/75 transition-[border-color] duration-[var(--dur-quick)] hover-capable:hover:border-ink/70 focus-visible:border-ink focus-visible:outline-2 focus-visible:outline-offset-1 focus-visible:outline-ink aria-[invalid=true]:border-danger disabled:cursor-not-allowed disabled:opacity-60 read-only:bg-surface';

const LTR_TYPES = new Set(['email', 'tel', 'url', 'password']);

export function Input({ className, type = 'text', dir, ...props }: ComponentProps<'input'>) {
  const ltr = LTR_TYPES.has(type);
  return <input type={type} dir={dir ?? (ltr ? 'ltr' : undefined)} className={cn(controlClass, 'h-9 py-1.5 pointer-coarse:h-11', ltr && 'text-start rtl:text-end', className)} {...props} />;
}

export function Textarea({ className, ...props }: ComponentProps<'textarea'>) {
  return <textarea className={cn(controlClass, 'min-h-20 resize-y py-2 leading-relaxed', className)} {...props} />;
}

/** Native select: fast, accessible and keyboard-perfect on every device. */
export function Select({ className, children, ...props }: ComponentProps<'select'>) {
  return (
    <div className="relative">
      <select className={cn(controlClass, 'h-9 appearance-none py-1.5 pe-9 pointer-coarse:h-11', className)} {...props}>
        {children}
      </select>
      <svg aria-hidden="true" viewBox="0 0 24 24" width="16" height="16" className="pointer-events-none absolute inset-y-0 end-3 my-auto text-muted" fill="none" stroke="currentColor" strokeWidth="1.5">
        <path d="M6 9.5l6 6 6-6" />
      </svg>
    </div>
  );
}
