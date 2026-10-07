'use client';

import { Dialog as DialogPrimitive } from 'radix-ui';
import { X } from 'lucide-react';
import type { ComponentProps, ReactNode } from 'react';
import { useTranslations } from 'next-intl';
import { cn } from '@/lib/utils/cn';

export const Dialog = DialogPrimitive.Root;
export const DialogTrigger = DialogPrimitive.Trigger;
export const DialogClose = DialogPrimitive.Close;

function Overlay({ className, ...props }: ComponentProps<typeof DialogPrimitive.Overlay>) {
  return <DialogPrimitive.Overlay className={cn('fixed inset-0 z-50 bg-[rgba(18,17,25,0.45)] data-[state=open]:animate-[fade-in_var(--dur-quick)_var(--ease-shade)]', className)} {...props} />;
}

/** Centred modal for short tasks. Focus is trapped and returned; Escape closes. */
export function DialogContent({ className, children, title, description, size = 'md', hideClose, ...props }: ComponentProps<typeof DialogPrimitive.Content> & { title: ReactNode; description?: ReactNode; size?: 'sm' | 'md' | 'lg'; hideClose?: boolean }) {
  const t = useTranslations('admin.ui');
  return (
    <DialogPrimitive.Portal>
      <Overlay />
      <DialogPrimitive.Content
        className={cn(
          'cast fixed start-1/2 top-[8vh] z-50 flex max-h-[84vh] w-[calc(100vw-2rem)] -translate-x-1/2 flex-col rounded-soft border border-line bg-raised rtl:translate-x-1/2 focus:outline-none data-[state=open]:animate-[rise-in_var(--dur-base)_var(--ease-shade)]',
          size === 'sm' ? 'max-w-sm' : size === 'lg' ? 'max-w-3xl' : 'max-w-lg',
          className,
        )}
        {...(description ? {} : { 'aria-describedby': undefined })}
        {...props}
      >
        <div className="flex items-start justify-between gap-4 border-b border-line px-5 py-4">
          <div className="min-w-0">
            <DialogPrimitive.Title className="text-base font-semibold">{title}</DialogPrimitive.Title>
            {description ? <DialogPrimitive.Description className="mt-1 text-[0.8125rem] text-muted">{description}</DialogPrimitive.Description> : null}
          </div>
          {hideClose ? null : (
            <DialogPrimitive.Close className="-me-2 -mt-1 inline-flex size-8 items-center justify-center rounded-hair text-muted hover-capable:hover:bg-surface hover-capable:hover:text-ink" aria-label={t('close')}>
              <X className="size-4" />
            </DialogPrimitive.Close>
          )}
        </div>
        <div className="min-h-0 flex-1 overflow-y-auto px-5 py-4 scrollbar-thin">{children}</div>
      </DialogPrimitive.Content>
    </DialogPrimitive.Portal>
  );
}

export function DialogFooter({ className, ...props }: ComponentProps<'div'>) {
  return <div className={cn('-mx-5 -mb-4 mt-4 flex flex-wrap items-center justify-end gap-2 border-t border-line px-5 py-3', className)} {...props} />;
}

/** A panel that slides in from the reading direction's end — for record details and longer forms. */
export function SheetContent({ className, children, title, description, width = 'md', ...props }: ComponentProps<typeof DialogPrimitive.Content> & { title: ReactNode; description?: ReactNode; width?: 'md' | 'lg' }) {
  const t = useTranslations('admin.ui');
  return (
    <DialogPrimitive.Portal>
      <Overlay />
      <DialogPrimitive.Content
        className={cn(
          'fixed inset-y-0 end-0 z-50 flex w-full flex-col border-s border-line bg-raised focus:outline-none data-[state=open]:animate-[from-right_var(--dur-base)_var(--ease-shade)] rtl:data-[state=open]:animate-[from-left_var(--dur-base)_var(--ease-shade)]',
          width === 'lg' ? 'max-w-2xl' : 'max-w-lg',
          className,
        )}
        {...(description ? {} : { 'aria-describedby': undefined })}
        {...props}
      >
        <div className="flex items-start justify-between gap-4 border-b border-line px-5 py-4">
          <div className="min-w-0">
            <DialogPrimitive.Title className="text-base font-semibold">{title}</DialogPrimitive.Title>
            {description ? <DialogPrimitive.Description className="mt-1 text-[0.8125rem] text-muted">{description}</DialogPrimitive.Description> : null}
          </div>
          <DialogPrimitive.Close className="-me-2 -mt-1 inline-flex size-8 items-center justify-center rounded-hair text-muted hover-capable:hover:bg-surface hover-capable:hover:text-ink" aria-label={t('close')}>
            <X className="size-4" />
          </DialogPrimitive.Close>
        </div>
        <div className="min-h-0 flex-1 overflow-y-auto px-5 py-4 scrollbar-thin">{children}</div>
      </DialogPrimitive.Content>
    </DialogPrimitive.Portal>
  );
}
