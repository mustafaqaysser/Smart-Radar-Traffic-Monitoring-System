'use client';

import { useEffect, useId, useRef, type ReactNode } from 'react';
import { Icon } from '@/components/brand/icon';
import { useMotion } from '@/components/motion/motion-provider';
import { cn } from '@/lib/utils/cn';

interface DialogProps {
  open: boolean;
  onClose: () => void;
  title: ReactNode;
  /** Visually hide the title (it is still announced). */
  hideTitle?: boolean;
  closeLabel: string;
  /** 'center' for confirmations; 'sheet' slides from the inline end (bottom on small screens). */
  variant?: 'center' | 'sheet' | 'full';
  className?: string;
  children: ReactNode;
  footer?: ReactNode;
}

/**
 * Accessible modal built on the native <dialog>: focus is trapped and restored by the browser, Escape and the
 * backdrop close it, and page scroll is locked while it is open.
 */
export function Dialog({ open, onClose, title, hideTitle, closeLabel, variant = 'center', className, children, footer }: DialogProps) {
  const ref = useRef<HTMLDialogElement>(null);
  const titleId = useId();
  const { lockScroll } = useMotion();

  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    if (open && !el.open) {
      el.showModal();
      lockScroll(true);
      return () => lockScroll(false);
    }
    if (!open && el.open) el.close();
    return undefined;
  }, [open, lockScroll]);

  return (
    <dialog
      ref={ref}
      aria-labelledby={titleId}
      onCancel={(e) => {
        e.preventDefault();
        onClose();
      }}
      onClick={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
      data-variant={variant}
      className={cn('zill-dialog bg-raised text-ink', className)}
    >
      <div className="flex max-h-[inherit] flex-col">
        <header className="flex items-start justify-between gap-6 border-b border-line px-6 py-5">
          <h2 id={titleId} className={cn('t-heading-md', hideTitle && 'sr-only')}>
            {title}
          </h2>
          <button type="button" onClick={onClose} className="-m-2 inline-flex size-11 shrink-0 items-center justify-center" aria-label={closeLabel}>
            <Icon name="close" size={22} />
          </button>
        </header>
        <div className="min-h-0 flex-1 overflow-y-auto px-6 py-6" data-lenis-prevent>
          {children}
        </div>
        {footer ? <footer className="border-t border-line px-6 py-4">{footer}</footer> : null}
      </div>
    </dialog>
  );
}
