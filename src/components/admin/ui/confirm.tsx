'use client';

import { AlertDialog } from 'radix-ui';
import { useState, type ReactNode } from 'react';
import { useTranslations } from 'next-intl';
import { Button } from './button';

/**
 * Asks before something that cannot be undone. `onConfirm` may be async; the dialog stays open (and busy)
 * until it finishes, and closes only on success.
 */
export function Confirm({
  trigger,
  title,
  description,
  confirmLabel,
  tone = 'danger',
  onConfirm,
  children,
}: {
  trigger: ReactNode;
  title: ReactNode;
  description?: ReactNode;
  confirmLabel: ReactNode;
  tone?: 'danger' | 'primary';
  onConfirm: () => Promise<boolean | void> | boolean | void;
  children?: ReactNode;
}) {
  const t = useTranslations('admin.ui');
  const [open, setOpen] = useState(false);
  const [busy, setBusy] = useState(false);
  return (
    <AlertDialog.Root open={open} onOpenChange={(v) => !busy && setOpen(v)}>
      <AlertDialog.Trigger asChild>{trigger}</AlertDialog.Trigger>
      <AlertDialog.Portal>
        <AlertDialog.Overlay className="fixed inset-0 z-50 bg-[rgba(18,17,25,0.45)]" />
        <AlertDialog.Content className="cast fixed start-1/2 top-[18vh] z-50 w-[calc(100vw-2rem)] max-w-md -translate-x-1/2 rounded-soft border border-line bg-raised p-5 rtl:translate-x-1/2 focus:outline-none" {...(description ? {} : { 'aria-describedby': undefined })}>
          <AlertDialog.Title className="text-base font-semibold">{title}</AlertDialog.Title>
          {description ? <AlertDialog.Description className="mt-2 text-[0.875rem] text-muted">{description}</AlertDialog.Description> : null}
          {children ? <div className="mt-4">{children}</div> : null}
          <div className="mt-5 flex flex-wrap justify-end gap-2">
            <AlertDialog.Cancel asChild>
              <Button disabled={busy}>{t('cancel')}</Button>
            </AlertDialog.Cancel>
            <Button
              variant={tone === 'danger' ? 'danger' : 'primary'}
              disabled={busy}
              aria-busy={busy}
              onClick={async () => {
                setBusy(true);
                try {
                  const result = await onConfirm();
                  if (result !== false) setOpen(false);
                } finally {
                  setBusy(false);
                }
              }}
            >
              {confirmLabel}
            </Button>
          </div>
        </AlertDialog.Content>
      </AlertDialog.Portal>
    </AlertDialog.Root>
  );
}
