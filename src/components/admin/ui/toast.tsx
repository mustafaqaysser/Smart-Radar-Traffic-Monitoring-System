'use client';

import { CircleAlert, CircleCheck, X } from 'lucide-react';
import { useSyncExternalStore } from 'react';
import { cn } from '@/lib/utils/cn';

interface ToastItem {
  id: number;
  message: string;
  tone: 'success' | 'error' | 'info';
}

let items: ToastItem[] = [];
let nextId = 1;
const listeners = new Set<() => void>();
const emit = () => listeners.forEach((l) => l());

function push(message: string, tone: ToastItem['tone']) {
  const id = nextId++;
  items = [...items.slice(-3), { id, message, tone }];
  emit();
  window.setTimeout(() => dismiss(id), tone === 'error' ? 8000 : 4200);
}

function dismiss(id: number) {
  items = items.filter((t) => t.id !== id);
  emit();
}

/** Short confirmations and errors, announced politely (errors assertively). */
export const toast = {
  success: (message: string) => push(message, 'success'),
  error: (message: string) => push(message, 'error'),
  info: (message: string) => push(message, 'info'),
};

export function Toaster({ closeLabel }: { closeLabel: string }) {
  const list = useSyncExternalStore(
    (l) => {
      listeners.add(l);
      return () => listeners.delete(l);
    },
    () => items,
    () => items,
  );
  return (
    <>
      <div role="status" aria-live="polite" className="sr-only">
        {list.filter((t) => t.tone !== 'error').map((t) => t.message).join('. ')}
      </div>
      <div role="alert" aria-live="assertive" className="sr-only">
        {list.filter((t) => t.tone === 'error').map((t) => t.message).join('. ')}
      </div>
      <div aria-hidden="true" className="pointer-events-none fixed inset-x-0 bottom-4 z-[60] flex flex-col items-center gap-2 px-4 sm:items-end sm:px-6" data-admin-chrome>
        {list.map((t) => (
          <div key={t.id} className="cast pointer-events-auto flex w-full max-w-sm items-start gap-3 rounded-soft border border-line bg-raised px-4 py-3 animate-[rise-in_var(--dur-base)_var(--ease-shade)]">
            {t.tone === 'error' ? <CircleAlert className="mt-0.5 size-4 text-danger" /> : <CircleCheck className={cn('mt-0.5 size-4', t.tone === 'success' ? 'text-success' : 'text-muted')} />}
            <p className="min-w-0 flex-1 text-[0.875rem]">{t.message}</p>
            <button type="button" tabIndex={-1} className="-me-1 inline-flex size-6 items-center justify-center rounded-hair text-muted hover-capable:hover:text-ink" aria-label={closeLabel} onClick={() => dismiss(t.id)}>
              <X className="size-3.5" />
            </button>
          </div>
        ))}
      </div>
    </>
  );
}
