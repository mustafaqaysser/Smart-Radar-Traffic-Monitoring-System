'use client';

import { useSyncExternalStore } from 'react';
import { Icon } from '@/components/brand/icon';
import { Link } from '@/i18n/navigation';

interface Toast {
  id: number;
  message: string;
  action?: { label: string; href: string };
}

let toasts: Toast[] = [];
let nextId = 1;
const listeners = new Set<() => void>();
const emit = () => listeners.forEach((l) => l());

/** Shows a short, polite notice (announced to screen readers). */
export function toast(message: string, action?: { label: string; href: string }) {
  const id = nextId++;
  toasts = [...toasts.slice(-2), { id, message, action }];
  emit();
  window.setTimeout(() => dismiss(id), 5200);
}

function dismiss(id: number) {
  toasts = toasts.filter((t) => t.id !== id);
  emit();
}

export function Toaster({ closeLabel }: { closeLabel: string }) {
  const list = useSyncExternalStore(
    (l) => {
      listeners.add(l);
      return () => listeners.delete(l);
    },
    () => toasts,
    () => toasts,
  );
  return (
    <div aria-live="polite" role="status" className="pointer-events-none fixed inset-x-0 bottom-20 z-50 flex flex-col items-center gap-2 px-4 lg:bottom-8 lg:items-end lg:px-8">
      {list.map((t) => (
        <div key={t.id} className="toast pointer-events-auto flex w-full max-w-md items-center gap-4 bg-ink px-5 py-4 text-bg surface-inverse">
          <Icon name="check" size={20} />
          <p className="t-small flex-1">{t.message}</p>
          {t.action ? (
            <Link href={t.action.href} className="t-small shrink-0 underline underline-offset-4" onClick={() => dismiss(t.id)}>
              {t.action.label}
            </Link>
          ) : null}
          <button type="button" className="-me-2 inline-flex size-9 shrink-0 items-center justify-center" aria-label={closeLabel} onClick={() => dismiss(t.id)}>
            <Icon name="close" size={16} />
          </button>
        </div>
      ))}
    </div>
  );
}
