'use client';

import { useTranslations } from 'next-intl';
import { useCallback, useTransition } from 'react';
import type { ActionResult } from '@/lib/actions/result';
import { toast } from './ui/toast';

/** The message for an action error key: back-office wording first, then the shared form errors. */
export function useErrorMessage() {
  const ta = useTranslations('admin.errors');
  const tf = useTranslations('forms.errors');
  return useCallback((key: string) => (ta.has(key) ? ta(key) : tf.has(key) ? tf(key) : tf('unknown')), [ta, tf]);
}

/**
 * Runs a server action inside a transition and reports the outcome with a toast. Returns the pending flag and a
 * runner; the runner resolves to the result so callers can also use field errors or returned data.
 */
export function useAdminAction() {
  const [pending, startTransition] = useTransition();
  const message = useErrorMessage();
  const run = useCallback(
    <T,>(action: () => Promise<ActionResult<T>>, options: { success?: string; onSuccess?: (data: T) => void; onError?: (error: { error: string; fieldErrors?: Record<string, string> }) => void } = {}) =>
      new Promise<ActionResult<T>>((resolve) => {
        startTransition(async () => {
          try {
            const result = await action();
            if (result.ok) {
              if (options.success) toast.success(options.success);
              options.onSuccess?.(result.data);
            } else {
              toast.error(message(result.error));
              options.onError?.(result);
            }
            resolve(result);
          } catch {
            toast.error(message('unknown'));
            resolve({ ok: false, error: 'unknown' });
          }
        });
      }),
    [message],
  );
  return [pending, run] as const;
}
