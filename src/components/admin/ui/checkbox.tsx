'use client';

import { Checkbox as CheckboxPrimitive } from 'radix-ui';
import type { ComponentProps } from 'react';
import { cn } from '@/lib/utils/cn';

export function Checkbox({ className, ...props }: ComponentProps<typeof CheckboxPrimitive.Root>) {
  return (
    <CheckboxPrimitive.Root
      className={cn(
        'peer inline-flex size-[1.125rem] shrink-0 items-center justify-center rounded-hair border border-field bg-raised transition-colors duration-[var(--dur-quick)] data-[state=checked]:border-ink data-[state=checked]:bg-ink data-[state=checked]:text-bg data-[state=indeterminate]:border-ink data-[state=indeterminate]:bg-ink data-[state=indeterminate]:text-bg disabled:opacity-50 aria-[invalid=true]:border-danger',
        className,
      )}
      {...props}
    >
      <CheckboxPrimitive.Indicator className="flex items-center justify-center">
        {props.checked === 'indeterminate' ? (
          <svg viewBox="0 0 16 16" width="12" height="12" aria-hidden="true" fill="none" stroke="currentColor" strokeWidth="2">
            <path d="M3.5 8h9" />
          </svg>
        ) : (
          <svg viewBox="0 0 16 16" width="12" height="12" aria-hidden="true" fill="none" stroke="currentColor" strokeWidth="2">
            <path d="M3 8.5 6.5 12 13 4.5" />
          </svg>
        )}
      </CheckboxPrimitive.Indicator>
    </CheckboxPrimitive.Root>
  );
}
