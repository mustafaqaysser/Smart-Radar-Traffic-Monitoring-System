'use client';

import { Switch as SwitchPrimitive } from 'radix-ui';
import type { ComponentProps } from 'react';
import { cn } from '@/lib/utils/cn';

/** On/off toggle. The thumb travels toward the reading direction's end when on. */
export function Switch({ className, ...props }: ComponentProps<typeof SwitchPrimitive.Root>) {
  return (
    <SwitchPrimitive.Root
      className={cn(
        'peer inline-flex h-5 w-9 shrink-0 items-center rounded-pill border border-field bg-surface p-0.5 transition-colors duration-[var(--dur-quick)] data-[state=checked]:border-success data-[state=checked]:bg-success disabled:cursor-not-allowed disabled:opacity-50',
        className,
      )}
      {...props}
    >
      <SwitchPrimitive.Thumb className="block size-3.5 rounded-full bg-ink shadow-none transition-transform duration-[var(--dur-quick)] ease-[var(--ease-breeze)] data-[state=checked]:translate-x-4 data-[state=checked]:bg-raised rtl:data-[state=checked]:-translate-x-4" />
    </SwitchPrimitive.Root>
  );
}
