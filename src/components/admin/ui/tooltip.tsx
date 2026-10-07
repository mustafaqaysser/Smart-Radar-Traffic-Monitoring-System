'use client';

import { Tooltip as TooltipPrimitive } from 'radix-ui';
import type { ReactNode } from 'react';

export const TooltipProvider = TooltipPrimitive.Provider;

/** A short label on hover/focus for icon-only controls (the control still carries its own aria-label). */
export function Tooltip({ label, children, side = 'top' }: { label: ReactNode; children: ReactNode; side?: 'top' | 'bottom' | 'left' | 'right' }) {
  return (
    <TooltipPrimitive.Root>
      <TooltipPrimitive.Trigger asChild>{children}</TooltipPrimitive.Trigger>
      <TooltipPrimitive.Portal>
        <TooltipPrimitive.Content side={side} sideOffset={6} className="z-50 max-w-64 rounded-hair bg-ink px-2 py-1 text-xs text-bg">
          {label}
        </TooltipPrimitive.Content>
      </TooltipPrimitive.Portal>
    </TooltipPrimitive.Root>
  );
}
