'use client';

import { Popover as PopoverPrimitive } from 'radix-ui';
import type { ComponentProps } from 'react';
import { cn } from '@/lib/utils/cn';

export const Popover = PopoverPrimitive.Root;
export const PopoverTrigger = PopoverPrimitive.Trigger;
export const PopoverClose = PopoverPrimitive.Close;

export function PopoverContent({ className, sideOffset = 6, align = 'end', ...props }: ComponentProps<typeof PopoverPrimitive.Content>) {
  return (
    <PopoverPrimitive.Portal>
      <PopoverPrimitive.Content
        sideOffset={sideOffset}
        align={align}
        className={cn('cast z-50 w-80 rounded-soft border border-line bg-raised p-4 focus:outline-none data-[state=open]:animate-[fade-in_var(--dur-quick)_var(--ease-shade)]', className)}
        {...props}
      />
    </PopoverPrimitive.Portal>
  );
}
