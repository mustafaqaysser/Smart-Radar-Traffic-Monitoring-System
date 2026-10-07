'use client';

import { Tabs as TabsPrimitive } from 'radix-ui';
import type { ComponentProps } from 'react';
import { cn } from '@/lib/utils/cn';

export const Tabs = TabsPrimitive.Root;

export function TabsList({ className, ...props }: ComponentProps<typeof TabsPrimitive.List>) {
  return <TabsPrimitive.List className={cn('flex gap-1 overflow-x-auto border-b border-line scrollbar-thin', className)} {...props} />;
}

export function TabsTrigger({ className, ...props }: ComponentProps<typeof TabsPrimitive.Trigger>) {
  return (
    <TabsPrimitive.Trigger
      className={cn(
        '-mb-px inline-flex h-9 shrink-0 items-center gap-2 border-b-2 border-transparent px-3 text-[0.875rem] text-muted transition-colors hover-capable:hover:text-ink data-[state=active]:border-accent data-[state=active]:font-medium data-[state=active]:text-ink pointer-coarse:h-11',
        className,
      )}
      {...props}
    />
  );
}

export function TabsContent({ className, ...props }: ComponentProps<typeof TabsPrimitive.Content>) {
  return <TabsPrimitive.Content className={cn('pt-5 focus-visible:outline-none', className)} {...props} />;
}
