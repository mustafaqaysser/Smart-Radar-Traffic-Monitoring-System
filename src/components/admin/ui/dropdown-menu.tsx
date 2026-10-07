'use client';

import { DropdownMenu as Menu } from 'radix-ui';
import type { ComponentProps } from 'react';
import { cn } from '@/lib/utils/cn';

export const DropdownMenu = Menu.Root;
export const DropdownMenuTrigger = Menu.Trigger;
export const DropdownMenuGroup = Menu.Group;
export const DropdownMenuRadioGroup = Menu.RadioGroup;

export function DropdownMenuContent({ className, sideOffset = 6, align = 'end', ...props }: ComponentProps<typeof Menu.Content>) {
  return (
    <Menu.Portal>
      <Menu.Content
        sideOffset={sideOffset}
        align={align}
        className={cn('cast z-50 min-w-48 overflow-hidden rounded-soft border border-line bg-raised p-1 text-[0.875rem] data-[state=open]:animate-[fade-in_var(--dur-quick)_var(--ease-shade)]', className)}
        {...props}
      />
    </Menu.Portal>
  );
}

const itemClass = 'relative flex min-h-8 cursor-default select-none items-center gap-2 rounded-hair px-2 py-1.5 outline-none data-[disabled]:opacity-50 data-[highlighted]:bg-surface [&_svg]:size-4 pointer-coarse:min-h-11';

export function DropdownMenuItem({ className, tone, ...props }: ComponentProps<typeof Menu.Item> & { tone?: 'danger' }) {
  return <Menu.Item className={cn(itemClass, tone === 'danger' && 'text-danger', className)} {...props} />;
}

export function DropdownMenuRadioItem({ className, children, ...props }: ComponentProps<typeof Menu.RadioItem>) {
  return (
    <Menu.RadioItem className={cn(itemClass, 'ps-7', className)} {...props}>
      <span className="absolute start-2 flex size-4 items-center justify-center">
        <Menu.ItemIndicator>
          <span className="block size-1.5 rounded-full bg-ink" />
        </Menu.ItemIndicator>
      </span>
      {children}
    </Menu.RadioItem>
  );
}

export function DropdownMenuLabel({ className, ...props }: ComponentProps<typeof Menu.Label>) {
  return <Menu.Label className={cn('px-2 py-1.5 text-xs font-medium text-muted', className)} {...props} />;
}

export function DropdownMenuSeparator({ className, ...props }: ComponentProps<typeof Menu.Separator>) {
  return <Menu.Separator className={cn('-mx-1 my-1 h-px bg-line', className)} {...props} />;
}
