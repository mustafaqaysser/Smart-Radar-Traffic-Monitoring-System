import { cva, type VariantProps } from 'class-variance-authority';
import { Slot } from 'radix-ui';
import type { ComponentProps } from 'react';
import { cn } from '@/lib/utils/cn';

export const buttonVariants = cva(
  'inline-flex shrink-0 items-center justify-center gap-2 whitespace-nowrap rounded-hair font-medium transition-[background-color,border-color,color,opacity] duration-[var(--dur-quick)] disabled:pointer-events-none disabled:opacity-50 aria-disabled:pointer-events-none aria-disabled:opacity-50 [&_svg]:size-4',
  {
    variants: {
      variant: {
        primary: 'bg-accent text-on-accent hover-capable:hover:bg-[color-mix(in_oklab,var(--c-accent)_86%,var(--c-ink))]',
        secondary: 'bg-ink text-bg hover-capable:hover:bg-[color-mix(in_oklab,var(--c-ink)_86%,var(--c-bg))]',
        outline: 'border border-field bg-raised hover-capable:hover:border-ink hover-capable:hover:bg-surface',
        ghost: 'hover-capable:hover:bg-surface',
        danger: 'bg-danger text-raised hover-capable:hover:bg-[color-mix(in_oklab,var(--c-danger)_86%,var(--c-ink))]',
        'danger-outline': 'border border-danger/60 text-danger hover-capable:hover:bg-danger/10',
        link: 'h-auto px-0 text-link underline-offset-4 hover-capable:hover:underline',
      },
      size: {
        sm: 'h-8 px-3 text-[0.8125rem] pointer-coarse:h-10',
        md: 'h-9 px-4 pointer-coarse:h-11',
        lg: 'h-11 px-5 text-[0.9375rem]',
        icon: 'size-9 pointer-coarse:size-11',
        'icon-sm': 'size-8 pointer-coarse:size-10',
      },
    },
    defaultVariants: { variant: 'outline', size: 'md' },
  },
);

export type ButtonProps = ComponentProps<'button'> & VariantProps<typeof buttonVariants> & { asChild?: boolean };

export function Button({ className, variant, size, asChild = false, type, ...props }: ButtonProps) {
  const Comp = asChild ? Slot.Root : 'button';
  return <Comp className={cn(buttonVariants({ variant, size }), className)} {...(asChild ? {} : { type: type ?? 'button' })} {...props} />;
}
