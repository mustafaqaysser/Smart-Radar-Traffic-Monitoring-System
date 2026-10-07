import type { ComponentProps, ReactNode } from 'react';
import { Link } from '@/i18n/navigation';
import { Icon, type IconName } from '@/components/brand/icon';
import { cn } from '@/lib/utils/cn';

export type ButtonVariant = 'primary' | 'secondary' | 'quiet' | 'inverse';
export type ButtonSize = 'sm' | 'md' | 'lg';

const base =
  'group/btn relative inline-flex items-center justify-center gap-3 font-label select-none whitespace-nowrap transition-[background-color,color,border-color,box-shadow] duration-[var(--dur-quick)] ease-[var(--ease-shade)] disabled:cursor-not-allowed disabled:opacity-55 aria-disabled:cursor-not-allowed aria-disabled:opacity-55';

const variants: Record<ButtonVariant, string> = {
  // Henna fill; on hover the button casts the sun's shade.
  primary: 'bg-accent text-on-accent hover-capable:hover:shadow-[calc(var(--shade-x)*5px)_calc(var(--shade-y)*5px)_0_0_var(--c-shade)] active:shadow-none',
  secondary: 'border border-ink text-ink hover-capable:hover:bg-ink hover-capable:hover:text-bg',
  quiet: 'text-ink underline decoration-line underline-offset-[0.35em] hover-capable:hover:decoration-ink px-0!',
  inverse: 'bg-ink text-bg hover-capable:hover:bg-accent hover-capable:hover:text-on-accent',
};

const sizes: Record<ButtonSize, string> = {
  sm: 'min-h-10 px-4 text-[0.6875rem] tracking-[0.14em] uppercase rtl:text-[0.9375rem] rtl:tracking-normal rtl:normal-case',
  md: 'min-h-12 px-6 text-[0.75rem] tracking-[0.14em] uppercase rtl:text-base rtl:tracking-normal rtl:normal-case',
  lg: 'min-h-14 px-8 text-[0.8125rem] tracking-[0.14em] uppercase rtl:text-[1.0625rem] rtl:tracking-normal rtl:normal-case',
};

export function buttonClasses(variant: ButtonVariant = 'primary', size: ButtonSize = 'md', className?: string): string {
  return cn(base, variants[variant], sizes[size], className);
}

function Arrow({ icon }: { icon: IconName }) {
  return (
    <span aria-hidden="true" className="inline-flex transition-transform duration-[var(--dur-base)] ease-[var(--ease-shade)] group-hover/btn:translate-x-1 rtl:group-hover/btn:-translate-x-1">
      <Icon name={icon} size={18} />
    </span>
  );
}

interface CommonProps {
  variant?: ButtonVariant;
  size?: ButtonSize;
  /** Trailing icon; `arrow` mirrors in RTL. */
  icon?: IconName | null;
  leadingIcon?: IconName;
  children: ReactNode;
}

export function Button({ variant = 'primary', size = 'md', icon = null, leadingIcon, className, children, type = 'button', ...rest }: CommonProps & ComponentProps<'button'>) {
  return (
    <button type={type} className={buttonClasses(variant, size, className)} {...rest}>
      {leadingIcon ? <Icon name={leadingIcon} size={18} /> : null}
      <span>{children}</span>
      {icon ? <Arrow icon={icon} /> : null}
    </button>
  );
}

export function ButtonLink({ variant = 'primary', size = 'md', icon = 'arrow', leadingIcon, className, children, ...rest }: CommonProps & ComponentProps<typeof Link>) {
  return (
    <Link className={buttonClasses(variant, size, className)} {...rest}>
      {leadingIcon ? <Icon name={leadingIcon} size={18} /> : null}
      <span>{children}</span>
      {icon ? <Arrow icon={icon} /> : null}
    </Link>
  );
}

/** External link styled as a button (opens in a new tab with an accessible hint). */
export function ButtonAnchor({ variant = 'secondary', size = 'md', icon = 'external', leadingIcon, className, children, newWindowLabel, ...rest }: CommonProps & ComponentProps<'a'> & { newWindowLabel?: string }) {
  const external = rest.target === '_blank';
  return (
    <a className={buttonClasses(variant, size, className)} rel={external ? 'noopener noreferrer' : undefined} {...rest}>
      {leadingIcon ? <Icon name={leadingIcon} size={18} /> : null}
      <span>
        {children}
        {external && newWindowLabel ? <span className="sr-only"> ({newWindowLabel})</span> : null}
      </span>
      {icon ? <Arrow icon={icon} /> : null}
    </a>
  );
}

/** A text link with a moving arrow — the default "more" affordance. */
export function ArrowLink({ className, children, ...rest }: ComponentProps<typeof Link>) {
  return (
    <Link className={cn('group/btn inline-flex items-center gap-2 font-label text-[0.75rem] tracking-[0.14em] uppercase rtl:text-base rtl:tracking-normal rtl:normal-case underline decoration-line underline-offset-[0.4em] hover-capable:hover:decoration-ink', className)} {...rest}>
      <span>{children}</span>
      <Arrow icon="arrow" />
    </Link>
  );
}

/** "Back to …" link: the arrow leads, pointing toward the start of the line (mirrors in RTL). */
export function BackLink({ className, children, ...rest }: ComponentProps<typeof Link>) {
  return (
    <Link className={cn('group/btn inline-flex items-center gap-2 font-label text-[0.75rem] tracking-[0.14em] uppercase rtl:text-base rtl:tracking-normal rtl:normal-case', className)} {...rest}>
      <span aria-hidden="true" className="inline-flex transition-transform duration-[var(--dur-base)] ease-[var(--ease-shade)] group-hover/btn:-translate-x-1 rtl:group-hover/btn:translate-x-1">
        <Icon name="arrowBack" size={18} />
      </span>
      <span>{children}</span>
    </Link>
  );
}
