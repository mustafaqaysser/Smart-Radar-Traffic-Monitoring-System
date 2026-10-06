import type { ComponentProps, ReactNode } from 'react';
import { cn } from '@/lib/utils/cn';

const control =
  'block w-full min-h-12 rounded-hair border border-field bg-raised px-4 py-3 text-ink placeholder:text-muted/80 transition-[border-color,box-shadow] duration-[var(--dur-quick)] focus:border-ink focus:outline-none focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-ink aria-[invalid=true]:border-danger disabled:opacity-60';

export function Label({ htmlFor, children, required, requiredLabel, className }: { htmlFor: string; children: ReactNode; required?: boolean; requiredLabel?: string; className?: string }) {
  return (
    <label htmlFor={htmlFor} className={cn('t-label mb-2 block text-muted', className)}>
      {children}
      {required ? (
        <span className="text-accent" aria-hidden="true">
          {' '}
          *
        </span>
      ) : null}
      {required && requiredLabel ? <span className="sr-only"> ({requiredLabel})</span> : null}
    </label>
  );
}

export function Input({ className, ...rest }: ComponentProps<'input'>) {
  const ltr = rest.type === 'email' || rest.type === 'tel' || rest.type === 'url';
  return <input dir={ltr ? 'ltr' : undefined} className={cn(control, ltr && 'text-start rtl:text-end', className)} {...rest} />;
}

export function Textarea({ className, ...rest }: ComponentProps<'textarea'>) {
  return <textarea className={cn(control, 'min-h-32 resize-y leading-relaxed', className)} {...rest} />;
}

export function Select({ className, children, ...rest }: ComponentProps<'select'>) {
  return (
    <div className="relative">
      <select className={cn(control, 'appearance-none pe-12', className)} {...rest}>
        {children}
      </select>
      <svg aria-hidden="true" viewBox="0 0 24 24" width="18" height="18" className="pointer-events-none absolute inset-y-0 end-4 my-auto" fill="none" stroke="currentColor" strokeWidth="1.5">
        <path d="M6 9.5l6 6 6-6" />
      </svg>
    </div>
  );
}

export function Hint({ id, children }: { id?: string; children: ReactNode }) {
  return (
    <p id={id} className="t-small mt-2 text-muted">
      {children}
    </p>
  );
}

export function FieldError({ id, children }: { id?: string; children?: ReactNode }) {
  if (!children) return null;
  return (
    <p id={id} role="alert" className="t-small mt-2 flex items-start gap-2 text-danger">
      <svg aria-hidden="true" viewBox="0 0 24 24" width="18" height="18" className="mt-[0.2em] shrink-0" fill="none" stroke="currentColor" strokeWidth="1.5">
        <path d="M12 4 21 19.5H3L12 4zM12 10v4.5M12 16.8v.4" />
      </svg>
      <span>{children}</span>
    </p>
  );
}

/** Label + control + hint + error, wired with ids for assistive technology. */
export function Field({
  id,
  label,
  hint,
  error,
  required,
  requiredLabel,
  className,
  children,
}: {
  id: string;
  label: ReactNode;
  hint?: ReactNode;
  error?: ReactNode;
  required?: boolean;
  requiredLabel?: string;
  className?: string;
  children: ReactNode;
}) {
  return (
    <div className={className}>
      <Label htmlFor={id} required={required} requiredLabel={requiredLabel}>
        {label}
      </Label>
      {children}
      {hint ? <Hint id={`${id}-hint`}>{hint}</Hint> : null}
      <FieldError id={`${id}-error`}>{error}</FieldError>
    </div>
  );
}

/** Props that connect a control to its Field's hint and error. */
export function describedBy(id: string, opts: { hint?: boolean; error?: boolean }): { 'aria-describedby'?: string; 'aria-invalid'?: boolean } {
  const ids = [opts.hint ? `${id}-hint` : null, opts.error ? `${id}-error` : null].filter(Boolean).join(' ');
  return { 'aria-describedby': ids || undefined, 'aria-invalid': opts.error || undefined };
}

/** A pill toggle used for filters, party size, time slots and tips. */
export function Chip({ pressed, className, children, ...rest }: ComponentProps<'button'> & { pressed?: boolean }) {
  return (
    <button
      type="button"
      aria-pressed={pressed}
      className={cn(
        'inline-flex min-h-11 items-center justify-center gap-2 rounded-pill border px-4 text-[0.9375rem] transition-[background-color,color,border-color] duration-[var(--dur-quick)] disabled:cursor-not-allowed disabled:opacity-45',
        pressed ? 'border-ink bg-ink text-bg' : 'border-line text-ink hover-capable:hover:border-ink',
        className,
      )}
      {...rest}
    >
      {children}
    </button>
  );
}

export function Checkbox({ id, label, className, ...rest }: Omit<ComponentProps<'input'>, 'type'> & { id: string; label: ReactNode }) {
  return (
    <label htmlFor={id} className={cn('flex cursor-pointer items-start gap-3', className)}>
      <input id={id} type="checkbox" className="mt-[0.3em] size-5 shrink-0 accent-[var(--c-accent)]" {...rest} />
      <span className="t-small">{label}</span>
    </label>
  );
}
