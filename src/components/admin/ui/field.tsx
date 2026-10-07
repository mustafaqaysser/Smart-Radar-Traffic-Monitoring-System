import type { ReactNode } from 'react';
import { cn } from '@/lib/utils/cn';

export function Label({ htmlFor, children, className, required }: { htmlFor?: string; children: ReactNode; className?: string; required?: boolean }) {
  return (
    <label htmlFor={htmlFor} className={cn('mb-1.5 block text-[0.8125rem] font-medium text-ink', className)}>
      {children}
      {required ? (
        <span aria-hidden="true" className="text-accent">
          {' '}
          *
        </span>
      ) : null}
    </label>
  );
}

export function Hint({ id, children, className }: { id?: string; children: ReactNode; className?: string }) {
  return (
    <p id={id} className={cn('mt-1.5 text-xs text-muted', className)}>
      {children}
    </p>
  );
}

export function FieldError({ id, children }: { id?: string; children?: ReactNode }) {
  if (!children) return null;
  return (
    <p id={id} role="alert" className="mt-1.5 text-xs font-medium text-danger">
      {children}
    </p>
  );
}

/**
 * Label, control, hint and error with matching ids. The control receives `aria-describedby` and `aria-invalid`
 * through the render prop so any input type can be used.
 */
export function Field({
  id,
  label,
  hint,
  error,
  required,
  className,
  children,
}: {
  id: string;
  label: ReactNode;
  hint?: ReactNode;
  error?: ReactNode;
  required?: boolean;
  className?: string;
  children: (a11y: { id: string; 'aria-describedby'?: string; 'aria-invalid'?: true; required?: boolean }) => ReactNode;
}) {
  const describedBy = [hint ? `${id}-hint` : null, error ? `${id}-error` : null].filter(Boolean).join(' ') || undefined;
  return (
    <div className={className}>
      <Label htmlFor={id} required={required}>
        {label}
      </Label>
      {children({ id, 'aria-describedby': describedBy, ...(error ? { 'aria-invalid': true as const } : {}), ...(required ? { required: true } : {}) })}
      {hint ? <Hint id={`${id}-hint`}>{hint}</Hint> : null}
      <FieldError id={`${id}-error`}>{error}</FieldError>
    </div>
  );
}

/** A labelled on/off row: the whole row is the label for its switch or checkbox. */
export function ToggleRow({ id, label, description, children, className }: { id: string; label: ReactNode; description?: ReactNode; children: ReactNode; className?: string }) {
  return (
    <div className={cn('flex items-start justify-between gap-4 py-3', className)}>
      <div className="min-w-0">
        <label htmlFor={id} className="block text-[0.875rem] font-medium">
          {label}
        </label>
        {description ? <p className="mt-0.5 text-xs text-muted">{description}</p> : null}
      </div>
      <div className="pt-0.5">{children}</div>
    </div>
  );
}
