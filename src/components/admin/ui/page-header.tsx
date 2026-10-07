import type { ReactNode } from 'react';
import { cn } from '@/lib/utils/cn';

/** Title row of every admin screen: what it is, one line of context, and the screen's primary actions. */
export function PageHeader({ title, description, actions, eyebrow, className }: { title: ReactNode; description?: ReactNode; actions?: ReactNode; eyebrow?: ReactNode; className?: string }) {
  return (
    <header className={cn('flex flex-wrap items-end justify-between gap-x-6 gap-y-3', className)}>
      <div className="min-w-0">
        {eyebrow ? <div className="mb-1 text-xs text-muted">{eyebrow}</div> : null}
        <h1 className="text-xl font-semibold sm:text-2xl">{title}</h1>
        {description ? <p className="mt-1 max-w-3xl text-[0.875rem] text-muted">{description}</p> : null}
      </div>
      {actions ? <div className="flex flex-wrap items-center gap-2">{actions}</div> : null}
    </header>
  );
}

export function EmptyState({ icon, title, body, action, className }: { icon?: ReactNode; title: ReactNode; body?: ReactNode; action?: ReactNode; className?: string }) {
  return (
    <div className={cn('flex flex-col items-center justify-center gap-2 px-6 py-12 text-center', className)}>
      {icon ? <div className="mb-1 text-muted [&_svg]:size-7">{icon}</div> : null}
      <p className="font-medium">{title}</p>
      {body ? <p className="max-w-sm text-[0.8125rem] text-muted">{body}</p> : null}
      {action ? <div className="mt-3">{action}</div> : null}
    </div>
  );
}

/** A key figure: label, value, and an optional comparison line. */
export function Stat({ label, value, note, tone, className }: { label: ReactNode; value: ReactNode; note?: ReactNode; tone?: 'up' | 'down' | 'flat'; className?: string }) {
  return (
    <div className={cn('rounded-soft border border-line bg-raised px-4 py-3', className)}>
      <p className="text-xs text-muted">{label}</p>
      <p className="mt-1 text-2xl font-semibold tabular">{value}</p>
      {note ? <p className={cn('mt-0.5 text-xs', tone === 'up' ? 'text-success' : tone === 'down' ? 'text-danger' : 'text-muted')}>{note}</p> : null}
    </div>
  );
}

/** Definition list rows for record details. */
export function DetailList({ items, className }: { items: { label: ReactNode; value: ReactNode }[]; className?: string }) {
  return (
    <dl className={cn('grid grid-cols-[minmax(7rem,auto)_1fr] gap-x-4 gap-y-2 text-[0.875rem]', className)}>
      {items.map((item, i) => (
        <div key={i} className="contents">
          <dt className="text-muted">{item.label}</dt>
          <dd className="min-w-0 [overflow-wrap:anywhere]">{item.value}</dd>
        </div>
      ))}
    </dl>
  );
}
