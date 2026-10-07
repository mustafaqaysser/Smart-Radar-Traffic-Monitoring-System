import type { ReactNode } from 'react';
import { SplitWords } from '@/components/motion/split-words';

/** The opening of every account section: eyebrow, a word-revealed title, a line of context. */
export function AccountHeader({ eyebrow, title, intro, children }: { eyebrow?: string; title: string; intro?: string; children?: ReactNode }) {
  return (
    <header className="flex flex-col gap-4">
      {eyebrow ? <p className="t-label text-muted">{eyebrow}</p> : null}
      <h1 className="t-display-md">
        <SplitWords text={title} />
      </h1>
      {intro ? <p className="t-body-lg measure text-muted">{intro}</p> : null}
      {children}
    </header>
  );
}

/** A titled block of the account ledger: a label rule, then its content. */
export function LedgerBlock({ title, action, children, className }: { title: string; action?: ReactNode; children: ReactNode; className?: string }) {
  return (
    <section className={className}>
      <div className="flex items-baseline justify-between gap-4 border-t border-ink pt-4">
        <h2 className="t-label text-muted">{title}</h2>
        {action}
      </div>
      <div className="pt-5">{children}</div>
    </section>
  );
}
