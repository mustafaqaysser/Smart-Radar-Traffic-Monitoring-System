import type { ReactNode } from 'react';
import { SplitWords } from '@/components/motion/split-words';
import { Reveal } from '@/components/motion/reveal';
import { cn } from '@/lib/utils/cn';

/** The opening of an inner page: eyebrow, a word-revealed h1, an intro and an optional aside (photo, actions). */
export function PageHeader({ eyebrow, title, intro, aside, className, children }: { eyebrow?: string; title: string; intro?: ReactNode; aside?: ReactNode; className?: string; children?: ReactNode }) {
  return (
    <header className={cn('site-grid gap-y-8 pt-10 pb-[var(--spacing-stack)] lg:pt-16', className)}>
      <div className={cn('col-span-full flex flex-col gap-6', aside ? 'md:col-span-5 lg:col-span-7 lg:self-end' : 'lg:col-span-10')}>
        {eyebrow ? (
          <Reveal as="p" variant="fade" className="t-label text-muted">
            {eyebrow}
          </Reveal>
        ) : null}
        <h1 className="t-display-lg">
          <SplitWords text={title} />
        </h1>
        {intro ? (
          <Reveal as="div" delay={160} className="t-body-lg measure">
            {intro}
          </Reveal>
        ) : null}
        {children}
      </div>
      {aside ? <div className="col-span-full md:col-span-3 lg:col-span-4 lg:col-start-9">{aside}</div> : null}
    </header>
  );
}
