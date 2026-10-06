import type { ReactNode } from 'react';
import { SplitWords } from '@/components/motion/split-words';
import { Reveal } from '@/components/motion/reveal';
import { cn } from '@/lib/utils/cn';

interface SectionHeadingProps {
  id: string;
  eyebrow?: string;
  title: string;
  intro?: ReactNode;
  size?: 'lg' | 'md' | 'heading';
  as?: 'h1' | 'h2';
  className?: string;
  action?: ReactNode;
}

/** Eyebrow label, a word-revealed title and an optional intro — the standard opening of a section. */
export function SectionHeading({ id, eyebrow, title, intro, size = 'md', as = 'h2', className, action }: SectionHeadingProps) {
  const Title = as;
  const titleClass = size === 'lg' ? 't-display-lg' : size === 'md' ? 't-display-md' : 't-heading-lg';
  return (
    <div className={cn('flex flex-col gap-5', className)}>
      {eyebrow ? (
        <Reveal as="p" variant="fade" className="t-label text-muted">
          {eyebrow}
        </Reveal>
      ) : null}
      <Title id={id} className={cn(titleClass, 'max-w-[18ch]')}>
        <SplitWords text={title} />
      </Title>
      {intro ? (
        <Reveal as="div" delay={160} className="t-body-lg measure text-ink">
          {intro}
        </Reveal>
      ) : null}
      {action ? <Reveal delay={240}>{action}</Reveal> : null}
    </div>
  );
}
