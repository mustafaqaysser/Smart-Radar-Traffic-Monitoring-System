import { Fragment, type CSSProperties, type ElementType } from 'react';

interface SplitWordsProps {
  text: string;
  as?: ElementType;
  className?: string;
  /** Base delay in ms before the first word. */
  delay?: number;
  /** Per-word stagger in ms (45 by default; capped so the whole line stays under 700 ms). */
  stagger?: number;
  id?: string;
}

/**
 * Splits a heading into words — never characters — so Arabic letters stay joined. Each word rises from
 * behind its own line mask when the heading is revealed. Assistive technology reads the sentence once.
 */
export function SplitWords({ text, as: Tag = 'span', className, delay = 0, stagger = 45, id }: SplitWordsProps) {
  const words = text.split(/\s+/).filter(Boolean);
  const each = words.length > 1 ? Math.min(stagger, 700 / (words.length - 1)) : 0;
  return (
    <Tag id={id} className={className} data-reveal="words">
      <span className="sr-only">{text}</span>
      <span aria-hidden="true">
        {words.map((word, i) => (
          <Fragment key={`${word}-${i}`}>
            <span className="split-mask">
              <span className="split-word" style={{ '--word-delay': `${Math.round(delay + i * each)}ms` } as CSSProperties}>
                {word}
              </span>
            </span>
            {i < words.length - 1 ? ' ' : null}
          </Fragment>
        ))}
      </span>
    </Tag>
  );
}
