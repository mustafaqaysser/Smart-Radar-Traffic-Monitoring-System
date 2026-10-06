import type { CSSProperties, ElementType, ReactNode } from 'react';

type RevealVariant = 'rise' | 'fade' | 'shade' | 'wipe';

interface RevealProps {
  as?: ElementType;
  variant?: RevealVariant;
  /** Delay in ms (keep sequences ≤ 700 ms in total). */
  delay?: number;
  className?: string;
  style?: CSSProperties;
  children: ReactNode;
  id?: string;
}

/**
 * Reveals its content when scrolled into view. Server-rendered: the animation is pure CSS keyed off
 * `data-revealed`, set by MotionProvider's observer. Without JavaScript or under reduced motion the content
 * is simply visible.
 *   rise  — lifts 24 px along the shade direction and fades in
 *   fade  — opacity only
 *   shade — a band of shade slides off it (clip-path) in the direction the shadow falls
 *   wipe  — clip-path from the inline start (mirrors in RTL)
 */
export function Reveal({ as: Tag = 'div', variant = 'rise', delay = 0, className, style, children, id }: RevealProps) {
  return (
    <Tag id={id} data-reveal={variant} className={className} style={{ ...style, '--reveal-delay': `${delay}ms` } as CSSProperties}>
      {children}
    </Tag>
  );
}
