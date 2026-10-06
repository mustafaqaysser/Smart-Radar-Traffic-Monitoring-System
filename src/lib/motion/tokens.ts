/** Motion tokens for GSAP and Motion — mirrors the CSS custom properties in src/styles/tokens.css. */
export const DUR = {
  instant: 0.09,
  quick: 0.18,
  base: 0.32,
  calm: 0.56,
  slow: 0.9,
  sun: 1.4,
  horizon: 2.2,
} as const;

/** Cubic-bézier control points. */
export const EASE = {
  shade: [0.16, 1, 0.3, 1],
  sun: [0.65, 0, 0.35, 1],
  dusk: [0.7, 0, 0.84, 0],
  breeze: [0.34, 1.2, 0.64, 1],
} as const;

/** GSAP CustomEase-free equivalents (expo/power curves that match the brand curves closely). */
export const GSAP_EASE = {
  shade: 'expo.out',
  sun: 'power2.inOut',
  dusk: 'power3.in',
  breeze: 'back.out(1.2)',
} as const;

export const STAGGER = { word: 0.045, line: 0.09, item: 0.07, maxTotal: 0.7 } as const;

/** Stagger per element so that a sequence never exceeds the maximum total. */
export function staggerFor(count: number, each: number = STAGGER.item): number {
  if (count <= 1) return 0;
  return Math.min(each, STAGGER.maxTotal / (count - 1));
}

export function prefersReducedMotion(): boolean {
  return typeof window !== 'undefined' && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
}

/** Heavy effects (pinned chapters, WebGL, pointer-following) are desktop-first. */
export function isFinePointerDesktop(): boolean {
  return typeof window !== 'undefined' && window.matchMedia('(hover: hover) and (pointer: fine) and (min-width: 64rem)').matches;
}
