'use client';

import { useTranslations } from 'next-intl';
import { useEffect, useRef, type CSSProperties } from 'react';
import { MONOGRAM_PATH, MONOGRAM_SUN } from '@/components/brand/logo';

const SEEN_KEY = 'zill_seen';

function finish() {
  document.documentElement.removeAttribute('data-preload');
  try {
    window.localStorage.setItem(SEEN_KEY, '1');
  } catch {
    // Private mode: the preloader may show again next time, which is harmless.
  }
}

/**
 * The Gnomon — the first-visit preloader. The monogram ظ stands as a sundial; its shadow sweeps from dawn to
 * the sun's real angle over the house right now, then the shade slides off the page. Under two seconds,
 * shown once, and never under reduced motion (decided before first paint by the inline script in the layout).
 */
export function GnomonPreloader({ angle, length }: { angle: number; length: number }) {
  const t = useTranslations('common.actions');
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!document.documentElement.hasAttribute('data-preload')) return;
    const timer = window.setTimeout(finish, 2400);
    const onKey = (e: KeyboardEvent) => e.key === 'Escape' && finish();
    window.addEventListener('keydown', onKey);
    return () => {
      window.clearTimeout(timer);
      window.removeEventListener('keydown', onKey);
    };
  }, []);

  const style = { '--g-to': `${angle}deg`, '--g-len': length } as CSSProperties;
  return (
    <div ref={ref} className="gnomon" style={style} onAnimationEnd={(e) => e.animationName === 'gnomon-exit' && finish()}>
      <svg viewBox="0 0 400 400" className="gnomon-dial" aria-hidden="true" focusable="false">
        <g className="gnomon-marks" stroke="currentColor" strokeWidth="1.5">
          {Array.from({ length: 13 }, (_, i) => {
            const a = Math.PI + (Math.PI * i) / 12;
            const r1 = 168;
            const r2 = i % 3 === 0 ? 150 : 158;
            const r = (v: number) => Math.round(v * 100) / 100;
            return <line key={i} x1={r(200 + Math.cos(a) * r1)} y1={r(300 + Math.sin(a) * r1 * 0.42)} x2={r(200 + Math.cos(a) * r2)} y2={r(300 + Math.sin(a) * r2 * 0.42)} />;
          })}
        </g>
        <line x1="40" y1="300" x2="360" y2="300" stroke="currentColor" strokeWidth="1.5" opacity="0.4" />
        <g className="gnomon-shadow-wrap">
          <rect className="gnomon-shadow" x="200" y="296" width="150" height="8" fill="var(--c-shade)" />
        </g>
        <g transform="translate(166 140)">
          <path d={MONOGRAM_PATH} fill="currentColor" fillRule="evenodd" />
          <circle cx={MONOGRAM_SUN.cx} cy={MONOGRAM_SUN.cy} r={MONOGRAM_SUN.r} fill="var(--c-sun)" className="gnomon-sun" />
        </g>
      </svg>
      <button type="button" className="gnomon-skip t-label" onClick={finish}>
        {t('skip')}
      </button>
    </div>
  );
}
