import type { CSSProperties } from 'react';

/**
 * Zill marks as React components. Geometry is identical to public/brand/*.svg:
 * 12-unit uprights (gnomons) cut at 21.5° — the latitude of Jeddah — and one sun dot per script.
 * With `shade`, the uprights cast the live shadow of the moment (driven by --shade-x/-y/-len).
 */

export const AR_WORDMARK_PATH =
  'M16 112h12v38h44V28.7L84 24v126h34V28.7L130 24v88h40a24 24 0 0 1 0 48H16zM130 122v28h40a14 14 0 0 0 0-28z';
export const AR_SUN = { cx: 160, cy: 82, r: 10 };
export const EN_WORDMARK_PATHS = [
  'M8 112h38v10L20 150h26v10H8v-10l26-28H8z',
  'M58 112h12v48H58z',
  'M84 28.7 96 24v136H84zM110 28.7l12-4.7v136h-12z',
];
export const EN_SUN = { cx: 64, cy: 82, r: 10 };
export const MONOGRAM_PATH = 'M28 28.7 40 24v88h40a24 24 0 0 1 0 48H28zM40 122v28h40a14 14 0 0 0 0-28z';
export const MONOGRAM_SUN = { cx: 70, cy: 82, r: 10 };

const shadeStyle: CSSProperties = {
  transform: 'translate(calc(var(--shade-x) * var(--shade-len) * 9px), calc(var(--shade-y) * var(--shade-len) * 9px))',
  transition: 'transform var(--dur-horizon) var(--ease-sun)',
};

interface MarkProps {
  className?: string;
  /** Accessible name; decorative when omitted and `decorative` is true. */
  title?: string;
  decorative?: boolean;
  /** Cast the live shade of the moment behind the uprights. */
  shade?: boolean;
  /** Tint the sun with the phase's sun colour. */
  sunTint?: boolean;
}

function a11y(title: string | undefined, decorative: boolean | undefined) {
  return decorative ? { 'aria-hidden': true as const } : { role: 'img' as const, 'aria-label': title };
}

export function WordmarkAr({ className, title = 'ظل', decorative, shade, sunTint }: MarkProps) {
  return (
    <svg viewBox="8 16 194 152" className={className} {...a11y(title, decorative)} overflow="visible">
      {shade ? (
        <g style={shadeStyle} fill="var(--c-shade)">
          <path d={AR_WORDMARK_PATH} fillRule="evenodd" />
        </g>
      ) : null}
      <path d={AR_WORDMARK_PATH} fill="currentColor" fillRule="evenodd" />
      <circle {...AR_SUN} fill={sunTint ? 'var(--c-sun)' : 'currentColor'} />
    </svg>
  );
}

export function WordmarkEn({ className, title = 'Zill', decorative, shade, sunTint }: MarkProps) {
  return (
    <svg viewBox="0 16 130 152" className={className} {...a11y(title, decorative)} overflow="visible">
      {shade ? (
        <g style={shadeStyle} fill="var(--c-shade)">
          {EN_WORDMARK_PATHS.map((d) => (
            <path key={d} d={d} />
          ))}
        </g>
      ) : null}
      <g fill="currentColor">
        {EN_WORDMARK_PATHS.map((d) => (
          <path key={d} d={d} />
        ))}
      </g>
      <circle {...EN_SUN} fill={sunTint ? 'var(--c-sun)' : 'currentColor'} />
    </svg>
  );
}

/** Horizontal lockup; the script matching the page language leads. */
export function Lockup({ className, locale, title = 'Zill · ظل', decorative, sunTint }: MarkProps & { locale: 'ar' | 'en' }) {
  const ar = (
    <g transform={locale === 'ar' ? 'translate(0 0)' : 'translate(184 0)'}>
      <path d={AR_WORDMARK_PATH} fill="currentColor" fillRule="evenodd" />
      <circle {...AR_SUN} fill={sunTint ? 'var(--c-sun)' : 'currentColor'} />
    </g>
  );
  const en = (
    <g transform={locale === 'ar' ? 'translate(252 0)' : 'translate(0 0)'}>
      {EN_WORDMARK_PATHS.map((d) => (
        <path key={d} d={d} fill="currentColor" />
      ))}
      <circle {...EN_SUN} fill={sunTint ? 'var(--c-sun)' : 'currentColor'} />
    </g>
  );
  return (
    <svg viewBox={locale === 'ar' ? '8 16 382 152' : '0 16 382 152'} className={className} {...a11y(title, decorative)}>
      {locale === 'ar' ? (
        <>
          {ar}
          <rect x={222} y={64} width={1.5} height={96} fill="currentColor" opacity={0.4} />
          {en}
        </>
      ) : (
        <>
          {en}
          <rect x={152} y={64} width={1.5} height={96} fill="currentColor" opacity={0.4} />
          {ar}
        </>
      )}
    </svg>
  );
}

export function Monogram({ className, title = 'ظ', decorative, shade, sunTint = true }: MarkProps) {
  return (
    <svg viewBox="16 16 100 152" className={className} {...a11y(title, decorative)} overflow="visible">
      {shade ? (
        <g style={shadeStyle} fill="var(--c-shade)">
          <path d={MONOGRAM_PATH} fillRule="evenodd" />
        </g>
      ) : null}
      <path d={MONOGRAM_PATH} fill="currentColor" fillRule="evenodd" />
      <circle {...MONOGRAM_SUN} fill={sunTint ? 'var(--c-sun)' : 'currentColor'} />
    </svg>
  );
}

/** The wordmark for a page language. */
export function Wordmark({ locale, ...props }: MarkProps & { locale: string }) {
  return locale === 'ar' ? <WordmarkAr {...props} /> : <WordmarkEn {...props} />;
}
