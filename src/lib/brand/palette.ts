/**
 * Zill colour system — single source of truth.
 *
 * The site's palette follows the sun over the restaurant: six phases, each a complete, WCAG-AA-verified set of
 * semantic roles (see tests/unit/palette.test.ts). Warm light, cool shade: shadows on sunlit plaster are lit by
 * blue sky, so the brand's "shade" is a cool violet-brown, never grey.
 *
 * Re-skinning: change the raw swatches and phase roles here; everything else reads these tokens.
 */

export const swatches = {
  plaster50: '#FAF6EF',
  plaster100: '#F4EDE2',
  plaster200: '#EADFCD',
  plaster300: '#DCCBB1',
  clay400: '#C49A6C',
  clay500: '#A5784D',
  clay700: '#6B4A2F',
  shade300: '#B7ADBC',
  shade500: '#7A6E82',
  shade600: '#574D5F',
  shade800: '#342D3A',
  shade900: '#221D27',
  shade950: '#16131B',
  saffron300: '#F0C987',
  saffron500: '#D9982F',
  saffron700: '#7F5010',
  henna400: '#E57F5C',
  henna600: '#A3402A',
  henna700: '#8A3520',
  palm500: '#5F6E4A',
  palm700: '#3E5A33',
  night900: '#1A1824',
  night950: '#121119',
} as const;

export type Phase = 'dawn' | 'morning' | 'noon' | 'afternoon' | 'dusk' | 'night';
export const PHASES: readonly Phase[] = ['dawn', 'morning', 'noon', 'afternoon', 'dusk', 'night'];

export interface PhaseRoles {
  /** Page background. */
  bg: string;
  /** Raised panels (sheets, popovers). */
  raised: string;
  /** Quiet surfaces (cards, image placeholders). */
  surface: string;
  /** Primary text. */
  ink: string;
  /** Secondary text. */
  muted: string;
  /** Links and inline accents used as text. */
  link: string;
  /** Primary action background. */
  accent: string;
  /** Text on the primary action. */
  onAccent: string;
  /** Decorative hairlines (non-text, exempt from contrast). */
  line: string;
  /** Form-field borders — at least 3:1 against `bg` (WCAG 1.4.11). */
  field: string;
  /** The sun / lamp: decorative highlight. */
  sun: string;
  /** Cast-shadow colour (with alpha). */
  shade: string;
  success: string;
  warning: string;
  danger: string;
  /** Whether the phase is dark (used for `color-scheme`). */
  dark: boolean;
}

export const phases: Record<Phase, PhaseRoles> = {
  dawn: {
    bg: '#EDE7E4', raised: '#F7F3F0', surface: '#E1D8D3', ink: '#221D27', muted: '#574D5F', link: '#8A3520',
    accent: '#A3402A', onAccent: '#FBF8F4', line: 'rgba(34, 29, 39, 0.14)', field: '#7A6E82', sun: '#E0A15A',
    shade: 'rgba(64, 52, 88, 0.22)', success: '#3E5A33', warning: '#7F5010', danger: '#9E2F22', dark: false,
  },
  morning: {
    bg: '#F4EDE2', raised: '#FAF6EF', surface: '#EADFCD', ink: '#221D27', muted: '#574D5F', link: '#8A3520',
    accent: '#A3402A', onAccent: '#FBF8F4', line: 'rgba(34, 29, 39, 0.14)', field: '#7A6E82', sun: '#D9982F',
    shade: 'rgba(58, 45, 74, 0.26)', success: '#3E5A33', warning: '#7F5010', danger: '#9E2F22', dark: false,
  },
  noon: {
    bg: '#F8F4EC', raised: '#FDFBF7', surface: '#EEE6D8', ink: '#221D27', muted: '#574D5F', link: '#8A3520',
    accent: '#A3402A', onAccent: '#FBF8F4', line: 'rgba(34, 29, 39, 0.13)', field: '#7A6E82', sun: '#E3A442',
    shade: 'rgba(58, 45, 74, 0.2)', success: '#3E5A33', warning: '#7F5010', danger: '#9E2F22', dark: false,
  },
  afternoon: {
    bg: '#F0E2CC', raised: '#F8EFE1', surface: '#E5D2B4', ink: '#221D27', muted: '#54474C', link: '#82311E',
    accent: '#9C3B25', onAccent: '#FBF8F4', line: 'rgba(34, 29, 39, 0.15)', field: '#76675F', sun: '#CF7F24',
    shade: 'rgba(74, 48, 70, 0.3)', success: '#3A5430', warning: '#744A0E', danger: '#952B1F', dark: false,
  },
  dusk: {
    bg: '#231B23', raised: '#2C222C', surface: '#372A36', ink: '#F3E8DC', muted: '#CDBDB2', link: '#F2A07E',
    accent: '#EE8A62', onAccent: '#1B1419', line: 'rgba(243, 232, 220, 0.16)', field: '#9C8C94', sun: '#F0A55E',
    shade: 'rgba(8, 4, 12, 0.45)', success: '#A9C08C', warning: '#F0C987', danger: '#F59A86', dark: true,
  },
  night: {
    bg: '#121119', raised: '#1A1824', surface: '#242131', ink: '#EFE6D8', muted: '#B9AEA3', link: '#F09C7B',
    accent: '#E57F5C', onAccent: '#15121A', line: 'rgba(239, 230, 216, 0.15)', field: '#8E8699', sun: '#F0C987',
    shade: 'rgba(0, 0, 0, 0.5)', success: '#A3BC86', warning: '#F0C987', danger: '#F3927D', dark: true,
  },
};

/** CSS custom properties for one phase (consumed by Tailwind's `@theme inline` tokens). */
export function phaseCssVars(phase: Phase): Record<string, string> {
  const p = phases[phase];
  return {
    '--c-bg': p.bg,
    '--c-raised': p.raised,
    '--c-surface': p.surface,
    '--c-ink': p.ink,
    '--c-muted': p.muted,
    '--c-link': p.link,
    '--c-accent': p.accent,
    '--c-on-accent': p.onAccent,
    '--c-line': p.line,
    '--c-field': p.field,
    '--c-sun': p.sun,
    '--c-shade': p.shade,
    '--c-success': p.success,
    '--c-warning': p.warning,
    '--c-danger': p.danger,
  };
}

/** The stylesheet that defines every phase; rendered once in the root layout. */
export function phaseStylesheet(): string {
  return PHASES.map((phase) => {
    const vars = Object.entries(phaseCssVars(phase))
      .map(([k, v]) => `${k}:${v}`)
      .join(';');
    return `[data-phase="${phase}"]{${vars};color-scheme:${phases[phase].dark ? 'dark' : 'light'}}`;
  }).join('\n');
}

/**
 * The back-office palette: the morning roles by day and the night roles in dark mode (an explicit choice, or the
 * system setting when the staff member has not chosen). `.admin-dark` forces night for the kitchen display.
 */
export function adminThemeStylesheet(): string {
  const vars = (phase: Phase) =>
    Object.entries(phaseCssVars(phase))
      .map(([k, v]) => `${k}:${v}`)
      .join(';');
  const light = `${vars('morning')};color-scheme:light`;
  const dark = `${vars('night')};color-scheme:dark`;
  return [
    `:root{${light}}`,
    `:root[data-theme="dark"],.admin-dark{${dark}}`,
    `@media (prefers-color-scheme: dark){:root[data-theme="system"]{${dark}}}`,
  ].join('\n');
}

/** WCAG 2.x relative luminance of a #RRGGBB colour. */
export function luminance(hex: string): number {
  const channels = [1, 3, 5].map((i) => parseInt(hex.slice(i, i + 2), 16) / 255);
  const [r, g, b] = channels.map((c) => (c <= 0.04045 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4)) as [number, number, number];
  return 0.2126 * r + 0.7152 * g + 0.0722 * b;
}

/** WCAG contrast ratio between two #RRGGBB colours. */
export function contrastRatio(a: string, b: string): number {
  const [hi, lo] = [luminance(a), luminance(b)].sort((x, y) => y - x) as [number, number];
  return (hi + 0.05) / (lo + 0.05);
}
