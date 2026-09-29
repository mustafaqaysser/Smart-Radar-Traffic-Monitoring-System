import localFont from 'next/font/local';

/**
 * Self-hosted brand fonts (all SIL Open Font License), loaded with next/font/local and split per script.
 * Each face is limited with `unicode-range`, so a page downloads only the scripts it actually renders.
 *
 *  Reem Kufi   — Arabic display: a geometric Kufi whose tall verticals stand like sundial gnomons.
 *  Imbue       — Latin display: condensed, high contrast — thick stroke as shade, hairline as light.
 *  Markazi Text — running text in both scripts, designed as one bilingual family.
 *  IBM Plex Mono — the "instrument" layer: time, coordinates, the sun's angle.
 */

// Unicode ranges must be literals: next/font analyses these calls at build time.
export const kufiArabic = localFont({
  src: '../fonts/reem-kufi-arabic.woff2',
  weight: '400 700',
  display: 'swap',
  preload: true,
  variable: '--font-kufi-ar',
  adjustFontFallback: false,
  fallback: ['Geeza Pro', 'Segoe UI', 'Tahoma', 'sans-serif'],
  declarations: [{ prop: 'unicode-range', value: 'U+0600-06FF, U+0750-077F, U+0870-088E, U+0890-0891, U+0897-08E1, U+08E3-08FF, U+200C-200E, U+2010-2011, U+204F, U+2E41, U+FB50-FDFF, U+FE70-FE74, U+FE76-FEFC' }],
});

export const kufiLatin = localFont({
  src: '../fonts/reem-kufi-latin.woff2',
  weight: '400 700',
  display: 'swap',
  preload: false,
  variable: '--font-kufi-lat',
  adjustFontFallback: 'Arial',
  declarations: [{ prop: 'unicode-range', value: 'U+0000-00FF, U+0131, U+0152-0153, U+02BB-02BC, U+02C6, U+02DA, U+02DC, U+0304, U+0308, U+0329, U+2000-206F, U+20AC, U+2122, U+2191, U+2193, U+2212, U+2215, U+FEFF, U+FFFD' }],
});

export const markaziArabic = localFont({
  src: '../fonts/markazi-arabic.woff2',
  weight: '400 700',
  display: 'swap',
  preload: true,
  variable: '--font-markazi-ar',
  adjustFontFallback: false,
  fallback: ['Geeza Pro', 'Segoe UI', 'Tahoma', 'serif'],
  declarations: [{ prop: 'unicode-range', value: 'U+0600-06FF, U+0750-077F, U+0870-088E, U+0890-0891, U+0897-08E1, U+08E3-08FF, U+200C-200E, U+2010-2011, U+204F, U+2E41, U+FB50-FDFF, U+FE70-FE74, U+FE76-FEFC' }],
});

export const markaziLatin = localFont({
  src: '../fonts/markazi-latin.woff2',
  weight: '400 700',
  display: 'swap',
  preload: false,
  variable: '--font-markazi-lat',
  adjustFontFallback: 'Times New Roman',
  declarations: [{ prop: 'unicode-range', value: 'U+0000-00FF, U+0131, U+0152-0153, U+02BB-02BC, U+02C6, U+02DA, U+02DC, U+0304, U+0308, U+0329, U+2000-206F, U+20AC, U+2122, U+2191, U+2193, U+2212, U+2215, U+FEFF, U+FFFD' }],
});

export const imbue = localFont({
  src: '../fonts/imbue-latin.woff2',
  weight: '100 900',
  display: 'swap',
  preload: true,
  variable: '--font-imbue',
  adjustFontFallback: 'Times New Roman',
  declarations: [{ prop: 'unicode-range', value: 'U+0000-00FF, U+0131, U+0152-0153, U+02BB-02BC, U+02C6, U+02DA, U+02DC, U+0304, U+0308, U+0329, U+2000-206F, U+20AC, U+2122, U+2191, U+2193, U+2212, U+2215, U+FEFF, U+FFFD' }],
});

export const plexMono = localFont({
  src: [
    { path: '../fonts/plex-mono-latin-400.woff2', weight: '400', style: 'normal' },
    { path: '../fonts/plex-mono-latin-500.woff2', weight: '500', style: 'normal' },
  ],
  display: 'swap',
  preload: false,
  variable: '--font-plex-mono',
  adjustFontFallback: false,
  fallback: ['ui-monospace', 'SFMono-Regular', 'Menlo', 'monospace'],
  declarations: [{ prop: 'unicode-range', value: 'U+0000-00FF, U+0131, U+0152-0153, U+02BB-02BC, U+02C6, U+02DA, U+02DC, U+0304, U+0308, U+0329, U+2000-206F, U+20AC, U+2122, U+2191, U+2193, U+2212, U+2215, U+FEFF, U+FFFD' }],
});

/** CSS variable classes for the public site. */
export const siteFontVariables = [kufiArabic, kufiLatin, markaziArabic, markaziLatin, imbue, plexMono]
  .map((font) => font.variable)
  .join(' ');
