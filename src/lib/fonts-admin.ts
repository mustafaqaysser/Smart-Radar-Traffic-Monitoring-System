import localFont from 'next/font/local';

/** Admin UI faces: IBM Plex Sans Arabic + IBM Plex Sans, tuned for dense, legible back-office screens. */

// Unicode ranges must be literals: next/font analyses these calls at build time.
export const plexSansArabic = localFont({
  src: [
    { path: '../fonts/plex-sans-arabic-400.woff2', weight: '400', style: 'normal' },
    { path: '../fonts/plex-sans-arabic-500.woff2', weight: '500', style: 'normal' },
    { path: '../fonts/plex-sans-arabic-600.woff2', weight: '600', style: 'normal' },
  ],
  display: 'swap',
  variable: '--font-plex-ar',
  adjustFontFallback: false,
  fallback: ['Segoe UI', 'Tahoma', 'sans-serif'],
  declarations: [{ prop: 'unicode-range', value: 'U+0600-06FF, U+0750-077F, U+0870-088E, U+0890-0891, U+0897-08E1, U+08E3-08FF, U+200C-200E, U+2010-2011, U+204F, U+2E41, U+FB50-FDFF, U+FE70-FE74, U+FE76-FEFC' }],
});

export const plexSansLatin = localFont({
  src: '../fonts/plex-sans-latin.woff2',
  weight: '100 700',
  display: 'swap',
  variable: '--font-plex-lat',
  adjustFontFallback: 'Arial',
  declarations: [{ prop: 'unicode-range', value: 'U+0000-00FF, U+0131, U+0152-0153, U+02BB-02BC, U+02C6, U+02DA, U+02DC, U+0304, U+0308, U+0329, U+2000-206F, U+20AC, U+2122, U+2191, U+2193, U+2212, U+2215, U+FEFF, U+FFFD' }],
});

/** Order numbers, codes and timers on tickets and the kitchen display. */
export const plexMonoAdmin = localFont({
  src: [
    { path: '../fonts/plex-mono-latin-400.woff2', weight: '400', style: 'normal' },
    { path: '../fonts/plex-mono-latin-500.woff2', weight: '500', style: 'normal' },
  ],
  display: 'swap',
  preload: false,
  variable: '--font-plex-mono',
  adjustFontFallback: false,
  fallback: ['ui-monospace', 'SFMono-Regular', 'Menlo', 'monospace'],
});

export const adminFontVariables = [plexSansArabic, plexSansLatin, plexMonoAdmin].map((font) => font.variable).join(' ');
