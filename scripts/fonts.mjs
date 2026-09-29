/**
 * Copies the self-hosted brand fonts (OFL-licensed, per-script subsets) from Fontsource packages into
 * src/fonts, where next/font/local picks them up. Run with `npm run fonts:build` after changing fonts.
 * Cross-platform: plain Node, no shell utilities.
 */
import { copyFileSync, mkdirSync, existsSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = join(dirname(fileURLToPath(import.meta.url)), '..');
const out = join(root, 'src', 'fonts');
mkdirSync(out, { recursive: true });

const files = [
  // Arabic display — Reem Kufi (variable weight)
  ['@fontsource-variable/reem-kufi/files/reem-kufi-arabic-wght-normal.woff2', 'reem-kufi-arabic.woff2'],
  ['@fontsource-variable/reem-kufi/files/reem-kufi-latin-wght-normal.woff2', 'reem-kufi-latin.woff2'],
  // Text, both scripts — Markazi Text (variable weight)
  ['@fontsource-variable/markazi-text/files/markazi-text-arabic-wght-normal.woff2', 'markazi-arabic.woff2'],
  ['@fontsource-variable/markazi-text/files/markazi-text-latin-wght-normal.woff2', 'markazi-latin.woff2'],
  // Latin display — Imbue (variable weight + optical size)
  ['@fontsource-variable/imbue/files/imbue-latin-opsz-normal.woff2', 'imbue-latin.woff2'],
  // Instrument layer — IBM Plex Mono
  ['@fontsource/ibm-plex-mono/files/ibm-plex-mono-latin-400-normal.woff2', 'plex-mono-latin-400.woff2'],
  ['@fontsource/ibm-plex-mono/files/ibm-plex-mono-latin-500-normal.woff2', 'plex-mono-latin-500.woff2'],
  // Admin UI — IBM Plex Sans Arabic + IBM Plex Sans
  ['@fontsource/ibm-plex-sans-arabic/files/ibm-plex-sans-arabic-arabic-400-normal.woff2', 'plex-sans-arabic-400.woff2'],
  ['@fontsource/ibm-plex-sans-arabic/files/ibm-plex-sans-arabic-arabic-500-normal.woff2', 'plex-sans-arabic-500.woff2'],
  ['@fontsource/ibm-plex-sans-arabic/files/ibm-plex-sans-arabic-arabic-600-normal.woff2', 'plex-sans-arabic-600.woff2'],
  ['@fontsource-variable/ibm-plex-sans/files/ibm-plex-sans-latin-wght-normal.woff2', 'plex-sans-latin.woff2'],
];

for (const [from, to] of files) {
  const src = join(root, 'node_modules', from);
  if (!existsSync(src)) throw new Error(`Missing font file: ${from} — run npm install first.`);
  copyFileSync(src, join(out, to));
  console.info(`✓ ${to}`);
}
