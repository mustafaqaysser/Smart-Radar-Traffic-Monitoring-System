/**
 * Generates favicon, Apple touch icon and PWA icons from the ظ monogram (sharp; cross-platform).
 * Usage: node scripts/brand/icons.mjs
 */
import sharp from 'sharp';
import { writeFileSync, mkdirSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = join(dirname(fileURLToPath(import.meta.url)), '..', '..');
const INK = '#221D27';
const PLASTER = '#F4EDE2';
const SUN = '#D9982F';
const SHADE = 'rgba(87,77,95,0.28)';
const MONO = 'M28 28.7 40 24v88h40a24 24 0 0 1 0 48H28zM40 122v28h40a14 14 0 0 0 0-28z';

/** Monogram centred in a square canvas. `scale` is the glyph height as a share of the canvas. */
function monogramSvg({ size, bg, fg, sun, shade = false, scale = 0.62, radius = 0 }) {
  const glyphH = 136; // path spans y 24..160
  const glyphW = 76; // path spans x 28..104
  const s = (size * scale) / glyphH;
  const tx = size / 2 - (28 + glyphW / 2) * s;
  const ty = size / 2 - (24 + glyphH / 2) * s;
  const shadeLayer = shade
    ? `<g transform="translate(${tx - 14 * s} ${ty + 9 * s}) scale(${s})"><path fill="${SHADE}" fill-rule="evenodd" d="${MONO}"/></g>`
    : '';
  return `<svg xmlns="http://www.w3.org/2000/svg" width="${size}" height="${size}" viewBox="0 0 ${size} ${size}"><rect width="${size}" height="${size}" rx="${radius}" fill="${bg}"/>${shadeLayer}<g transform="translate(${tx} ${ty}) scale(${s})"><path fill="${fg}" fill-rule="evenodd" d="${MONO}"/><circle cx="70" cy="82" r="10" fill="${sun}"/></g></svg>`;
}

async function png(svg, out) {
  await sharp(Buffer.from(svg)).png({ compressionLevel: 9 }).toFile(out);
  console.info('✓', out.replace(root, '.'));
}

function ico(pngs) {
  const header = Buffer.alloc(6);
  header.writeUInt16LE(0, 0);
  header.writeUInt16LE(1, 2);
  header.writeUInt16LE(pngs.length, 4);
  const entries = [];
  let offset = 6 + 16 * pngs.length;
  for (const { size, data } of pngs) {
    const e = Buffer.alloc(16);
    e.writeUInt8(size >= 256 ? 0 : size, 0);
    e.writeUInt8(size >= 256 ? 0 : size, 1);
    e.writeUInt8(0, 2);
    e.writeUInt8(0, 3);
    e.writeUInt16LE(1, 4);
    e.writeUInt16LE(32, 6);
    e.writeUInt32LE(data.length, 8);
    e.writeUInt32LE(offset, 12);
    offset += data.length;
    entries.push(e);
  }
  return Buffer.concat([header, ...entries, ...pngs.map((p) => p.data)]);
}

mkdirSync(join(root, 'public', 'icons'), { recursive: true });

// Favicon (SVG) — dark tile reads in both light and dark browser chrome.
writeFileSync(join(root, 'src', 'app', 'icon.svg'), monogramSvg({ size: 64, bg: INK, fg: PLASTER, sun: '#E3A442', scale: 0.66, radius: 12 }));
console.info('✓ ./src/app/icon.svg');

// favicon.ico (16 + 32 + 48)
const icoPngs = [];
for (const size of [16, 32, 48]) {
  const data = await sharp(Buffer.from(monogramSvg({ size, bg: INK, fg: PLASTER, sun: '#E3A442', scale: 0.7, radius: size / 6 }))).png().toBuffer();
  icoPngs.push({ size, data });
}
writeFileSync(join(root, 'src', 'app', 'favicon.ico'), ico(icoPngs));
console.info('✓ ./src/app/favicon.ico');

await png(monogramSvg({ size: 180, bg: PLASTER, fg: INK, sun: SUN, shade: true, scale: 0.6 }), join(root, 'src', 'app', 'apple-icon.png'));
await png(monogramSvg({ size: 192, bg: PLASTER, fg: INK, sun: SUN, shade: true, scale: 0.6 }), join(root, 'public', 'icons', 'icon-192.png'));
await png(monogramSvg({ size: 512, bg: PLASTER, fg: INK, sun: SUN, shade: true, scale: 0.6 }), join(root, 'public', 'icons', 'icon-512.png'));
// Maskable: glyph inside the 80% safe zone.
await png(monogramSvg({ size: 192, bg: PLASTER, fg: INK, sun: SUN, scale: 0.46 }), join(root, 'public', 'icons', 'maskable-192.png'));
await png(monogramSvg({ size: 512, bg: PLASTER, fg: INK, sun: SUN, scale: 0.46 }), join(root, 'public', 'icons', 'maskable-512.png'));
// Monochrome for Android themed icons.
await png(monogramSvg({ size: 512, bg: 'rgba(0,0,0,0)', fg: '#000000', sun: '#000000', scale: 0.46 }), join(root, 'public', 'icons', 'monochrome-512.png'));
