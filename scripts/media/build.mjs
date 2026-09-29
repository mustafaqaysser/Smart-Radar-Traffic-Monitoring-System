/**
 * Zill media pipeline (sharp + ffmpeg-static; cross-platform Node).
 *
 *   media/originals/photos/<name>.jpg + <name>.json   sourced originals + provenance (git-ignored)
 *   media/originals/video/<name>.mp4                   sourced video (git-ignored)
 *   media/sources/*.json                               art-direction notes: focal point, bilingual alt text
 *
 * Produces (committed):
 *   public/media/<name>.webp            graded master (≤ 2400 px), optimised to AVIF/WebP by next/image
 *   public/media/<name>.mp4 / .webm     graded, seamless, muted loops (≤ 4 MB) + <name>-poster.webp
 *   src/content/media.json              manifest: size, blur placeholder, focal point, alt, credit
 *   docs/CREDITS.md                     photographer / licence credits
 *
 * One grade for everything, so photographs from many sources read as one photographer's work:
 * warm highlights, shadows lifted toward the brand's violet-brown shade, greens and blues muted, fine grain.
 *
 * Usage: npm run media:build [-- --only name1,name2] [-- --skip-video]
 */
import sharp from 'sharp';
import ffmpegPath from 'ffmpeg-static';
import { execFileSync } from 'node:child_process';
import { existsSync, mkdirSync, readFileSync, readdirSync, writeFileSync, statSync } from 'node:fs';
import { join, dirname, basename } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = join(dirname(fileURLToPath(import.meta.url)), '..', '..');
const ORIG_PHOTOS = join(root, 'media', 'originals', 'photos');
const ORIG_VIDEO = join(root, 'media', 'originals', 'video');
const SOURCES = join(root, 'media', 'sources');
const OUT = join(root, 'public', 'media');
const MANIFEST = join(root, 'src', 'content', 'media.json');
const CREDITS = join(root, 'docs', 'CREDITS.md');

const args = process.argv.slice(2);
const only = args.includes('--only') ? new Set(args[args.indexOf('--only') + 1].split(',')) : null;
const skipVideo = args.includes('--skip-video');

mkdirSync(OUT, { recursive: true });
mkdirSync(dirname(MANIFEST), { recursive: true });

/** Art-direction annotations from every media/sources/*.json, keyed by name. */
function loadAnnotations() {
  const map = new Map();
  for (const f of readdirSync(SOURCES).filter((f) => f.endsWith('.json'))) {
    const list = JSON.parse(readFileSync(join(SOURCES, f), 'utf8'));
    for (const entry of Array.isArray(list) ? list : []) if (entry?.name) map.set(entry.name, entry);
  }
  return map;
}

let grainCache = null;
/** A fine monochrome grain layer (deterministic PRNG so rebuilds are reproducible). */
async function grain(width, height) {
  if (!grainCache) {
    const size = 512;
    const buf = Buffer.alloc(size * size * 4);
    let seed = 1337;
    const rand = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
    for (let i = 0; i < size * size; i++) {
      const v = Math.round(128 + (rand() - 0.5) * 70);
      buf[i * 4] = v;
      buf[i * 4 + 1] = v;
      buf[i * 4 + 2] = v;
      buf[i * 4 + 3] = 34;
    }
    grainCache = await sharp(buf, { raw: { width: size, height: size, channels: 4 } }).png().toBuffer();
  }
  return { input: grainCache, tile: true, blend: 'overlay', width, height };
}

/** The unified Zill grade. */
function grade(pipeline) {
  return pipeline
    .recomb([
      [1.05, 0.02, -0.03],
      [0.0, 0.99, 0.0],
      [-0.03, 0.02, 0.93],
    ])
    .modulate({ saturation: 0.88, brightness: 1.0 })
    .linear([0.93, 0.925, 0.935], [11, 8, 14]);
}

function credit(prov) {
  return {
    author: prov.author ?? 'Unknown',
    authorUrl: prov.authorUrl ?? null,
    source: prov.source ?? 'Unknown',
    sourceUrl: prov.pageUrl ?? null,
    license: prov.license ?? 'Unknown',
    licenseUrl: prov.licenseUrl ?? null,
  };
}

async function buildPhoto(name, annotations, previous) {
  const input = join(ORIG_PHOTOS, `${name}.jpg`);
  const provPath = join(ORIG_PHOTOS, `${name}.json`);
  const note = annotations.get(name);
  const prov = existsSync(provPath) ? JSON.parse(readFileSync(provPath, 'utf8')) : null;
  if (!prov) throw new Error(`Missing provenance for ${name}`);
  if (!note) throw new Error(`Missing art-direction notes (alt text, focal point) for ${name}`);

  const outFile = join(OUT, `${name}.webp`);
  const resized = sharp(input).rotate().resize({ width: 2400, height: 2400, fit: 'inside', withoutEnlargement: true }).toColourspace('srgb');
  const graded = await grade(resized).toBuffer({ resolveWithObject: true });
  const { width, height } = graded.info;
  await sharp(graded.data)
    .composite([await grain(width, height)])
    .webp({ quality: 84, smartSubsample: true, effort: 5 })
    .toFile(outFile);
  const blurBuf = await sharp(graded.data).resize(16, 16, { fit: 'inside' }).webp({ quality: 40 }).toBuffer();
  return {
    kind: 'image',
    src: `/media/${name}.webp`,
    width,
    height,
    blur: `data:image/webp;base64,${blurBuf.toString('base64')}`,
    focal: note.focal ?? previous?.focal ?? [0.5, 0.5],
    alt: { ar: note.altAr, en: note.altEn },
    credit: credit(prov),
    forItem: note.forItem ?? null,
  };
}

function ff(argsList) {
  execFileSync(ffmpegPath, ['-hide_banner', '-loglevel', 'error', '-y', ...argsList], { stdio: 'inherit' });
}

async function buildVideo(entry) {
  const input = join(ORIG_VIDEO, `${entry.name}.mp4`);
  if (!existsSync(input)) throw new Error(`Missing video original: ${entry.name}`);
  const IN = entry.loop.inSec;
  const OUTS = entry.loop.outSec;
  const D = entry.name === 'video-night' ? 1.0 : 0.8;
  const total = OUTS - IN;
  const L = +(total - D).toFixed(3);
  const gradeFilter = 'eq=saturation=0.9:contrast=1.04:brightness=0.005,colorbalance=rs=0.03:gs=0.0:bs=-0.03:rm=0.02:bm=-0.02:rh=0.04:bh=-0.04';
  const graph =
    `[0:v]trim=${IN}:${OUTS},setpts=PTS-STARTPTS,scale=1280:-2:flags=lanczos,fps=25,${gradeFilter},format=yuv420p,split=3[a][b][c];` +
    `[a]trim=0:${D},setpts=PTS-STARTPTS,fps=25,settb=AVTB[head];` +
    `[b]trim=${L}:${total},setpts=PTS-STARTPTS,fps=25,settb=AVTB[tail];` +
    `[c]trim=${D}:${L},setpts=PTS-STARTPTS,fps=25,settb=AVTB[mid];` +
    `[tail][head]xfade=transition=fade:duration=${D}:offset=0[x];` +
    `[x][mid]concat=n=2:v=1:a=0[out]`;
  const mp4 = join(OUT, `${entry.name}.mp4`);
  const webm = join(OUT, `${entry.name}.webm`);
  ff(['-i', input, '-filter_complex', graph, '-map', '[out]', '-an', '-c:v', 'libx264', '-preset', 'slow', '-crf', '27', '-profile:v', 'high', '-pix_fmt', 'yuv420p', '-movflags', '+faststart', mp4]);
  ff(['-i', input, '-filter_complex', graph, '-map', '[out]', '-an', '-c:v', 'libvpx-vp9', '-b:v', '0', '-crf', '38', '-row-mt', '1', '-deadline', 'good', '-cpu-used', '2', webm]);
  for (const f of [mp4, webm]) {
    const mb = statSync(f).size / 1024 / 1024;
    if (mb > 4) throw new Error(`${basename(f)} is ${mb.toFixed(2)} MB (> 4 MB budget)`);
  }
  // Poster: a graded still from inside the loop.
  const rawPoster = join(OUT, `${entry.name}-poster.png`);
  ff(['-ss', String(Math.max(0, entry.posterAtSec - IN)), '-i', mp4, '-frames:v', '1', rawPoster]);
  const posterWebp = join(OUT, `${entry.name}-poster.webp`);
  const img = sharp(rawPoster);
  const meta = await img.metadata();
  await img.webp({ quality: 80 }).toFile(posterWebp);
  const blurBuf = await sharp(rawPoster).resize(16, 16, { fit: 'inside' }).webp({ quality: 40 }).toBuffer();
  execFileSync(process.execPath, ['-e', `require('fs').rmSync(${JSON.stringify(rawPoster)})`]);
  return {
    kind: 'video',
    src: `/media/${entry.name}.mp4`,
    sources: [
      { src: `/media/${entry.name}.webm`, type: 'video/webm' },
      { src: `/media/${entry.name}.mp4`, type: 'video/mp4' },
    ],
    poster: `/media/${entry.name}-poster.webp`,
    width: meta.width,
    height: meta.height,
    blur: `data:image/webp;base64,${blurBuf.toString('base64')}`,
    focal: entry.focal ?? [0.5, 0.5],
    alt: { ar: entry.altAr, en: entry.altEn },
    credit: { author: entry.author ?? `${entry.source} contributor`, authorUrl: null, source: entry.source, sourceUrl: entry.pageUrl, license: entry.license, licenseUrl: entry.licenseUrl },
    forItem: 'atmosphere',
  };
}

function writeCredits(manifest) {
  const rows = Object.entries(manifest)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([name, m]) => {
      const who = m.credit.authorUrl ? `[${m.credit.author}](${m.credit.authorUrl})` : m.credit.author;
      const where = m.credit.sourceUrl ? `[${m.credit.source}](${m.credit.sourceUrl})` : m.credit.source;
      const lic = m.credit.licenseUrl ? `[${m.credit.license}](${m.credit.licenseUrl})` : m.credit.license;
      return `| \`${name}\` | ${m.kind} | ${who} | ${where} | ${lic} |`;
    });
  const doc = `# Credits

All photography and video was sourced from libraries that allow free commercial use, downloaded, and self-hosted
(no hotlinking). Every file was inspected before use and graded with one shared look by \`scripts/media/build.mjs\`.
Unprocessed originals are kept out of git (\`media/originals/\`, git-ignored); provenance lives in
\`media/originals/**/<name>.json\` and \`media/sources/*.json\`, and the published manifest is \`src/content/media.json\`.

People appear only as hands, backs or silhouettes, and no stock model is presented as a named team member.
The team names on the site are fictional characters of this concept project.

## Typefaces

| Family | Use | Licence |
| --- | --- | --- |
| Reem Kufi | Arabic display | SIL Open Font License 1.1 |
| Imbue | Latin display | SIL Open Font License 1.1 |
| Markazi Text | Text, Arabic and Latin | SIL Open Font License 1.1 |
| IBM Plex Mono | Instrument layer | SIL Open Font License 1.1 |
| IBM Plex Sans Arabic, IBM Plex Sans | Admin | SIL Open Font License 1.1 |

## Media

| Name | Kind | Author | Source | Licence |
| --- | --- | --- | --- | --- |
${rows.join('\n')}

Logos, monogram, icons, patterns and illustrations are original work made for this project.
`;
  writeFileSync(CREDITS, doc);
}

async function main() {
  const annotations = loadAnnotations();
  const previous = existsSync(MANIFEST) ? JSON.parse(readFileSync(MANIFEST, 'utf8')) : {};
  const manifest = { ...previous };
  const photos = readdirSync(ORIG_PHOTOS)
    .filter((f) => f.endsWith('.jpg') && !f.endsWith('.preview.jpg'))
    .map((f) => f.replace(/\.jpg$/, ''))
    .filter((n) => !only || only.has(n));
  let done = 0;
  for (const name of photos) {
    manifest[name] = await buildPhoto(name, annotations, previous[name]);
    done++;
    if (done % 10 === 0) console.info(`  photos ${done}/${photos.length}`);
  }
  console.info(`✓ ${photos.length} photos graded`);
  if (!skipVideo && existsSync(join(SOURCES, 'video.json'))) {
    for (const entry of JSON.parse(readFileSync(join(SOURCES, 'video.json'), 'utf8'))) {
      if (only && !only.has(entry.name)) continue;
      manifest[entry.name] = await buildVideo(entry);
      console.info(`✓ ${entry.name}`);
    }
  }
  const sorted = Object.fromEntries(Object.entries(manifest).sort(([a], [b]) => a.localeCompare(b)));
  writeFileSync(MANIFEST, `${JSON.stringify(sorted, null, 2)}\n`);
  writeCredits(sorted);
  console.info(`✓ manifest: ${Object.keys(sorted).length} entries → src/content/media.json, docs/CREDITS.md`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
