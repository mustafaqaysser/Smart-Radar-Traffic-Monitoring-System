/**
 * Gift card artwork: one sundial per design — the brand's upright, the shade it casts at that hour, the sun
 * (or the courtyard lamp at night) and the bilingual lockup. Pure SVG text, so the same art is shown on the
 * site (SVG) and in emails (rendered to PNG on the server). No text glyphs: every mark is a path.
 */
import { AR_SUN, AR_WORDMARK_PATH, EN_SUN, EN_WORDMARK_PATHS } from '@/components/brand/logo';
import { phases, type Phase } from './palette';

export const GIFT_CARD_SIZE = { width: 1000, height: 630 } as const;

interface DesignSpec {
  phase: Phase;
  /** Sun position, or null for the night lamp. */
  sun: { x: number; y: number } | null;
  /** Offset of the cast shade (the brand's shade is a hard-edged, translated copy of the upright). */
  shade: { x: number; y: number };
}

const DESIGNS: Record<string, DesignSpec> = {
  dawn: { phase: 'dawn', sun: { x: 250, y: 410 }, shade: { x: 150, y: 16 } },
  noon: { phase: 'noon', sun: { x: 640, y: 120 }, shade: { x: 6, y: 24 } },
  'long-shade': { phase: 'afternoon', sun: { x: 890, y: 380 }, shade: { x: -190, y: 18 } },
  night: { phase: 'night', sun: null, shade: { x: 10, y: 14 } },
};

export function isGiftCardDesign(design: string): boolean {
  return Object.hasOwn(DESIGNS, design);
}

const r = (n: number) => Math.round(n * 100) / 100;

export function giftCardSvg(design: string): string {
  const spec = DESIGNS[design] ?? (DESIGNS.noon as DesignSpec);
  const p = phases[spec.phase];
  const { width: W, height: H } = GIFT_CARD_SIZE;
  const groundY = 500;
  const baseX = 640;
  // The upright: the wordmark's gnomon (12 × 136 with a slanted cap), scaled up.
  const upright = `M${baseX} ${groundY}V${groundY - 300}l30 -11.7V${groundY}z`;
  const dial = Array.from({ length: 11 }, (_, i) => {
    const a = (Math.PI * (i + 1)) / 12;
    const x1 = baseX + 15 + Math.cos(a) * 340;
    const y1 = groundY - Math.sin(a) * 340;
    const x2 = baseX + 15 + Math.cos(a) * (i % 3 === 2 ? 392 : 372);
    const y2 = groundY - Math.sin(a) * (i % 3 === 2 ? 392 : 372);
    return `<line x1="${r(x1)}" y1="${r(y1)}" x2="${r(x2)}" y2="${r(y2)}" stroke="${p.muted}" stroke-width="2" stroke-opacity="0.55"/>`;
  }).join('');
  const stars = spec.sun
    ? ''
    : [
        [150, 140],
        [260, 90],
        [380, 170],
        [470, 80],
        [820, 120],
        [900, 220],
        [760, 60],
        [330, 260],
      ]
        .map(([x, y], i) => `<circle cx="${x}" cy="${y}" r="${i % 3 === 0 ? 3 : 2}" fill="${p.ink}" fill-opacity="0.7"/>`)
        .join('');
  const light = spec.sun
    ? `<circle cx="${spec.sun.x}" cy="${spec.sun.y}" r="38" fill="${p.sun}"/>`
    : `<g transform="translate(470 ${groundY})"><path d="M-26 0h52M-18 0v-60h36V0M-12 -60l12-26 12 26" fill="none" stroke="${p.ink}" stroke-width="5"/><circle cx="0" cy="-30" r="9" fill="${p.sun}"/><circle cx="0" cy="-30" r="40" fill="${p.sun}" fill-opacity="0.16"/></g>`;
  const en = EN_WORDMARK_PATHS.map((d) => `<path d="${d}"/>`).join('');
  return [
    `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${W} ${H}" width="${W}" height="${H}">`,
    `<rect width="${W}" height="${H}" fill="${p.bg}"/>`,
    `<path d="M0 ${groundY}H${W}V${H}H0z" fill="${p.surface}"/>`,
    dial,
    stars,
    light,
    `<path d="${upright}" fill="${p.shade}" transform="translate(${spec.shade.x} ${spec.shade.y})"/>`,
    `<path d="${upright}" fill="${p.ink}"/>`,
    `<line x1="0" y1="${groundY}" x2="${W}" y2="${groundY}" stroke="${p.ink}" stroke-width="2"/>`,
    // Bilingual lockup, bottom start: Zill | ظل
    `<g transform="translate(64 538) scale(0.42)" fill="${p.ink}">${en}<circle cx="${EN_SUN.cx}" cy="${EN_SUN.cy}" r="${EN_SUN.r}" fill="${p.sun}"/></g>`,
    `<line x1="132" y1="548" x2="132" y2="604" stroke="${p.ink}" stroke-width="1.5" stroke-opacity="0.5"/>`,
    `<g transform="translate(146 538) scale(0.42)" fill="${p.ink}"><path d="${AR_WORDMARK_PATH}" fill-rule="evenodd"/><circle cx="${AR_SUN.cx}" cy="${AR_SUN.cy}" r="${AR_SUN.r}" fill="${p.sun}"/></g>`,
    `<rect x="1" y="1" width="${W - 2}" height="${H - 2}" fill="none" stroke="${p.ink}" stroke-opacity="0.12" stroke-width="2"/>`,
    '</svg>',
  ].join('');
}
