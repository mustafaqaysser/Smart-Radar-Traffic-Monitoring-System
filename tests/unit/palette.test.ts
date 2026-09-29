import { describe, expect, it } from 'vitest';
import { contrastRatio, PHASES, phases, phaseStylesheet } from '@/lib/brand/palette';

describe('palette', () => {
  for (const phase of PHASES) {
    const p = phases[phase];
    describe(phase, () => {
      for (const surface of ['bg', 'raised', 'surface'] as const) {
        for (const text of ['ink', 'muted', 'link', 'success', 'warning', 'danger'] as const) {
          it(`${text} on ${surface} passes AA (4.5:1)`, () => {
            expect(contrastRatio(p[text], p[surface])).toBeGreaterThanOrEqual(4.5);
          });
        }
      }
      it('on-accent text on accent passes AA', () => {
        expect(contrastRatio(p.onAccent, p.accent)).toBeGreaterThanOrEqual(4.5);
      });
      it('accent as a UI shape against bg passes 3:1', () => {
        expect(contrastRatio(p.accent, p.bg)).toBeGreaterThanOrEqual(3);
      });
      it('form field borders pass 3:1 against bg and raised', () => {
        expect(contrastRatio(p.field, p.bg)).toBeGreaterThanOrEqual(3);
        expect(contrastRatio(p.field, p.raised)).toBeGreaterThanOrEqual(3);
      });
      it('focus ring (ink) passes 3:1 against bg', () => {
        expect(contrastRatio(p.ink, p.bg)).toBeGreaterThanOrEqual(3);
      });
    });
  }

  it('emits a rule for every phase', () => {
    const css = phaseStylesheet();
    for (const phase of PHASES) expect(css).toContain(`[data-phase="${phase}"]`);
  });
});
