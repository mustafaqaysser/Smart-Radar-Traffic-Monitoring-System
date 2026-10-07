import { describe, expect, it } from 'vitest';
import { sunFacts } from '@/lib/site/sun-facts';

const JEDDAH = { lat: 21.4854, lng: 39.1869 };
const RIYADH = { lat: 24.7249, lng: 46.585 };

describe('sun facts', () => {
  it('has no noon shadow in Jeddah on its zero-shade day in late May', () => {
    // Declination ≈ +21.5° around 24–26 May.
    const days = ['2026-05-23', '2026-05-24', '2026-05-25', '2026-05-26', '2026-05-27'].map((d) => sunFacts(d, JEDDAH.lat, JEDDAH.lng));
    expect(days.some((f) => f.noonShadowRatio === null || f.noonShadowRatio < 0.01)).toBe(true);
  });

  it('casts noon shadows to the south in Jeddah in mid-June (sun north of the zenith)', () => {
    const f = sunFacts('2026-06-21', JEDDAH.lat, JEDDAH.lng);
    expect(f.shadowFallsSouth).toBe(true);
    expect(f.noonShadowRatio).toBeGreaterThan(0);
    expect(f.noonShadowRatio as number).toBeLessThan(0.1);
  });

  it('never loses the noon shadow in Riyadh (north of the Tropic of Cancer)', () => {
    const f = sunFacts('2026-06-21', RIYADH.lat, RIYADH.lng);
    expect(f.shadowFallsSouth).toBe(false);
    expect(f.noonShadowRatio as number).toBeGreaterThan(0.01);
  });

  it('casts long noon shadows in winter', () => {
    const f = sunFacts('2026-12-21', RIYADH.lat, RIYADH.lng);
    expect(f.noonShadowRatio as number).toBeGreaterThan(0.8);
    expect(f.sunrise && f.sunset && f.sunset > f.sunrise).toBe(true);
  });
});
