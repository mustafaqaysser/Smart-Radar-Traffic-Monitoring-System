import { describe, expect, it } from 'vitest';
import { compassPoint, sunPosition, sunTimes } from '@/lib/time/sun';
import { computeAtmosphere, phaseFor, shadeFor } from '@/lib/brand/atmosphere';
import { getZonedParts } from '@/lib/time/zoned';

const JEDDAH = { lat: 21.4858, lng: 39.1925, timeZone: 'Asia/Riyadh' };
const RIYADH = { lat: 24.7136, lng: 46.6753, timeZone: 'Asia/Riyadh' };

function localHm(date: Date | null, tz: string): number {
  if (!date) throw new Error('missing time');
  const p = getZonedParts(date, tz);
  return p.hour * 60 + p.minute;
}

describe('sun', () => {
  it('matches NOAA reference times for Boulder on the June solstice (±3 min)', () => {
    const t = sunTimes('2010-06-21', 40, -105);
    // NOAA: sunrise 05:32 MDT, sunset 20:32 MDT (UTC-6).
    expect(Math.abs(localHm(t.sunrise, 'America/Denver') - (5 * 60 + 32))).toBeLessThanOrEqual(3);
    expect(Math.abs(localHm(t.sunset, 'America/Denver') - (20 * 60 + 32))).toBeLessThanOrEqual(3);
  });

  it('puts Jeddah sunsets later than Riyadh on the same date (same zone, different sun)', () => {
    const j = sunTimes('2026-10-02', JEDDAH.lat, JEDDAH.lng);
    const r = sunTimes('2026-10-02', RIYADH.lat, RIYADH.lng);
    const diff = (localHm(j.sunset, 'Asia/Riyadh') - localHm(r.sunset, 'Asia/Riyadh'));
    expect(diff).toBeGreaterThan(15);
    expect(diff).toBeLessThan(35);
  });

  it('finds the zero-shade noon in Jeddah in late May (sun almost overhead)', () => {
    expect(sunTimes('2026-05-26', JEDDAH.lat, JEDDAH.lng).noonAltitude).toBeGreaterThan(89.3);
    expect(sunTimes('2026-12-21', JEDDAH.lat, JEDDAH.lng).noonAltitude).toBeLessThan(46);
  });

  it('places the morning sun in the east and the afternoon sun in the west', () => {
    const morning = sunPosition(new Date('2026-10-02T05:00:00Z'), JEDDAH.lat, JEDDAH.lng); // 08:00 local
    const afternoon = sunPosition(new Date('2026-10-02T13:00:00Z'), JEDDAH.lat, JEDDAH.lng); // 16:00 local
    expect(morning.azimuth).toBeGreaterThan(60);
    expect(morning.azimuth).toBeLessThan(130);
    expect(afternoon.azimuth).toBeGreaterThan(230);
    expect(afternoon.azimuth).toBeLessThan(300);
    expect(compassPoint(afternoon.azimuth)).toMatch(/W/);
  });
});

describe('atmosphere', () => {
  it('walks through the phases of a day in Jeddah', () => {
    const at = (iso: string) => computeAtmosphere(new Date(iso), JEDDAH).phase;
    expect(at('2026-10-02T00:30:00Z')).toBe('night'); // 03:30
    expect(at('2026-10-02T02:55:00Z')).toBe('dawn'); // 05:55
    expect(at('2026-10-02T05:30:00Z')).toBe('morning'); // 08:30
    expect(at('2026-10-02T09:00:00Z')).toBe('noon'); // 12:00
    expect(at('2026-10-02T12:30:00Z')).toBe('afternoon'); // 15:30
    expect(at('2026-10-02T15:15:00Z')).toBe('dusk'); // 18:15, just after sunset
    expect(at('2026-10-02T18:00:00Z')).toBe('night'); // 21:00
  });

  it('throws morning shade to the left and afternoon shade to the right', () => {
    expect(shadeFor({ altitude: 25, azimuth: 100 }).x).toBeLessThan(0);
    expect(shadeFor({ altitude: 25, azimuth: 260 }).x).toBeGreaterThan(0);
    expect(shadeFor({ altitude: 80, azimuth: 180 }).length).toBeLessThan(shadeFor({ altitude: 10, azimuth: 250 }).length);
    expect(shadeFor({ altitude: -20, azimuth: 0 }).y).toBeGreaterThan(0.9);
  });

  it('classifies the noon window symmetrically around solar noon', () => {
    const times = sunTimes('2026-10-02', JEDDAH.lat, JEDDAH.lng);
    const before = new Date(times.solarNoon.getTime() - 80 * 60000);
    const after = new Date(times.solarNoon.getTime() + 80 * 60000);
    expect(phaseFor(before, sunPosition(before, JEDDAH.lat, JEDDAH.lng), times)).toBe('noon');
    expect(phaseFor(after, sunPosition(after, JEDDAH.lat, JEDDAH.lng), times)).toBe('noon');
  });
});
