import type { Phase } from './palette';
import { sunPosition, sunTimes, type SunPosition, type SunTimes } from '../time/sun';
import { toDateString } from '../time/zoned';

/**
 * The live atmosphere: which phase of the day it is at a branch, and where the shade falls.
 *
 * Phases follow the sun, not the clock:
 *   night      sun more than 6° below the horizon
 *   dawn       from civil dawn until the sun is 10° up
 *   morning    until 90 minutes before solar noon
 *   noon       solar noon ± 90 minutes — the short shade
 *   afternoon  from then until sunset — the long shade
 *   dusk       from sunset until civil dusk (maghrib)
 */

export interface AtmosphereLocation {
  lat: number;
  lng: number;
  timeZone: string;
}

export interface Shade {
  /** Unit direction of the cast shade on a wall facing the viewer (x → end of line in LTR, y → down). */
  x: number;
  y: number;
  /** Relative length of the shade, ~0.6 (high sun) to ~1.8 (low sun). */
  length: number;
}

export interface Atmosphere {
  phase: Phase;
  sun: SunPosition;
  shade: Shade;
  times: SunTimes;
  /** True when the sun is above the horizon. */
  daylight: boolean;
  /** Minutes until sunset (negative after sunset), or null if the sun does not set. */
  minutesToSunset: number | null;
}

const NOON_WINDOW_MIN = 90;

export function phaseFor(date: Date, sun: SunPosition, times: SunTimes): Phase {
  if (sun.altitude < -6) return 'night';
  const minutesFromNoon = (date.getTime() - times.solarNoon.getTime()) / 60000;
  if (minutesFromNoon < 0) {
    if (sun.altitude < 10) return 'dawn';
    return minutesFromNoon < -NOON_WINDOW_MIN ? 'morning' : 'noon';
  }
  if (minutesFromNoon <= NOON_WINDOW_MIN) return 'noon';
  if (sun.altitude >= 0) return 'afternoon';
  return 'dusk';
}

/**
 * Art-directed, physically inspired shade vector for type "mounted" on a south-facing wall:
 * an eastern sun throws shade to the west (screen left), a high sun drops it straight down, a low sun
 * stretches it sideways. At night the courtyard lamp takes over: short, soft, mostly downward.
 */
export function shadeFor(sun: SunPosition): Shade {
  if (sun.altitude <= 0) return { x: 0.28, y: 0.96, length: 0.55 };
  const alt = sun.altitude * (Math.PI / 180);
  const az = sun.azimuth * (Math.PI / 180);
  const rawX = -Math.sin(az) * Math.cos(alt);
  const rawY = Math.max(0.18, Math.sin(alt));
  const norm = Math.hypot(rawX, rawY) || 1;
  const length = 0.6 + 1.2 * (1 - Math.sin(alt));
  return { x: round(rawX / norm), y: round(rawY / norm), length: round(Math.min(1.8, length)) };
}

export function computeAtmosphere(date: Date, location: AtmosphereLocation): Atmosphere {
  const sun = sunPosition(date, location.lat, location.lng);
  const times = sunTimes(toDateString(date, location.timeZone), location.lat, location.lng);
  const phase = phaseFor(date, sun, times);
  return {
    phase,
    sun: { altitude: round(sun.altitude, 1), azimuth: round(sun.azimuth, 1) },
    shade: shadeFor(sun),
    times,
    daylight: sun.altitude > 0,
    minutesToSunset: times.sunset ? Math.round((times.sunset.getTime() - date.getTime()) / 60000) : null,
  };
}

/** CSS custom properties consumed by `.cast-shade` and the hero. */
export function atmosphereCssVars(a: Pick<Atmosphere, 'sun' | 'shade'>): Record<string, string> {
  return {
    '--sun-alt': String(a.sun.altitude),
    '--sun-az': String(a.sun.azimuth),
    '--shade-x': String(a.shade.x),
    '--shade-y': String(a.shade.y),
    '--shade-len': String(a.shade.length),
  };
}

function round(value: number, digits = 3): number {
  const f = 10 ** digits;
  return Math.round(value * f) / f;
}
