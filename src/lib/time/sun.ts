/**
 * Solar position and sun times (NOAA Solar Calculator algorithm, accurate to about a minute).
 * Drives Zill's live atmosphere: the phase palette, the direction of cast shade and the iftar hour in Ramadan.
 */

const RAD = Math.PI / 180;
const DEG = 180 / Math.PI;

function julianCentury(ms: number): number {
  const jd = ms / 86400000 + 2440587.5;
  return (jd - 2451545) / 36525;
}

interface SolarTerms {
  declination: number; // degrees
  equationOfTime: number; // minutes
}

function solarTerms(t: number): SolarTerms {
  const l0 = (280.46646 + t * (36000.76983 + t * 0.0003032)) % 360;
  const m = 357.52911 + t * (35999.05029 - 0.0001537 * t);
  const e = 0.016708634 - t * (0.000042037 + 0.0000001267 * t);
  const c =
    Math.sin(m * RAD) * (1.914602 - t * (0.004817 + 0.000014 * t)) +
    Math.sin(2 * m * RAD) * (0.019993 - 0.000101 * t) +
    Math.sin(3 * m * RAD) * 0.000289;
  const trueLong = l0 + c;
  const omega = 125.04 - 1934.136 * t;
  const appLong = trueLong - 0.00569 - 0.00478 * Math.sin(omega * RAD);
  const meanObliq = 23 + (26 + (21.448 - t * (46.815 + t * (0.00059 - t * 0.001813))) / 60) / 60;
  const obliq = meanObliq + 0.00256 * Math.cos(omega * RAD);
  const declination = Math.asin(Math.sin(obliq * RAD) * Math.sin(appLong * RAD)) * DEG;
  const y = Math.tan((obliq / 2) * RAD) ** 2;
  const eqTime =
    4 *
    DEG *
    (y * Math.sin(2 * l0 * RAD) -
      2 * e * Math.sin(m * RAD) +
      4 * e * y * Math.sin(m * RAD) * Math.cos(2 * l0 * RAD) -
      0.5 * y * y * Math.sin(4 * l0 * RAD) -
      1.25 * e * e * Math.sin(2 * m * RAD));
  return { declination, equationOfTime: eqTime };
}

export interface SunPosition {
  /** Degrees above the horizon (negative below), including atmospheric refraction. */
  altitude: number;
  /** Degrees clockwise from true north (90 = east, 180 = south, 270 = west). */
  azimuth: number;
}

export function sunPosition(date: Date, lat: number, lng: number): SunPosition {
  const ms = date.getTime();
  const { declination, equationOfTime } = solarTerms(julianCentury(ms));
  const d = new Date(ms);
  const minutesUtc = d.getUTCHours() * 60 + d.getUTCMinutes() + d.getUTCSeconds() / 60;
  const trueSolarTime = (((minutesUtc + equationOfTime + 4 * lng) % 1440) + 1440) % 1440;
  let hourAngle = trueSolarTime / 4 - 180;
  if (hourAngle < -180) hourAngle += 360;

  const cosZenith =
    Math.sin(lat * RAD) * Math.sin(declination * RAD) +
    Math.cos(lat * RAD) * Math.cos(declination * RAD) * Math.cos(hourAngle * RAD);
  const zenith = Math.acos(Math.min(1, Math.max(-1, cosZenith))) * DEG;
  const elevation = 90 - zenith;

  // Refraction (NOAA approximation).
  let refraction = 0;
  if (elevation <= 85) {
    const te = Math.tan(elevation * RAD);
    if (elevation > 5) refraction = 58.1 / te - 0.07 / te ** 3 + 0.000086 / te ** 5;
    else if (elevation > -0.575) refraction = 1735 + elevation * (-518.2 + elevation * (103.4 + elevation * (-12.79 + elevation * 0.711)));
    else refraction = -20.772 / te;
    refraction /= 3600;
  }

  const sinZenith = Math.sin(zenith * RAD);
  let azimuth: number;
  if (sinZenith < 1e-9) {
    azimuth = 180;
  } else {
    const cosAz = (Math.sin(lat * RAD) * Math.cos(zenith * RAD) - Math.sin(declination * RAD)) / (Math.cos(lat * RAD) * sinZenith);
    const az = Math.acos(Math.min(1, Math.max(-1, cosAz))) * DEG;
    azimuth = hourAngle > 0 ? (az + 180) % 360 : (540 - az) % 360;
  }
  return { altitude: elevation + refraction, azimuth };
}

export interface SunTimes {
  /** Civil dawn (sun 6° below the horizon). */
  dawn: Date | null;
  sunrise: Date | null;
  solarNoon: Date;
  sunset: Date | null;
  /** Civil dusk (sun 6° below the horizon). */
  dusk: Date | null;
  /** Maximum altitude of the day, degrees. */
  noonAltitude: number;
}

function eventMinutesUtc(dayStartMs: number, lat: number, lng: number, zenith: number, rising: boolean): number | null {
  // Two passes: estimate at solar noon, then refine at the event itself.
  let estimate = 720 - 4 * lng;
  for (let pass = 0; pass < 3; pass++) {
    const { declination, equationOfTime } = solarTerms(julianCentury(dayStartMs + estimate * 60000));
    const cosH =
      Math.cos(zenith * RAD) / (Math.cos(lat * RAD) * Math.cos(declination * RAD)) - Math.tan(lat * RAD) * Math.tan(declination * RAD);
    if (cosH > 1 || cosH < -1) return null; // polar day or night
    const hourAngle = Math.acos(cosH) * DEG;
    const noon = 720 - 4 * lng - equationOfTime;
    estimate = rising ? noon - hourAngle * 4 : noon + hourAngle * 4;
  }
  return estimate;
}

/**
 * Sun times for a local calendar date ('YYYY-MM-DD') at a location. The date is interpreted as the civil
 * date at the location; results are UTC instants.
 */
export function sunTimes(dateString: string, lat: number, lng: number): SunTimes {
  const [y, m, d] = dateString.split('-').map(Number) as [number, number, number];
  const dayStart = Date.UTC(y, m - 1, d);
  let noonMinutes = 720 - 4 * lng;
  for (let pass = 0; pass < 2; pass++) {
    noonMinutes = 720 - 4 * lng - solarTerms(julianCentury(dayStart + noonMinutes * 60000)).equationOfTime;
  }
  const at = (minutes: number | null) => (minutes === null ? null : new Date(dayStart + minutes * 60000));
  const solarNoon = new Date(dayStart + noonMinutes * 60000);
  return {
    dawn: at(eventMinutesUtc(dayStart, lat, lng, 96, true)),
    sunrise: at(eventMinutesUtc(dayStart, lat, lng, 90.833, true)),
    solarNoon,
    sunset: at(eventMinutesUtc(dayStart, lat, lng, 90.833, false)),
    dusk: at(eventMinutesUtc(dayStart, lat, lng, 96, false)),
    noonAltitude: sunPosition(solarNoon, lat, lng).altitude,
  };
}

/** Compass point (16-wind) for an azimuth, e.g. 247.5 → 'WSW'. */
export function compassPoint(azimuth: number): string {
  const points = ['N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 'SE', 'SSE', 'S', 'SSW', 'SW', 'WSW', 'W', 'WNW', 'NW', 'NNW'];
  return points[Math.round((((azimuth % 360) + 360) % 360) / 22.5) % 16] as string;
}
