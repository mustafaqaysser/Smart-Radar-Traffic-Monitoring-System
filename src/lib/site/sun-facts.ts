import { sunPosition, sunTimes } from '@/lib/time/sun';

export interface SunFacts {
  sunrise: Date | null;
  solarNoon: Date;
  sunset: Date | null;
  /** Noon shadow as a share of a standing person's height (null when the sun is overhead). */
  noonShadowRatio: number | null;
  /** True when the noon sun is north of the zenith (shadows fall south — summer in Jeddah). */
  shadowFallsSouth: boolean;
}

/** The day's sun at a house: sunrise, solar noon, sunset, and how long a person's noon shadow is. */
export function sunFacts(date: string, lat: number, lng: number): SunFacts {
  const times = sunTimes(date, lat, lng);
  const noon = sunPosition(times.solarNoon, lat, lng);
  const overhead = noon.altitude >= 89.5;
  const north = noon.azimuth < 90 || noon.azimuth > 270;
  return {
    sunrise: times.sunrise,
    solarNoon: times.solarNoon,
    sunset: times.sunset,
    noonShadowRatio: overhead ? null : 1 / Math.tan((noon.altitude * Math.PI) / 180),
    shadowFallsSouth: !overhead && north,
  };
}
