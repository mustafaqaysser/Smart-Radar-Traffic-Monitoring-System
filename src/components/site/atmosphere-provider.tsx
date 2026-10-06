'use client';

import { createContext, useContext, useEffect, useState, type ReactNode } from 'react';
import { atmosphereCssVars, computeAtmosphere } from '@/lib/brand/atmosphere';
import type { Phase } from '@/lib/brand/palette';

export interface AtmosphereBranch {
  slug: string;
  name: string;
  city: string;
  lat: number;
  lng: number;
  timeZone: string;
}

export interface AtmosphereState {
  phase: Phase;
  altitude: number;
  azimuth: number;
  /** ISO instant the state was computed for. */
  at: string;
  minutesToSunset: number | null;
}

interface AtmosphereContextValue {
  branch: AtmosphereBranch | null;
  state: AtmosphereState;
}

const AtmosphereContext = createContext<AtmosphereContextValue | null>(null);

export function useAtmosphere(): AtmosphereContextValue {
  const ctx = useContext(AtmosphereContext);
  if (!ctx) throw new Error('useAtmosphere must be used inside <AtmosphereProvider>');
  return ctx;
}

/** Recomputes the sun over the selected house every minute and updates the page's phase and shade. */
export function AtmosphereProvider({ branch, initial, children }: { branch: AtmosphereBranch | null; initial: AtmosphereState; children: ReactNode }) {
  const [state, setState] = useState(initial);

  useEffect(() => {
    if (!branch) return;
    const root = document.documentElement;
    const update = () => {
      const now = new Date();
      const a = computeAtmosphere(now, { lat: branch.lat, lng: branch.lng, timeZone: branch.timeZone });
      if (root.dataset.phaseLock !== 'true') root.dataset.phase = a.phase;
      for (const [key, value] of Object.entries(atmosphereCssVars(a))) root.style.setProperty(key, value);
      root.dataset.shadeDir = a.shade.x < 0 ? 'rev' : 'fwd';
      setState({ phase: a.phase, altitude: a.sun.altitude, azimuth: a.sun.azimuth, at: now.toISOString(), minutesToSunset: a.minutesToSunset });
    };
    update();
    // Align to the next minute boundary, then tick every minute.
    let interval: number | undefined;
    const timeout = window.setTimeout(() => {
      update();
      interval = window.setInterval(update, 60_000);
    }, 60_000 - (Date.now() % 60_000));
    const onVisible = () => document.visibilityState === 'visible' && update();
    document.addEventListener('visibilitychange', onVisible);
    return () => {
      window.clearTimeout(timeout);
      if (interval) window.clearInterval(interval);
      document.removeEventListener('visibilitychange', onVisible);
    };
  }, [branch]);

  return <AtmosphereContext value={{ branch, state }}>{children}</AtmosphereContext>;
}
