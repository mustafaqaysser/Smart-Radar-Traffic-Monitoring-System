'use client';

import { useTranslations } from 'next-intl';
import { useEffect, useRef, useState } from 'react';
import type { Map as MapLibreMap } from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';
import { Icon } from '@/components/brand/icon';
import { MAP_STYLE_URL, MAP_STYLE_URL_DARK } from '@/lib/services/maps';

/**
 * An interactive map that loads only when asked (third-party tiles are not requested until then).
 * The address, phone and deep links on the page never depend on it.
 */
export function HouseMap({ lat, lng, label }: { lat: number; lng: number; label: string }) {
  const t = useTranslations('visit.locations');
  const box = useRef<HTMLDivElement>(null);
  const [state, setState] = useState<'idle' | 'loading' | 'ready' | 'error'>('idle');
  // The map is created once when requested; status changes must not tear it down.
  const [requested, setRequested] = useState(false);

  useEffect(() => {
    if (!requested || !box.current) return;
    let map: MapLibreMap | null = null;
    let cancelled = false;
    void (async () => {
      try {
        const maplibregl = await import('maplibre-gl');
        if (cancelled || !box.current) return;
        // Served from /public/vendor (copied on install): the bundled module cannot locate its own worker.
        maplibregl.setWorkerUrl('/vendor/maplibre/maplibre-gl-worker.mjs');
        const dark = ['dusk', 'night'].includes(document.documentElement.dataset.phase ?? '');
        const instance = new maplibregl.Map({
          container: box.current,
          style: dark ? MAP_STYLE_URL_DARK : MAP_STYLE_URL,
          center: [lng, lat],
          zoom: 15.5,
          attributionControl: { compact: true },
          cooperativeGestures: true,
        });
        map = instance;
        instance.addControl(new maplibregl.NavigationControl({ showCompass: false }), 'top-right');
        const pin = document.createElement('div');
        pin.className = 'house-pin';
        pin.setAttribute('aria-label', label);
        pin.setAttribute('role', 'img');
        new maplibregl.Marker({ element: pin, anchor: 'bottom' }).setLngLat([lng, lat]).addTo(instance);
        // Individual tiles or sprites may fail without breaking the map; only a map that never loads is an error.
        const timeout = window.setTimeout(() => !cancelled && setState((s) => (s === 'ready' ? s : 'error')), 15000);
        instance.once('load', () => {
          window.clearTimeout(timeout);
          if (!cancelled) setState('ready');
        });
      } catch {
        if (!cancelled) setState('error');
      }
    })();
    return () => {
      cancelled = true;
      map?.remove();
    };
  }, [requested, lat, lng, label]);

  return (
    <div className="relative aspect-[4/3] w-full overflow-hidden bg-surface md:aspect-[16/9]">
      {/* maplibre-gl.css makes the container position: relative (unlayered CSS wins), so size it explicitly. */}
      <div ref={box} className="absolute inset-0 h-full w-full" aria-label={label} role={state === 'ready' ? 'region' : undefined} />
      {state === 'idle' || state === 'error' ? (
        <div className="absolute inset-0 flex flex-col items-center justify-center gap-4 p-6 text-center">
          <Icon name="map" size={32} className="text-muted" />
          {state === 'error' ? <p className="t-small text-muted">{t('mapError')}</p> : null}
          {state === 'idle' ? (
            <>
              <button
                type="button"
                onClick={() => {
                  setState('loading');
                  setRequested(true);
                }} className="t-label min-h-11 border border-ink px-5">
                {t('mapShow')}
              </button>
              <p className="t-small text-muted">{t('mapNote')}</p>
            </>
          ) : null}
        </div>
      ) : null}
      {state === 'loading' ? (
        <p className="t-small absolute inset-x-0 bottom-4 text-center text-muted" role="status">
          {t('mapLoading')}
        </p>
      ) : null}
    </div>
  );
}
