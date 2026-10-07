'use client';

import { useLocale, useTranslations } from 'next-intl';
import { useEffect, useRef, useState } from 'react';
import type { Map as MapLibreMap, Marker } from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';
import { Icon } from '@/components/brand/icon';
import { Button } from '@/components/site/ui/button';
import { formatNumber } from '@/lib/i18n/format';
import { distanceKm, MAP_STYLE_URL, MAP_STYLE_URL_DARK } from '@/lib/services/maps';

interface PinPickerProps {
  house: { lat: number; lng: number };
  radiusKm: number;
  value: { lat: number; lng: number } | null;
  onChange: (point: { lat: number; lng: number }) => void;
}

/** A GeoJSON ring approximating a circle of `km` around a point. */
function circle(center: { lat: number; lng: number }, km: number, steps = 72): [number, number][] {
  const ring: [number, number][] = [];
  const dLat = km / 110.574;
  const dLng = km / (111.32 * Math.cos((center.lat * Math.PI) / 180));
  for (let i = 0; i <= steps; i++) {
    const a = (i / steps) * Math.PI * 2;
    ring.push([center.lng + dLng * Math.cos(a), center.lat + dLat * Math.sin(a)]);
  }
  return ring;
}

/**
 * Delivery by distance: the guest drags a pin to their door (or uses their location) and sees the house's
 * delivery circle. The map loads only here, when a radius zone is chosen; the location button works without it.
 */
export function PinPicker({ house, radiusKm, value, onChange }: PinPickerProps) {
  const t = useTranslations('order.checkout.pin');
  const { lat, lng } = house;
  const locale = useLocale();
  const box = useRef<HTMLDivElement>(null);
  const map = useRef<MapLibreMap | null>(null);
  const marker = useRef<Marker | null>(null);
  const changeRef = useRef(onChange);
  const [mapState, setMapState] = useState<'loading' | 'ready' | 'error'>('loading');
  const [geo, setGeo] = useState<'idle' | 'locating' | 'denied'>('idle');

  useEffect(() => {
    changeRef.current = onChange;
  });

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      try {
        const maplibregl = await import('maplibre-gl');
        if (cancelled || !box.current) return;
        maplibregl.setWorkerUrl('/vendor/maplibre/maplibre-gl-worker.mjs');
        const root = document.documentElement;
        const dark = ['dusk', 'night'].includes(root.dataset.phase ?? '');
        // The pin and the delivery circle take the brand's colours for the current hour.
        const token = (name: string, fallback: string) => getComputedStyle(root).getPropertyValue(name).trim() || fallback;
        const accent = token('--c-accent', '#a2402a');
        const sun = token('--c-sun', '#e2a33c');
        const instance = new maplibregl.Map({ container: box.current, style: dark ? MAP_STYLE_URL_DARK : MAP_STYLE_URL, center: [lng, lat], zoom: 11.5, attributionControl: { compact: true }, cooperativeGestures: true });
        map.current = instance;
        instance.addControl(new maplibregl.NavigationControl({ showCompass: false }), 'top-right');
        const home = document.createElement('div');
        home.className = 'house-pin';
        new maplibregl.Marker({ element: home, anchor: 'bottom' }).setLngLat([lng, lat]).addTo(instance);
        const pin = new maplibregl.Marker({ draggable: true, color: accent }).setLngLat([lng + 0.01, lat + 0.008]).addTo(instance);
        marker.current = pin;
        pin.on('dragend', () => {
          const p = pin.getLngLat();
          changeRef.current({ lat: Math.round(p.lat * 1e6) / 1e6, lng: Math.round(p.lng * 1e6) / 1e6 });
        });
        instance.on('click', (e) => {
          pin.setLngLat(e.lngLat);
          changeRef.current({ lat: Math.round(e.lngLat.lat * 1e6) / 1e6, lng: Math.round(e.lngLat.lng * 1e6) / 1e6 });
        });
        const timeout = window.setTimeout(() => !cancelled && setMapState((s) => (s === 'ready' ? s : 'error')), 15000);
        instance.once('load', () => {
          window.clearTimeout(timeout);
          if (cancelled) return;
          instance.addSource('zone', { type: 'geojson', data: { type: 'Feature', properties: {}, geometry: { type: 'Polygon', coordinates: [circle({ lat, lng }, radiusKm)] } } });
          instance.addLayer({ id: 'zone-fill', type: 'fill', source: 'zone', paint: { 'fill-color': sun, 'fill-opacity': 0.14 } });
          instance.addLayer({ id: 'zone-line', type: 'line', source: 'zone', paint: { 'line-color': accent, 'line-width': 1.5, 'line-dasharray': [2, 2] } });
          setMapState('ready');
        });
      } catch {
        if (!cancelled) setMapState('error');
      }
    })();
    return () => {
      cancelled = true;
      map.current?.remove();
      map.current = null;
    };
  }, [lat, lng, radiusKm]);

  // Keep the pin where the chosen point is (e.g. from a saved address or the location button).
  useEffect(() => {
    if (value && marker.current) marker.current.setLngLat([value.lng, value.lat]);
  }, [value]);

  const locate = () => {
    if (!('geolocation' in navigator)) {
      setGeo('denied');
      return;
    }
    setGeo('locating');
    navigator.geolocation.getCurrentPosition(
      (pos) => {
        const point = { lat: Math.round(pos.coords.latitude * 1e6) / 1e6, lng: Math.round(pos.coords.longitude * 1e6) / 1e6 };
        setGeo('idle');
        changeRef.current(point);
        map.current?.flyTo({ center: [point.lng, point.lat], zoom: 14 });
      },
      () => setGeo('denied'),
      { enableHighAccuracy: true, timeout: 10000 },
    );
  };

  const distance = value ? distanceKm(house, value) : null;
  const inside = distance !== null && distance <= radiusKm;

  return (
    <div className="flex flex-col gap-4">
      <div className="flex flex-col gap-1">
        <p className="t-heading-sm">{t('title')}</p>
        <p className="t-small text-muted">{t('hint', { km: formatNumber(radiusKm, locale) })}</p>
      </div>
      <div className="relative aspect-[4/3] w-full overflow-hidden bg-surface md:aspect-[16/10]">
        <div ref={box} className="absolute inset-0 h-full w-full" role="region" aria-label={t('label')} />
        {mapState === 'error' ? (
          <p className="t-small absolute inset-0 flex items-center justify-center p-6 text-center text-muted">
            <Icon name="map" size={22} className="me-2" />
            {t('unavailable')}
          </p>
        ) : null}
      </div>
      <div className="flex flex-wrap items-center gap-4">
        <Button variant="secondary" size="sm" leadingIcon="pin" onClick={locate} disabled={geo === 'locating'}>
          {geo === 'locating' ? t('locating') : t('useLocation')}
        </Button>
        {distance !== null ? (
          <p className={inside ? 't-small' : 't-small text-danger'} role="status">
            {t('distance', { km: formatNumber(Math.round(distance * 10) / 10, locale) })}
            {inside ? null : ` — ${t('outside')}`}
          </p>
        ) : null}
        {geo === 'denied' ? (
          <p role="alert" className="t-small text-danger">
            {t('denied')}
          </p>
        ) : null}
      </div>
    </div>
  );
}
