/** Keyless maps: OpenFreeMap vector tiles styled for the brand, plus deep links to navigation apps. */

export const MAP_STYLE_URL = 'https://tiles.openfreemap.org/styles/positron';

export interface GeoPoint {
  lat: number;
  lng: number;
}

export function googleMapsUrl(p: GeoPoint): string {
  return `https://www.google.com/maps/search/?api=1&query=${p.lat},${p.lng}`;
}

export function googleDirectionsUrl(p: GeoPoint): string {
  return `https://www.google.com/maps/dir/?api=1&destination=${p.lat},${p.lng}`;
}

export function appleMapsUrl(p: GeoPoint, label?: string): string {
  return `https://maps.apple.com/?daddr=${p.lat},${p.lng}${label ? `&q=${encodeURIComponent(label)}` : ''}`;
}

export function wazeUrl(p: GeoPoint): string {
  return `https://waze.com/ul?ll=${p.lat},${p.lng}&navigate=yes`;
}

/** Great-circle distance in kilometres. */
export function distanceKm(a: GeoPoint, b: GeoPoint): number {
  const R = 6371;
  const toRad = (d: number) => (d * Math.PI) / 180;
  const dLat = toRad(b.lat - a.lat);
  const dLng = toRad(b.lng - a.lng);
  const h = Math.sin(dLat / 2) ** 2 + Math.cos(toRad(a.lat)) * Math.cos(toRad(b.lat)) * Math.sin(dLng / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(h));
}

export function whatsappUrl(phone: string, text?: string): string {
  const digits = phone.replace(/\D/g, '');
  return `https://wa.me/${digits}${text ? `?text=${encodeURIComponent(text)}` : ''}`;
}

export function telUrl(phone: string): string {
  return `tel:${phone.replace(/[^\d+]/g, '')}`;
}
