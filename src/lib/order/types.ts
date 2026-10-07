import type { PricingResult } from '@/lib/domain/pricing';

/** Client-safe shapes for ordering (quotes, checkout data). Amounts are minor units; times are ISO strings. */

export type OrderChannel = 'delivery' | 'pickup' | 'dine_in';

/** sessionStorage key: the tracking page empties the basket once the order placed from it is on its way. */
export const LAST_ORDER_KEY = 'zill_last_order';

export interface ZoneView {
  id: string;
  name: string;
  kind: 'area' | 'radius';
  areas: string[];
  radiusKm: number | null;
  fee: number;
  minOrder: number;
  etaMinutes: number;
}

export interface CheckoutBranch {
  slug: string;
  name: string;
  shortName: string;
  address: string;
  phone: string;
  lat: number;
  lng: number;
  timeZone: string;
  channels: { delivery: boolean; pickup: boolean };
  zones: ZoneView[];
}

export interface SavedAddress {
  id: string;
  label: string;
  zoneId: string | null;
  area: string;
  street: string;
  building: string | null;
  floor: string | null;
  notes: string | null;
  lat: number | null;
  lng: number | null;
  isDefault: boolean;
}

export interface QuoteLineView {
  key: string;
  slug: string;
  name: string;
  qty: number;
  unitPrice: number;
  lineTotal: number;
  options: string[];
  note: string;
  problem: 'unknown' | 'unavailable' | 'sold_out' | 'not_served' | 'modifiers' | 'not_orderable' | null;
}

export interface TimeOptionView {
  at: string;
  available: boolean;
  servesAll: boolean;
}

export interface QuoteView {
  lines: QuoteLineView[];
  pricing: PricingResult;
  promo: { code: string; ok: true; discount: number } | { code: string; ok: false; reason: string; minOrder?: number } | null;
  giftCard: { code: string; ok: true; balance: number; applied: number } | { code: string; ok: false; reason: string } | null;
  loyalty: { balance: number; maxPoints: number; maxValue: number; points: number; value: number } | null;
  zone: ZoneView | null;
  zoneProblem: string | null;
  timing: {
    prepMinutes: number;
    travelMinutes: number;
    readyAt: string | null;
    promisedAt: string | null;
    asapAvailable: boolean;
    asapAt: string | null;
    options: TimeOptionView[];
    problem: string | null;
  };
  tipAllowed: boolean;
  serviceChargeRate: number;
  taxRate: number;
  problems: string[];
  canPlace: boolean;
}

export interface CheckoutLine {
  slug: string;
  qty: number;
  optionIds: string[];
  note: string;
}
