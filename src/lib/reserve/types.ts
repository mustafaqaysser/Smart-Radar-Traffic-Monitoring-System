import type { ImageView } from '@/lib/menu/view';

/** Shapes and constants shared by the booking flow's server code and its client components (all serialisable). */

export const OCCASIONS = ['birthday', 'anniversary', 'business', 'date', 'family', 'celebration'] as const;
export type Occasion = (typeof OCCASIONS)[number];
/** Seating choices: no preference, or one of the table areas (kept in step with TABLE_AREAS in the schema). */
export const AREA_CHOICES = ['any', 'courtyard', 'liwan', 'roof', 'private'] as const;
export type AreaChoice = (typeof AREA_CHOICES)[number];

export function isAreaChoice(value: string): value is AreaChoice {
  return (AREA_CHOICES as readonly string[]).includes(value);
}

export type DayState = 'open' | 'closed' | 'blocked';

/** Everything the booking flow needs about one house, in the visitor's language. */
export interface FlowBranch {
  slug: string;
  name: string;
  shortName: string;
  city: string;
  address: string;
  phone: string;
  timeZone: string;
  lat: number;
  lng: number;
  image: ImageView | null;
  periods: { key: string; name: string }[];
  /** Areas with tables that can be booked online. */
  areas: string[];
  first: string;
  last: string;
  /** State of every date in the booking window. */
  days: Record<string, DayState>;
  /** Holiday names on the dates they apply. */
  notes: Record<string, string>;
}

export interface FlowSetup {
  branches: FlowBranch[];
  maxPartyOnline: number;
  /** Deposit rule; `perGuest` in minor units (0 disables deposits). */
  deposit: { minParty: number; perGuest: number };
  holdMinutes: number;
  cutoffHours: number;
  bookingWindowDays: number;
  /** Minutes an unpaid deposit keeps the table. */
  depositWindowMinutes: number;
  privateDining: boolean;
}

export interface GuestPrefill {
  name: string;
  email: string;
  phone: string;
}

export type SlotReason = 'past' | 'pacing' | 'full';

export interface PublicSlot {
  time: string;
  minutes: number;
  periodKey: string;
  available: boolean;
  reason: SlotReason | null;
  areas: string[];
  startsAt: string;
}

export interface SlotsResponse {
  date: string;
  slots: PublicSlot[];
  sunset: { at: string; minutes: number } | null;
}
