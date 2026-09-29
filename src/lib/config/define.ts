/**
 * Types and helper for `restaurant.config.ts` — the single white-label configuration file.
 * Everything a new restaurant needs to change (identity, locales, money, flags, policies) lives there;
 * operational data (branches, menus, hours, tables…) lives in the database and is edited in the admin.
 */

export type Locale = 'ar' | 'en';
export type Localized<T = string> = Record<Locale, T>;

/** Unicode numbering systems we support for each locale ('arab' = Arabic-Indic, 'latn' = Western). */
export type NumberingSystem = 'arab' | 'latn';

export type FeatureFlag =
  | 'reservations'
  | 'ordering'
  | 'delivery'
  | 'pickup'
  | 'dineInQr'
  | 'events'
  | 'giftCards'
  | 'loyalty'
  | 'journal'
  | 'careers'
  | 'reviews'
  | 'privateDining'
  | 'newsletter'
  | 'aiConcierge'
  | 'alcohol'
  | 'ambientSound'
  | 'preloader'
  | 'webgl'
  | 'analytics'
  | 'seasonalModes';

export type OrderChannel = 'delivery' | 'pickup' | 'dine_in';

export interface LoyaltyTier {
  id: string;
  name: Localized;
  /** Lifetime points needed to reach the tier. */
  minPoints: number;
  /** Points earned per currency unit are multiplied by this. */
  multiplier: number;
}

export interface RestaurantConfig {
  id: string;
  name: Localized;
  legalName: Localized;
  tagline: Localized;
  description: Localized;
  cuisine: Localized;
  priceRange: '$' | '$$' | '$$$' | '$$$$';
  locales: readonly Locale[];
  defaultLocale: Locale;
  /** Digits used when formatting numbers, prices and dates per locale. */
  numerals: Record<Locale, NumberingSystem>;
  currency: string;
  /** ISO 3166-1 alpha-2 — default region for phone validation. */
  country: string;
  defaultTimeZone: string;
  flagshipBranchSlug: string;
  contact: { email: string; press: string; careers: string; events: string; privacy: string };
  features: Record<FeatureFlag, boolean>;
  tax: { name: Localized; rate: number; pricesIncludeTax: boolean; registrationNumber: string };
  serviceCharge: { rate: number; channels: OrderChannel[] };
  tips: { enabled: boolean; presets: number[]; channels: OrderChannel[] };
  loyalty: {
    /** Points earned per 1 major currency unit spent (before tier multiplier). */
    pointsPerUnit: number;
    /** How many points equal 1 major currency unit when redeemed at checkout. */
    pointsPerUnitRedeemed: number;
    /** Maximum share of an order subtotal that can be paid with points (0–1). */
    maxRedeemShare: number;
    tiers: LoyaltyTier[];
    signupBonus: number;
  };
  giftCards: { min: number; max: number; presets: number[]; designs: string[]; validityMonths: number };
  reservations: {
    maxPartyOnline: number;
    /** Parties at or above this size are routed to private dining. */
    privateDiningThreshold: number;
    holdMinutes: number;
    /** Guests can modify or cancel online until this many hours before the booking. */
    modifyCutoffHours: number;
    /** Deposit per guest (major units) for parties of at least `minParty`; 0 disables. */
    deposit: { minParty: number; perGuest: number };
    reminderHoursBefore: number;
    bookingWindowDays: number;
  };
  ordering: { asapBufferMinutes: number; scheduleDaysAhead: number; slotMinutes: number };
  seo: { titleTemplate: Localized; defaultOgImage: string };
  /** When false no consent banner is shown because only essential cookies exist. */
  nonEssentialCookies: boolean;
}

export function defineRestaurantConfig<const T extends RestaurantConfig>(config: T): T {
  return config;
}
