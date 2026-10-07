import type { Allergen, DietaryTag, MenuKind, MenuWindow, OrderingSettings, ReservationSettings, TableArea } from '@/lib/db/schema';
import type { LocalizedText } from '@/lib/i18n/localized';
import type { SpecialDay, WeeklyRange } from '@/lib/domain/hours';

/** Plain, serialisable shapes returned by queries (safe to cache and to pass to client components). */

export interface MediaDTO {
  id: string;
  kind: 'image' | 'video';
  src: string;
  width: number;
  height: number;
  blur: string | null;
  focalX: number;
  focalY: number;
  alt: LocalizedText;
  poster: string | null;
  sources: { src: string; type: string }[] | null;
  credit: { author: string; source: string; license: string } | null;
}

export interface ZoneDTO {
  id: string;
  name: LocalizedText;
  kind: 'area' | 'radius';
  areas: LocalizedText[];
  radiusKm: number | null;
  fee: number;
  minOrder: number;
  etaMinutes: number;
}

export interface TableDTO {
  id: string;
  code: string;
  label: string;
  area: TableArea;
  minSeats: number;
  maxSeats: number;
  combineGroup: string | null;
  reservable: boolean;
  isActive: boolean;
  sortOrder: number;
}

export interface BranchDTO {
  id: string;
  slug: string;
  name: LocalizedText;
  shortName: LocalizedText;
  city: LocalizedText;
  district: LocalizedText;
  address: LocalizedText;
  story: LocalizedText | null;
  timeZone: string;
  lat: number;
  lng: number;
  phone: string;
  whatsapp: string | null;
  email: string;
  parking: LocalizedText | null;
  accessibility: LocalizedText | null;
  hero: MediaDTO | null;
  reservationSettings: ReservationSettings;
  orderingSettings: OrderingSettings;
  reservationsEnabled: boolean;
  deliveryEnabled: boolean;
  pickupEnabled: boolean;
  dineInEnabled: boolean;
  orderingPaused: boolean;
  busyMode: boolean;
  venueHours: WeeklyRange[];
  kitchenHours: WeeklyRange[];
  specials: (SpecialDay & { name: LocalizedText })[];
  periods: { key: string; name: LocalizedText; weekdays: number[]; start: string; end: string; maxCoversPerSlot: number | null }[];
  zones: ZoneDTO[];
}

export interface ModifierOptionDTO {
  id: string;
  name: LocalizedText;
  priceDelta: number;
  isDefault: boolean;
  isAvailable: boolean;
}

export interface ModifierGroupDTO {
  id: string;
  key: string;
  name: LocalizedText;
  minSelect: number;
  maxSelect: number;
  options: ModifierOptionDTO[];
}

export interface BranchItemState {
  available: boolean;
  soldOut: boolean;
  price: number;
}

export interface MenuItemDTO {
  id: string;
  slug: string;
  name: LocalizedText;
  description: LocalizedText;
  story: LocalizedText | null;
  ingredients: LocalizedText | null;
  price: number;
  calories: number | null;
  spiceLevel: number;
  allergens: Allergen[];
  dietary: DietaryTag[];
  image: MediaDTO | null;
  isSignature: boolean;
  isAlcoholic: boolean;
  orderable: boolean;
  upsell: boolean;
  prepMinutes: number;
  pairings: string[];
  tags: string[];
  modifierGroups: ModifierGroupDTO[];
  /** Keyed by branch id. */
  branches: Record<string, BranchItemState>;
  isActive: boolean;
}

export interface MenuCategoryDTO {
  id: string;
  slug: string;
  name: LocalizedText;
  description: LocalizedText | null;
  items: string[];
}

export interface MenuDTO {
  id: string;
  slug: string;
  kind: MenuKind;
  name: LocalizedText;
  hour: LocalizedText | null;
  description: LocalizedText | null;
  schedule: MenuWindow[];
  branchIds: string[] | null;
  seasonalModeId: string | null;
  isTasting: boolean;
  tastingPrice: number | null;
  image: MediaDTO | null;
  categories: MenuCategoryDTO[];
}

export interface MenuCatalog {
  menus: MenuDTO[];
  /** Keyed by slug. */
  items: Record<string, MenuItemDTO>;
}

export interface SeasonalModeDTO {
  id: string;
  slug: string;
  kind: string;
  name: LocalizedText;
  banner: LocalizedText | null;
  startDate: string;
  endDate: string;
  theme: string;
  hours: Record<string, { opens: string; closes: string }[]> | null;
  isEnabled: boolean;
}
