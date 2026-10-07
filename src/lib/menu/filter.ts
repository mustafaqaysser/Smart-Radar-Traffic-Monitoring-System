import { ALLERGENS, DIETARY_TAGS, type Allergen, type DietaryTag } from '@/lib/menu/tags';
import { searchTokens } from '@/lib/i18n/arabic';

export interface MenuFilters {
  query: string;
  diets: DietaryTag[];
  avoid: Allergen[];
  /** Maximum heat (0–3), or null for any. */
  maxSpice: number | null;
}

export const EMPTY_FILTERS: MenuFilters = { query: '', diets: [], avoid: [], maxSpice: null };

interface Filterable {
  search: string;
  dietary: DietaryTag[];
  allergens: Allergen[];
  spice: number;
}

/** True when a dish passes every active filter. Search is Arabic-aware and matches every word. */
export function passesFilters(item: Filterable, f: MenuFilters): boolean {
  if (f.diets.some((d) => !item.dietary.includes(d))) return false;
  if (f.avoid.some((a) => item.allergens.includes(a))) return false;
  if (f.maxSpice !== null && item.spice > f.maxSpice) return false;
  const tokens = searchTokens(f.query);
  if (tokens.length && !tokens.every((token) => item.search.includes(token))) return false;
  return true;
}

export function activeFilterCount(f: MenuFilters): number {
  return (f.query.trim() ? 1 : 0) + f.diets.length + f.avoid.length + (f.maxSpice !== null ? 1 : 0);
}

/** A signed-in guest's standing dietary profile (what they eat, what they avoid, how much heat). */
export interface DietProfile {
  diets: DietaryTag[];
  avoid: Allergen[];
  maxSpice: number | null;
}

/** The stored profile, keeping only known tags; null when it asks for nothing. */
export function toDietProfile(stored: { diets?: string[]; avoidAllergens?: string[]; maxSpice?: number | null } | null | undefined): DietProfile | null {
  if (!stored) return null;
  const diets = (stored.diets ?? []).filter((d): d is DietaryTag => (DIETARY_TAGS as readonly string[]).includes(d));
  const avoid = (stored.avoidAllergens ?? []).filter((a): a is Allergen => (ALLERGENS as readonly string[]).includes(a));
  const maxSpice = typeof stored.maxSpice === 'number' && stored.maxSpice >= 0 && stored.maxSpice <= 3 ? stored.maxSpice : null;
  return diets.length || avoid.length || maxSpice !== null ? { diets, avoid, maxSpice } : null;
}

export interface ProfileVerdict {
  suits: boolean;
  /** Allergens in the dish that the guest avoids. */
  allergens: Allergen[];
  /** Diets the dish does not meet. */
  diets: DietaryTag[];
  tooHot: boolean;
}

/** How a dish sits with a dietary profile: it suits, or the reasons it does not. */
export function profileVerdict(item: Pick<Filterable, 'dietary' | 'allergens' | 'spice'>, profile: DietProfile): ProfileVerdict {
  const allergens = profile.avoid.filter((a) => item.allergens.includes(a));
  const diets = profile.diets.filter((d) => !item.dietary.includes(d));
  const tooHot = profile.maxSpice !== null && item.spice > profile.maxSpice;
  return { suits: !allergens.length && !diets.length && !tooHot, allergens, diets, tooHot };
}
