import type { Allergen, DietaryTag } from '@/lib/db/schema';
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
