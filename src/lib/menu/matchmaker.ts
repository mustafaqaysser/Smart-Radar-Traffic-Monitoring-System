/**
 * "Help me choose": scores dishes from the menu being shown against three answers. Pure and deterministic,
 * so the same answers always give the same three dishes.
 */
export type Hunger = 'light' | 'proper' | 'share';
export type Mood = 'sea' | 'embers' | 'garden' | 'sweet' | 'surprise';
export type Heat = 'none' | 'some' | 'all';

export interface MatchInput {
  hunger: Hunger;
  mood: Mood;
  heat: Heat;
}

export interface Matchable {
  slug: string;
  signature: boolean;
  spice: number;
  calories: number | null;
  allergens: string[];
  dietary: string[];
  categories: string[];
  soldOut: boolean;
  available: boolean;
}

export type Reason = 'signature' | 'light' | 'hearty' | 'share' | 'sea' | 'embers' | 'garden' | 'sweet' | 'mild' | 'hot';

const SHARING = new Set(['to-share', 'to-begin', 'pots', 'pot', 'iftar-table', 'tannour']);
const LIGHT = new Set(['to-share', 'to-begin', 'tannour', 'little-plates', 'sweet-mornings', 'break-fast', 'coffee', 'tea']);
const SWEET = new Set(['sweets', 'afternoon-sweets', 'sweet-mornings', 'iftar-sweets']);
const EMBERS = new Set(['embers']);
const DRINKS = new Set(['coffee', 'tea', 'coolers', 'morning-cups']);

const isSea = (m: Matchable) => m.allergens.some((a) => a === 'fish' || a === 'crustaceans' || a === 'molluscs') || m.categories.some((c) => c === 'sea' || c === 'sea-and-stone');
const isGarden = (m: Matchable) => m.dietary.includes('vegetarian') || m.dietary.includes('vegan');

export function scoreDish(m: Matchable, input: MatchInput): { score: number; reasons: Reason[] } {
  const reasons: Reason[] = [];
  let score = 0;
  const cats = new Set(m.categories);
  const drink = m.categories.length > 0 && m.categories.every((c) => DRINKS.has(c));
  if (drink) return { score: -100, reasons };

  // Mood is the strongest signal.
  const sweet = m.categories.some((c) => SWEET.has(c));
  if (input.mood === 'sea') score += isSea(m) ? 6 : -6;
  if (input.mood === 'embers') score += m.categories.some((c) => EMBERS.has(c)) ? 6 : -4;
  if (input.mood === 'garden') score += isGarden(m) ? 6 : -8;
  if (input.mood === 'sweet') score += sweet ? 6 : -8;
  if (input.mood !== 'sweet' && sweet) score -= 3;
  if (input.mood === 'surprise' && m.signature) score += 3;
  if (input.mood === 'sea' && isSea(m)) reasons.push('sea');
  if (input.mood === 'embers' && cats.has('embers')) reasons.push('embers');
  if (input.mood === 'garden' && isGarden(m)) reasons.push('garden');
  if (input.mood === 'sweet' && sweet) reasons.push('sweet');

  // Hunger.
  const kcal = m.calories ?? 550;
  if (input.hunger === 'light') {
    if (kcal <= 480 || m.categories.some((c) => LIGHT.has(c))) {
      score += 3;
      reasons.push('light');
    } else score -= 2;
  }
  if (input.hunger === 'proper') {
    if (kcal >= 600 && !m.categories.some((c) => LIGHT.has(c))) {
      score += 3;
      reasons.push('hearty');
    }
  }
  if (input.hunger === 'share') {
    if (m.categories.some((c) => SHARING.has(c))) {
      score += 3;
      reasons.push('share');
    }
  }

  // Heat.
  if (input.heat === 'none') {
    if (m.spice === 0) reasons.push('mild');
    else score -= 4 * m.spice;
  }
  if (input.heat === 'all' && m.spice >= 2) {
    score += 2;
    reasons.push('hot');
  }
  if (input.heat === 'some' && m.spice >= 3) score -= 2;

  if (m.signature) {
    score += 1.5;
    reasons.unshift('signature');
  }
  return { score, reasons: reasons.slice(0, 2) };
}

/** The three best dishes (available, not sold out, positive score), stable for equal scores. */
export function matchDishes<T extends Matchable>(items: T[], input: MatchInput, limit = 3): { item: T; reasons: Reason[] }[] {
  return items
    .filter((i) => i.available && !i.soldOut)
    .map((item, index) => ({ item, index, ...scoreDish(item, input) }))
    .filter((r) => r.score > 0)
    .sort((a, b) => b.score - a.score || a.index - b.index)
    .slice(0, limit)
    .map(({ item, reasons }) => ({ item, reasons }));
}
