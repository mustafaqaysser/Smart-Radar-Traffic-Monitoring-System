import { describe, expect, it } from 'vitest';
import { activeFilterCount, EMPTY_FILTERS, passesFilters } from '@/lib/menu/filter';
import { matchDishes, scoreDish, type Matchable } from '@/lib/menu/matchmaker';
import { normalizeForSearch } from '@/lib/i18n/arabic';

const dish = (over: Partial<Matchable & { search: string }> = {}) => ({
  slug: 'x',
  signature: false,
  spice: 0,
  calories: 500,
  allergens: [] as string[],
  dietary: [] as string[],
  categories: ['to-share'],
  soldOut: false,
  available: true,
  search: normalizeForSearch('حمّص بالسمن Hummus with ghee chickpeas'),
  ...over,
});

describe('menu filters', () => {
  it('matches Arabic queries regardless of diacritics, hamza and taa marbuta', () => {
    const d = { ...dish(), allergens: [], dietary: [] } as never;
    expect(passesFilters(d, { ...EMPTY_FILTERS, query: 'حمص' })).toBe(true);
    expect(passesFilters(d, { ...EMPTY_FILTERS, query: 'الحمّص' })).toBe(true);
    expect(passesFilters(d, { ...EMPTY_FILTERS, query: 'بالسمن' })).toBe(true);
    expect(passesFilters(d, { ...EMPTY_FILTERS, query: 'لحم' })).toBe(false);
    expect(passesFilters(d, { ...EMPTY_FILTERS, query: 'ghee' })).toBe(true);
    expect(passesFilters(d, { ...EMPTY_FILTERS, query: 'GHEE chick' })).toBe(true);
  });

  it('requires every chosen diet, hides avoided allergens and caps heat', () => {
    const d = dish({ dietary: ['vegetarian'], allergens: ['sesame'], spice: 2 }) as never;
    expect(passesFilters(d, { ...EMPTY_FILTERS, diets: ['vegetarian'] })).toBe(true);
    expect(passesFilters(d, { ...EMPTY_FILTERS, diets: ['vegetarian', 'vegan'] })).toBe(false);
    expect(passesFilters(d, { ...EMPTY_FILTERS, avoid: ['sesame'] })).toBe(false);
    expect(passesFilters(d, { ...EMPTY_FILTERS, maxSpice: 1 })).toBe(false);
    expect(passesFilters(d, { ...EMPTY_FILTERS, maxSpice: 2 })).toBe(true);
    expect(activeFilterCount({ query: ' x ', diets: ['vegan'], avoid: ['milk', 'eggs'], maxSpice: 0 })).toBe(5);
  });
});

describe('matchmaker', () => {
  const fish = dish({ slug: 'fish', allergens: ['fish'], categories: ['sea'], calories: 650 });
  const grill = dish({ slug: 'grill', categories: ['embers'], calories: 780, spice: 2 });
  const salad = dish({ slug: 'salad', dietary: ['vegetarian', 'vegan'], categories: ['to-begin'], calories: 280 });
  const cake = dish({ slug: 'cake', categories: ['sweets'], dietary: ['vegetarian'], calories: 450, signature: true });
  const tea = dish({ slug: 'tea', categories: ['tea'] });
  const all = [fish, grill, salad, cake, tea];

  it('follows the mood', () => {
    expect(matchDishes(all, { hunger: 'proper', mood: 'sea', heat: 'some' })[0]?.item.slug).toBe('fish');
    expect(matchDishes(all, { hunger: 'proper', mood: 'embers', heat: 'all' })[0]?.item.slug).toBe('grill');
    expect(matchDishes(all, { hunger: 'light', mood: 'garden', heat: 'none' })[0]?.item.slug).toBe('salad');
    expect(matchDishes(all, { hunger: 'light', mood: 'sweet', heat: 'none' })[0]?.item.slug).toBe('cake');
  });

  it('never recommends drinks, sold-out or unavailable dishes', () => {
    const res = matchDishes([tea, { ...cake, soldOut: true }, { ...salad, available: false }], { hunger: 'light', mood: 'surprise', heat: 'none' });
    expect(res).toEqual([]);
  });

  it('penalises heat when none is wanted and explains its choices', () => {
    expect(scoreDish(grill, { hunger: 'proper', mood: 'embers', heat: 'none' }).score).toBeLessThan(scoreDish(grill, { hunger: 'proper', mood: 'embers', heat: 'all' }).score);
    expect(scoreDish(cake, { hunger: 'light', mood: 'sweet', heat: 'none' }).reasons).toContain('signature');
  });
});
