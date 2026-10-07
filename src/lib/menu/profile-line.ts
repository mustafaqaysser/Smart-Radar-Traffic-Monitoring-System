import { formatList } from '@/lib/i18n/format';
import { profileVerdict, type DietProfile } from './filter';
import type { Allergen, DietaryTag } from './tags';

export type ProfileLine = { tone: 'suits' | 'warn' | 'note'; text: string } | null;

type Translate = (key: string, values?: Record<string, string>) => string;

/**
 * The line a dish shows against a dietary profile: "suits you", or the first reason it does not (an avoided
 * allergen first). `t` reads `menu.profile.*`, `tc` reads `common.*`; works on the server and in the browser.
 */
export function profileLine(item: { dietary: DietaryTag[]; allergens: Allergen[]; spice: number }, profile: DietProfile | null, t: Translate, tc: Translate, locale: string): ProfileLine {
  if (!profile) return null;
  const v = profileVerdict(item, profile);
  // Mid-sentence in English the tag names read lower-case ("contains sesame"); Arabic has no case.
  const inline = (label: string) => (locale === 'en' ? label.toLowerCase() : label);
  if (v.suits) return { tone: 'suits', text: t('suits') };
  if (v.allergens.length) return { tone: 'warn', text: t('contains', { list: formatList(v.allergens.map((a) => inline(tc(`allergens.${a}`))), locale) }) };
  if (v.diets.length) return { tone: 'note', text: t('notDiet', { list: formatList(v.diets.map((d) => inline(tc(`dietary.${d}`))), locale) }) };
  return { tone: 'note', text: t('tooHot') };
}
