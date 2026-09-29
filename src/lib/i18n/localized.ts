import type { Locale } from '../config/define';

/** Localized text stored as JSON: `{ ar: '…', en: '…' }`. New locales add keys, not columns. */
export type LocalizedText = Partial<Record<Locale, string>> & { ar?: string; en?: string };

/** Picks the text for `locale`, falling back to the other locales so a field is never blank. */
export function tr(value: LocalizedText | null | undefined, locale: string, fallbackOrder: readonly string[] = ['ar', 'en']): string {
  if (!value) return '';
  const direct = (value as Record<string, string | undefined>)[locale];
  if (direct) return direct;
  for (const l of fallbackOrder) {
    const v = (value as Record<string, string | undefined>)[l];
    if (v) return v;
  }
  return '';
}

export function localized(ar: string, en: string): LocalizedText {
  return { ar, en };
}
