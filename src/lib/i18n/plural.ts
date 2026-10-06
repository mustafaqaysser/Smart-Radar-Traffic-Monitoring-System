import { formatNumber } from './format';

/**
 * Arguments for an ICU plural message: `count` selects the plural form, `n` is the number already formatted
 * with the configured digits. Messages write `{n}`, never `#` — ICU's `#` ignores the numbering system.
 */
export function plural(count: number, locale: string): { count: number; n: string } {
  return { count, n: formatNumber(count, locale) };
}
