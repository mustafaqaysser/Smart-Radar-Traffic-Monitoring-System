/**
 * Every number, price, date and time in the UI goes through these helpers, so digits follow
 * `restaurantConfig.numerals` (Arabic-Indic by default in Arabic) and times follow the branch's zone.
 */
import restaurantConfig from '@config';

type AppLocale = 'ar' | 'en';

const BASE: Record<AppLocale, string> = { ar: 'ar-SA', en: 'en-GB' };

export function numberingSystem(locale: string): string {
  return (restaurantConfig.numerals as Record<string, string>)[locale] ?? 'latn';
}

/**
 * BCP-47 tag with the configured numbering system and the Gregorian calendar, e.g. 'ar-SA-u-ca-gregory-nu-arab'.
 * The calendar is explicit because engines disagree on ar-SA's default (some use Umm al-Qura), which would
 * make server and browser render different dates. Hijri dates are shown deliberately via `formatHijri()`.
 */
export function intlLocale(locale: string): string {
  const base = BASE[(locale as AppLocale)] ?? locale;
  return `${base}-u-ca-gregory-nu-${numberingSystem(locale)}`;
}

/** An instant's date in the Umm al-Qura calendar (e.g. "٢٥ ربيع الآخر ١٤٤٨"), shown alongside Gregorian dates. */
export function formatHijri(date: Date, locale: string, timeZone: string): string {
  const base = BASE[(locale as AppLocale)] ?? locale;
  return new Intl.DateTimeFormat(`${base}-u-ca-islamic-umalqura-nu-${numberingSystem(locale)}`, { day: 'numeric', month: 'long', year: 'numeric', timeZone }).format(date);
}

const cache = new Map<string, Intl.NumberFormat | Intl.DateTimeFormat>();
function nf(locale: string, options: Intl.NumberFormatOptions): Intl.NumberFormat {
  const key = `n|${locale}|${JSON.stringify(options)}`;
  let f = cache.get(key) as Intl.NumberFormat | undefined;
  if (!f) {
    f = new Intl.NumberFormat(intlLocale(locale), options);
    cache.set(key, f);
  }
  return f;
}
function df(locale: string, options: Intl.DateTimeFormatOptions): Intl.DateTimeFormat {
  const key = `d|${locale}|${JSON.stringify(options)}`;
  let f = cache.get(key) as Intl.DateTimeFormat | undefined;
  if (!f) {
    f = new Intl.DateTimeFormat(intlLocale(locale), options);
    cache.set(key, f);
  }
  return f;
}

/** Money from minor units. Whole amounts drop the decimals ("٤٥ ر.س." / "SAR 45"). */
export function formatMoney(minor: number, locale: string, options: { currency?: string; always2?: boolean } = {}): string {
  const currency = options.currency ?? restaurantConfig.currency;
  const whole = minor % 100 === 0 && !options.always2;
  return nf(locale, {
    style: 'currency',
    currency,
    currencyDisplay: locale === 'ar' ? 'symbol' : 'code',
    minimumFractionDigits: whole ? 0 : 2,
    maximumFractionDigits: 2,
  }).format(minor / 100);
}

/** A bare amount without currency, for tight layouts next to a currency label. */
export function formatAmount(minor: number, locale: string): string {
  const whole = minor % 100 === 0;
  return nf(locale, { minimumFractionDigits: whole ? 0 : 2, maximumFractionDigits: 2 }).format(minor / 100);
}

export function formatNumber(value: number, locale: string, options: Intl.NumberFormatOptions = {}): string {
  return nf(locale, options).format(value);
}

export function formatPercent(fraction: number, locale: string): string {
  return nf(locale, { style: 'percent', maximumFractionDigits: 1 }).format(fraction);
}

/** Date of an instant, in a zone. */
export function formatDate(date: Date, locale: string, timeZone: string, options: Intl.DateTimeFormatOptions = { weekday: 'long', day: 'numeric', month: 'long' }): string {
  return df(locale, { ...options, timeZone }).format(date);
}

/** Clock time of an instant, in a zone ("٧:٣٠ م" / "7:30 pm"). */
export function formatClock(date: Date, locale: string, timeZone: string): string {
  return df(locale, { hour: 'numeric', minute: '2-digit', hourCycle: 'h12', timeZone }).format(date);
}

export function formatDateTime(date: Date, locale: string, timeZone: string): string {
  return df(locale, { weekday: 'short', day: 'numeric', month: 'short', hour: 'numeric', minute: '2-digit', hourCycle: 'h12', timeZone }).format(date);
}

/** A wall-clock 'HH:mm' string, formatted without any zone arithmetic. */
export function formatWallTime(hhmm: string, locale: string): string {
  const [h, m] = hhmm.split(':').map(Number) as [number, number];
  const d = new Date(Date.UTC(2020, 0, 1, h % 24, m));
  return df(locale, { hour: 'numeric', minute: '2-digit', hourCycle: 'h12', timeZone: 'UTC' }).format(d);
}

/** A calendar date 'YYYY-MM-DD', formatted without zone arithmetic. */
export function formatDateString(value: string, locale: string, options: Intl.DateTimeFormatOptions = { weekday: 'long', day: 'numeric', month: 'long' }): string {
  const [y, m, d] = value.split('-').map(Number) as [number, number, number];
  return df(locale, { ...options, timeZone: 'UTC' }).format(new Date(Date.UTC(y, m - 1, d, 12)));
}

/** Weekday name for 0 = Sunday … 6 = Saturday. */
export function formatWeekday(weekday: number, locale: string, style: 'long' | 'short' = 'long'): string {
  // 2023-01-01 was a Sunday.
  return df(locale, { weekday: style, timeZone: 'UTC' }).format(new Date(Date.UTC(2023, 0, 1 + weekday, 12)));
}

export function formatRelativeMinutes(minutes: number, locale: string): string {
  const rtf = new Intl.RelativeTimeFormat(intlLocale(locale), { numeric: 'auto' });
  if (Math.abs(minutes) < 60) return rtf.format(Math.round(minutes), 'minute');
  if (Math.abs(minutes) < 60 * 24) return rtf.format(Math.round(minutes / 60), 'hour');
  return rtf.format(Math.round(minutes / 1440), 'day');
}

export function formatList(items: string[], locale: string, type: Intl.ListFormatType = 'conjunction'): string {
  return new Intl.ListFormat(intlLocale(locale), { style: 'long', type }).format(items);
}

/** Duration in minutes → "1 hr 45 min" style using Intl units. */
export function formatDurationMinutes(minutes: number, locale: string): string {
  const h = Math.floor(minutes / 60);
  const m = minutes % 60;
  const parts: string[] = [];
  if (h) parts.push(nf(locale, { style: 'unit', unit: 'hour', unitDisplay: 'long' }).format(h));
  if (m || !h) parts.push(nf(locale, { style: 'unit', unit: 'minute', unitDisplay: 'long' }).format(m));
  return formatList(parts, locale, 'unit');
}
