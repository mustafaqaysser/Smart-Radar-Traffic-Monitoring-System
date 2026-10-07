import { parsePhoneNumberFromString, type CountryCode } from 'libphonenumber-js/min';
import restaurantConfig from '@config';
import { normalizeDigits } from '@/lib/i18n/digits';

/**
 * Phone numbers are stored in E.164 (+9665…). Input may use Arabic-Indic digits, spaces and a local 05… form;
 * numbers without a country code are read as the restaurant's country.
 */
export function normalizePhone(input: string, country: CountryCode = restaurantConfig.country as CountryCode): string | null {
  const cleaned = normalizeDigits(input).replace(/[^\d+]/g, '');
  if (cleaned.length < 6) return null;
  const parsed = parsePhoneNumberFromString(cleaned, country);
  if (!parsed || !parsed.isValid()) return null;
  return parsed.number;
}

/** Display form: international with spaces (+966 50 555 0111). Always shown left-to-right. */
export function formatPhone(e164: string): string {
  const parsed = parsePhoneNumberFromString(e164);
  return parsed ? parsed.formatInternational() : e164;
}

/** True for mobile numbers (used to decide whether WhatsApp/SMS makes sense). */
export function isMobile(e164: string): boolean {
  const parsed = parsePhoneNumberFromString(e164);
  const type = parsed?.getType();
  return type === 'MOBILE' || type === 'FIXED_LINE_OR_MOBILE' || type === undefined;
}
