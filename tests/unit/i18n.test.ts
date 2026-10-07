import { describe, expect, it } from 'vitest';
import { matchesSearch, normalizeArabic, normalizeForSearch } from '@/lib/i18n/arabic';
import { normalizeDigits, parseLocaleNumber, parseMoneyInput } from '@/lib/i18n/digits';
import { formatDate, formatDateString, formatHijri, formatMoney, formatWallTime, formatWeekday, intlLocale } from '@/lib/i18n/format';

describe('digits', () => {
  it('normalises Arabic-Indic and Eastern Arabic-Indic digits', () => {
    expect(normalizeDigits('٠١٢٣٤٥٦٧٨٩')).toBe('0123456789');
    expect(normalizeDigits('۰۱۲۳۴۵۶۷۸۹')).toBe('0123456789');
    expect(normalizeDigits('٤٥٫٥')).toBe('45.5');
  });
  it('parses numbers typed in any digits', () => {
    expect(parseLocaleNumber('٤')).toBe(4);
    expect(parseLocaleNumber('1,250')).toBe(1250);
    expect(parseLocaleNumber('abc')).toBeNaN();
    expect(parseMoneyInput('٤٥٫٥')).toBe(4550);
  });
});

describe('arabic search', () => {
  it('strips tashkeel and unifies letter variants', () => {
    expect(normalizeArabic('قَهْوَة')).toBe('قهوه');
    expect(normalizeArabic('أمّ علي')).toBe('ام علي');
    expect(normalizeArabic('حلوى')).toBe('حلوي');
    expect(normalizeArabic('إفطار')).toBe('افطار');
  });
  it('matches regardless of hamza, taa marbuta, alef maqsura and tashkeel', () => {
    expect(matchesSearch('كُنافة بالجبنة النابلسية', 'كنافه')).toBe(true);
    expect(matchesSearch('مهلّبية بالورد', 'مهلبيه ورد')).toBe(true);
    expect(matchesSearch('Umm Ali, warm bread pudding', 'umm ali')).toBe(true);
    expect(matchesSearch('Crème brûlée', 'creme')).toBe(true);
    expect(matchesSearch('شاي بالنعناع', 'قهوة')).toBe(false);
  });
  it('normalises punctuation and case', () => {
    expect(normalizeForSearch('  Foul & Tamees! ')).toBe('foul tamees');
  });
});

describe('format', () => {
  it('uses Arabic-Indic digits in Arabic by default and Western digits in English', () => {
    expect(intlLocale('ar')).toBe('ar-SA-u-ca-gregory-nu-arab');
    expect(formatMoney(4500, 'ar')).toMatch(/٤٥/);
    expect(formatMoney(4500, 'en')).toMatch(/SAR\s?45$/);
    expect(formatMoney(4550, 'en')).toMatch(/45\.50/);
  });
  it('always uses the Gregorian calendar (engines differ on the ar-SA default) and offers Hijri explicitly', () => {
    const d = new Date('2026-10-06T09:00:00Z');
    expect(formatDate(d, 'ar', 'Asia/Riyadh', { day: 'numeric', month: 'long' })).toBe('٦ أكتوبر');
    expect(formatHijri(d, 'ar', 'Asia/Riyadh')).toMatch(/ربيع/);
  });

  it('formats wall times and calendar dates without zone drift', () => {
    expect(formatWallTime('19:30', 'en').toLowerCase()).toMatch(/7:30\s?pm/);
    expect(formatWallTime('19:30', 'ar')).toMatch(/٧:٣٠/);
    expect(formatDateString('2026-10-02', 'en', { weekday: 'long' })).toBe('Friday');
    expect(formatWeekday(5, 'ar')).toBe('الجمعة');
  });
});

describe('sign-in redirects', () => {
  it('keeps only paths on this site, without the locale prefix', async () => {
    const { safeNext } = await import('@/lib/account/next');
    expect(safeNext('/order/checkout')).toBe('/order/checkout');
    expect(safeNext('/en/menu/dish/hummus')).toBe('/menu/dish/hummus');
    expect(safeNext('/ar')).toBe('/');
    expect(safeNext('https://evil.test/x')).toBe('/account');
    expect(safeNext('//evil.test')).toBe('/account');
    expect(safeNext('/\\evil.test')).toBe('/account');
    expect(safeNext('/api/auth/sign-out')).toBe('/account');
    expect(safeNext('/account/sign-in?next=/x')).toBe('/account');
    expect(safeNext(undefined, '/menu')).toBe('/menu');
    expect(safeNext(['/reserve', '/x'])).toBe('/reserve');
  });

  it('maps auth errors to messages', async () => {
    const { authErrorKey } = await import('@/lib/account/next');
    expect(authErrorKey({ code: 'INVALID_EMAIL_OR_PASSWORD' })).toBe('invalidCredentials');
    expect(authErrorKey({ code: 'USER_ALREADY_EXISTS_USE_ANOTHER_EMAIL' })).toBe('userExists');
    expect(authErrorKey({ status: 429 })).toBe('rateLimited');
    expect(authErrorKey(null)).toBe('unknown');
  });
});
