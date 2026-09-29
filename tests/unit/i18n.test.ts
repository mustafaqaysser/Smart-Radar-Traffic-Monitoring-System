import { describe, expect, it } from 'vitest';
import { matchesSearch, normalizeArabic, normalizeForSearch } from '@/lib/i18n/arabic';
import { normalizeDigits, parseLocaleNumber, parseMoneyInput } from '@/lib/i18n/digits';
import { formatDateString, formatMoney, formatWallTime, formatWeekday, intlLocale } from '@/lib/i18n/format';

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
    expect(intlLocale('ar')).toBe('ar-SA-u-nu-arab');
    expect(formatMoney(4500, 'ar')).toMatch(/٤٥/);
    expect(formatMoney(4500, 'en')).toMatch(/SAR\s?45$/);
    expect(formatMoney(4550, 'en')).toMatch(/45\.50/);
  });
  it('formats wall times and calendar dates without zone drift', () => {
    expect(formatWallTime('19:30', 'en').toLowerCase()).toMatch(/7:30\s?pm/);
    expect(formatWallTime('19:30', 'ar')).toMatch(/٧:٣٠/);
    expect(formatDateString('2026-10-02', 'en', { weekday: 'long' })).toBe('Friday');
    expect(formatWeekday(5, 'ar')).toBe('الجمعة');
  });
});
