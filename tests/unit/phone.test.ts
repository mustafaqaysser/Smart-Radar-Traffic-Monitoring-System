import { describe, expect, it } from 'vitest';
import { formatPhone, normalizePhone } from '@/lib/services/phone';

describe('phone numbers', () => {
  it('reads local Saudi mobiles, international forms and Arabic-Indic digits', () => {
    expect(normalizePhone('0505550111')).toBe('+966505550111');
    expect(normalizePhone('+966 50 555 0111')).toBe('+966505550111');
    expect(normalizePhone('٠٥٠٥٥٥٠١١١')).toBe('+966505550111');
    expect(normalizePhone('00966505550111')).toBe('+966505550111');
  });

  it('accepts foreign numbers with a country code and rejects nonsense', () => {
    expect(normalizePhone('+971 50 123 4567')).toBe('+971501234567');
    expect(normalizePhone('12345')).toBeNull();
    expect(normalizePhone('call me')).toBeNull();
  });

  it('formats for display', () => {
    expect(formatPhone('+966505550111')).toBe('+966 50 555 0111');
  });
});
