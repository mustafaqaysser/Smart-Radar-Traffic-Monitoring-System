import { describe, expect, it } from 'vitest';
import { TABLE_AREAS } from '@/lib/db/schema';
import { AREA_CHOICES, isAreaChoice, OCCASIONS } from '@/lib/reserve/types';
import { linkToken, verifyLinkToken } from '@/lib/server/tokens';

describe('booking choices', () => {
  it('offers exactly the table areas plus "no preference"', () => {
    expect([...AREA_CHOICES]).toEqual(['any', ...TABLE_AREAS]);
    expect(isAreaChoice('roof')).toBe(true);
    expect(isAreaChoice('terrace')).toBe(false);
  });

  it('has a fixed list of occasions', () => {
    expect(OCCASIONS).toContain('birthday');
    expect(new Set(OCCASIONS).size).toBe(OCCASIONS.length);
  });
});

describe('guest link tokens', () => {
  it('derives the same token for the same booking every time (so every email carries a working link)', () => {
    const a = linkToken('reservation', 'abc123');
    expect(a).toHaveLength(32);
    expect(linkToken('reservation', 'abc123')).toBe(a);
    expect(a).toMatch(/^[A-Za-z0-9_-]+$/);
  });

  it('is specific to the purpose and the record', () => {
    expect(linkToken('order', 'abc123')).not.toBe(linkToken('reservation', 'abc123'));
    expect(linkToken('reservation', 'abc124')).not.toBe(linkToken('reservation', 'abc123'));
  });

  it('verifies only the exact token', () => {
    const token = linkToken('waitlist', 'w1');
    expect(verifyLinkToken('waitlist', 'w1', token)).toBe(true);
    expect(verifyLinkToken('waitlist', 'w2', token)).toBe(false);
    expect(verifyLinkToken('reservation', 'w1', token)).toBe(false);
    expect(verifyLinkToken('waitlist', 'w1', `${token.slice(0, 31)}x`)).toBe(false);
    expect(verifyLinkToken('waitlist', 'w1', null)).toBe(false);
    expect(verifyLinkToken('waitlist', 'w1', '')).toBe(false);
  });
});
