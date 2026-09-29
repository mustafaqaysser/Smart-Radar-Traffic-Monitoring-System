import { describe, expect, it } from 'vitest';
import { lineTotal, priceCart, validateModifiers, type ModifierGroupDef, type PricedLineInput } from '@/lib/domain/pricing';
import { evaluatePromotion, normalizePromoCode, type PromotionRule } from '@/lib/domain/promotions';
import { checkGiftCard, normalizeGiftCardCode, redeemGiftCard } from '@/lib/domain/gift-cards';
import { maxRedeemablePoints, nextTier, pointsEarned, redemptionValue, tierFor, type LoyaltyRules } from '@/lib/domain/loyalty';
import { countInWindow, earliestAsap, scheduleSlots, windowStart } from '@/lib/domain/throttle';

const SIDE: ModifierGroupDef = {
  id: 'side',
  minSelect: 1,
  maxSelect: 1,
  options: [
    { id: 'rice', priceDelta: 0, isAvailable: true },
    { id: 'bread', priceDelta: 500, isAvailable: true },
    { id: 'fries', priceDelta: 300, isAvailable: false },
  ],
};
const EXTRAS: ModifierGroupDef = {
  id: 'extras',
  minSelect: 0,
  maxSelect: 2,
  options: [
    { id: 'ghee', priceDelta: 400, isAvailable: true },
    { id: 'nuts', priceDelta: 600, isAvailable: true },
    { id: 'chili', priceDelta: 0, isAvailable: true },
  ],
};

const line = (key: string, unitPrice: number, quantity: number, optionIds: string[] = [], groups: ModifierGroupDef[] = []): PricedLineInput => ({
  key,
  unitPrice,
  quantity,
  optionIds,
  groups,
});

const VAT_INCLUDED = { taxRate: 0.15, pricesIncludeTax: true, serviceChargeRate: 0, serviceChargeApplies: false };

describe('modifiers', () => {
  it('requires required groups and caps optional groups', () => {
    expect(validateModifiers([SIDE, EXTRAS], [])).toEqual([{ code: 'required', groupId: 'side', min: 1 }]);
    expect(validateModifiers([SIDE, EXTRAS], ['rice', 'ghee', 'nuts', 'chili'])).toEqual([{ code: 'too_many', groupId: 'extras', max: 2 }]);
    expect(validateModifiers([SIDE], ['rice', 'bread'])).toEqual([{ code: 'too_many', groupId: 'side', max: 1 }]);
    expect(validateModifiers([SIDE], ['fries'])).toEqual([{ code: 'unavailable_option', optionId: 'fries' }]);
    expect(validateModifiers([SIDE], ['caviar'])).toContainEqual({ code: 'unknown_option', optionId: 'caviar' });
    expect(validateModifiers([SIDE, EXTRAS], ['bread', 'ghee'])).toEqual([]);
  });

  it('adds option price deltas to each unit', () => {
    expect(lineTotal(line('a', 13800, 2, ['bread', 'ghee', 'nuts'], [SIDE, EXTRAS]))).toBe((13800 + 500 + 400 + 600) * 2);
  });
});

describe('pricing', () => {
  it('reports VAT included in prices (15% of the gross)', () => {
    const r = priceCart({ lines: [line('a', 11500, 1)], config: VAT_INCLUDED });
    expect(r.subtotal).toBe(11500);
    expect(r.tax).toBe(1500);
    expect(r.taxIncluded).toBe(true);
    expect(r.total).toBe(11500);
  });

  it('adds VAT on top for tax-exclusive markets', () => {
    const r = priceCart({ lines: [line('a', 10000, 1)], config: { ...VAT_INCLUDED, pricesIncludeTax: false } });
    expect(r.tax).toBe(1500);
    expect(r.total).toBe(11500);
  });

  it('applies delivery fee, zone minimum, promo and loyalty discounts in order', () => {
    const r = priceCart({
      lines: [line('a', 8400, 1), line('b', 3400, 2)],
      config: VAT_INCLUDED,
      zone: { fee: 1500, minOrder: 10000 },
      promoDiscount: 2280,
      loyaltyDiscount: 1000,
    });
    expect(r.subtotal).toBe(15200);
    expect(r.discount).toBe(2280);
    expect(r.loyaltyDiscount).toBe(1000);
    expect(r.deliveryFee).toBe(1500);
    expect(r.belowMinimum).toBeNull();
    expect(r.total).toBe(15200 - 2280 - 1000 + 1500);
    expect(r.tax).toBe(Math.round((13420 * 0.15) / 1.15));
  });

  it('flags orders below the zone minimum', () => {
    const r = priceCart({ lines: [line('a', 4200, 1)], config: VAT_INCLUDED, zone: { fee: 1500, minOrder: 6000 } });
    expect(r.belowMinimum).toEqual({ minOrder: 6000, shortBy: 1800 });
  });

  it('charges service on the discounted subtotal and never taxes or discounts the tip', () => {
    const r = priceCart({
      lines: [line('a', 10000, 1)],
      config: { ...VAT_INCLUDED, serviceChargeRate: 0.1, serviceChargeApplies: true },
      promoDiscount: 2000,
      tip: { kind: 'percent', value: 0.1 },
    });
    expect(r.serviceCharge).toBe(800);
    expect(r.tip).toBe(1000);
    expect(r.total).toBe(10000 - 2000 + 800 + 1000);
  });

  it('applies a gift card last, partially or fully', () => {
    const partial = priceCart({ lines: [line('a', 20000, 1)], config: VAT_INCLUDED, giftCardBalance: 5000 });
    expect(partial.giftCardAmount).toBe(5000);
    expect(partial.amountDue).toBe(15000);
    const full = priceCart({ lines: [line('a', 20000, 1)], config: VAT_INCLUDED, giftCardBalance: 50000 });
    expect(full.giftCardAmount).toBe(20000);
    expect(full.amountDue).toBe(0);
  });

  it('never discounts below zero', () => {
    const r = priceCart({ lines: [line('a', 1000, 1)], config: VAT_INCLUDED, promoDiscount: 5000, loyaltyDiscount: 5000 });
    expect(r.discount).toBe(1000);
    expect(r.loyaltyDiscount).toBe(0);
    expect(r.total).toBe(0);
  });
});

describe('promotions', () => {
  const base: PromotionRule = {
    code: 'SHADE15',
    kind: 'percent',
    value: 15,
    maxDiscount: 5000,
    minOrder: 10000,
    startsAt: new Date('2026-09-01T00:00:00Z'),
    endsAt: new Date('2026-12-31T00:00:00Z'),
    usageLimit: 100,
    perCustomerLimit: 1,
    usedCount: 3,
    firstOrderOnly: false,
    branchIds: null,
    channels: null,
    isActive: true,
  };
  const ctx = { now: new Date('2026-10-01T12:00:00Z'), subtotal: 20000, branchId: 'b1', channel: 'delivery', isFirstOrder: false, customerUses: 0 };

  it('computes percentage discounts with a cap', () => {
    expect(evaluatePromotion(base, ctx)).toEqual({ ok: true, discount: 3000 });
    expect(evaluatePromotion(base, { ...ctx, subtotal: 60000 })).toEqual({ ok: true, discount: 5000 });
  });
  it('computes fixed discounts, never above the subtotal', () => {
    expect(evaluatePromotion({ ...base, kind: 'fixed', value: 2500, maxDiscount: null, minOrder: 0 }, { ...ctx, subtotal: 2000 })).toEqual({ ok: true, discount: 2000 });
  });
  it('rejects for every rule', () => {
    expect(evaluatePromotion({ ...base, isActive: false }, ctx)).toMatchObject({ reason: 'inactive' });
    expect(evaluatePromotion(base, { ...ctx, now: new Date('2026-08-01T00:00:00Z') })).toMatchObject({ reason: 'not_started' });
    expect(evaluatePromotion(base, { ...ctx, now: new Date('2027-01-02T00:00:00Z') })).toMatchObject({ reason: 'expired' });
    expect(evaluatePromotion(base, { ...ctx, subtotal: 9000 })).toMatchObject({ reason: 'min_order', minOrder: 10000 });
    expect(evaluatePromotion({ ...base, branchIds: ['b2'] }, ctx)).toMatchObject({ reason: 'branch' });
    expect(evaluatePromotion({ ...base, channels: ['pickup'] }, ctx)).toMatchObject({ reason: 'channel' });
    expect(evaluatePromotion({ ...base, usedCount: 100 }, ctx)).toMatchObject({ reason: 'usage_limit' });
    expect(evaluatePromotion(base, { ...ctx, customerUses: 1 })).toMatchObject({ reason: 'customer_limit' });
    expect(evaluatePromotion({ ...base, firstOrderOnly: true }, ctx)).toMatchObject({ reason: 'first_order' });
  });
  it('normalises codes', () => {
    expect(normalizePromoCode(' shade 15 ')).toBe('SHADE15');
  });
});

describe('gift cards', () => {
  const now = new Date('2026-10-01T00:00:00Z');
  it('checks status, expiry and balance', () => {
    expect(checkGiftCard(null, now)).toEqual({ ok: false, reason: 'not_found' });
    expect(checkGiftCard({ balance: 100, status: 'void', expiresAt: null }, now)).toEqual({ ok: false, reason: 'void' });
    expect(checkGiftCard({ balance: 100, status: 'active', expiresAt: new Date('2026-01-01T00:00:00Z') }, now)).toEqual({ ok: false, reason: 'expired' });
    expect(checkGiftCard({ balance: 0, status: 'active', expiresAt: null }, now)).toEqual({ ok: false, reason: 'empty' });
    expect(checkGiftCard({ balance: 100, status: 'pending_payment', expiresAt: null }, now)).toEqual({ ok: false, reason: 'not_active' });
    expect(checkGiftCard({ balance: 100, status: 'active', expiresAt: null }, now)).toEqual({ ok: true });
  });
  it('redeems partially', () => {
    expect(redeemGiftCard(20000, 7550)).toEqual({ applied: 7550, remaining: 12450 });
    expect(redeemGiftCard(5000, 7550)).toEqual({ applied: 5000, remaining: 0 });
  });
  it('normalises codes typed with any spacing', () => {
    expect(normalizeGiftCardCode('zill 4h7q m2pk 9xra')).toBe('ZILL-4H7Q-M2PK-9XRA');
  });
});

describe('loyalty', () => {
  const rules: LoyaltyRules = {
    pointsPerUnit: 1,
    pointsPerUnitRedeemed: 20,
    maxRedeemShare: 0.5,
    tiers: [
      { id: 'morning', minPoints: 0, multiplier: 1 },
      { id: 'long-shade', minPoints: 1500, multiplier: 1.25 },
      { id: 'night', minPoints: 5000, multiplier: 1.5 },
    ],
  };
  it('finds tiers and the next tier', () => {
    expect(tierFor(0, rules.tiers).id).toBe('morning');
    expect(tierFor(1500, rules.tiers).id).toBe('long-shade');
    expect(tierFor(9000, rules.tiers).id).toBe('night');
    expect(nextTier(1200, rules.tiers)).toMatchObject({ tier: { id: 'long-shade' }, pointsToGo: 300 });
    expect(nextTier(6000, rules.tiers)).toBeNull();
  });
  it('earns points on money paid with the tier multiplier', () => {
    expect(pointsEarned(15000, 0, rules)).toBe(150);
    expect(pointsEarned(15000, 2000, rules)).toBe(187);
    expect(pointsEarned(15000, 6000, rules)).toBe(225);
  });
  it('values and caps redemptions in whole-currency steps', () => {
    expect(redemptionValue(200, rules)).toBe(1000);
    expect(maxRedeemablePoints(10000, 20000, rules)).toBe(2000); // 50% of SAR 200 = SAR 100 = 2000 pts
    expect(maxRedeemablePoints(330, 20000, rules)).toBe(320);
  });
});

describe('kitchen throttling', () => {
  const rules = { maxOrdersPerWindow: 2, windowMinutes: 15, basePrepMinutes: 20, busyExtraMinutes: 15 };
  const intervals = [{ start: new Date('2026-10-02T09:00:00Z'), end: new Date('2026-10-02T13:00:00Z') }];

  it('aligns windows and counts promises in a window', () => {
    expect(windowStart(new Date('2026-10-02T10:07:00Z'), 15).toISOString()).toBe('2026-10-02T10:00:00.000Z');
    expect(countInWindow([new Date('2026-10-02T10:00:00Z'), new Date('2026-10-02T10:14:59Z'), new Date('2026-10-02T10:15:00Z')], new Date('2026-10-02T10:00:00Z'), 15)).toBe(2);
  });

  it('promises the first window with room after prep time', () => {
    const now = new Date('2026-10-02T10:00:00Z');
    expect(earliestAsap(now, 20, rules, [], intervals)?.toISOString()).toBe('2026-10-02T10:20:00.000Z');
    const full = [new Date('2026-10-02T10:16:00Z'), new Date('2026-10-02T10:20:00Z')];
    expect(earliestAsap(now, 20, rules, full, intervals)?.toISOString()).toBe('2026-10-02T10:30:00.000Z');
  });

  it('respects ordering hours and returns null after closing', () => {
    expect(earliestAsap(new Date('2026-10-02T12:50:00Z'), 20, rules, [], intervals)).toBeNull();
    const before = earliestAsap(new Date('2026-10-02T07:00:00Z'), 20, rules, [], intervals);
    expect(before?.toISOString()).toBe('2026-10-02T09:20:00.000Z');
  });

  it('lists scheduled slots with capacity', () => {
    const slots = scheduleSlots(new Date('2026-10-02T11:30:00Z'), 20, rules, [new Date('2026-10-02T12:00:00Z'), new Date('2026-10-02T12:05:00Z')], intervals, 15);
    expect(slots[0]?.at.toISOString()).toBe('2026-10-02T12:00:00.000Z');
    expect(slots[0]?.available).toBe(false);
    expect(slots[1]?.available).toBe(true);
    expect(slots.at(-1)?.at.toISOString()).toBe('2026-10-02T13:00:00.000Z');
  });
});
