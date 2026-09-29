/** Loyalty: points per spend with tier multipliers, redemption value and caps. */

export interface Tier {
  id: string;
  minPoints: number;
  multiplier: number;
}

export interface LoyaltyRules {
  pointsPerUnit: number;
  pointsPerUnitRedeemed: number;
  maxRedeemShare: number;
  tiers: Tier[];
}

export function tierFor(lifetimePoints: number, tiers: Tier[]): Tier {
  const sorted = [...tiers].sort((a, b) => a.minPoints - b.minPoints);
  let current = sorted[0] as Tier;
  for (const t of sorted) if (lifetimePoints >= t.minPoints) current = t;
  return current;
}

export function nextTier(lifetimePoints: number, tiers: Tier[]): { tier: Tier; pointsToGo: number } | null {
  const next = [...tiers].sort((a, b) => a.minPoints - b.minPoints).find((t) => t.minPoints > lifetimePoints);
  return next ? { tier: next, pointsToGo: next.minPoints - lifetimePoints } : null;
}

/** Points earned on an amount actually paid (minor units), after the tier multiplier. */
export function pointsEarned(paidMinor: number, lifetimePoints: number, rules: LoyaltyRules): number {
  const tier = tierFor(lifetimePoints, rules.tiers);
  return Math.floor((Math.max(0, paidMinor) / 100) * rules.pointsPerUnit * tier.multiplier);
}

/** Value in minor units of redeeming `points`. */
export function redemptionValue(points: number, rules: LoyaltyRules): number {
  return Math.floor((Math.max(0, points) / rules.pointsPerUnitRedeemed) * 100);
}

/** Largest number of points usable on an order, given the balance and the redeem cap. */
export function maxRedeemablePoints(balance: number, discountableMinor: number, rules: LoyaltyRules): number {
  const capMinor = Math.floor(discountableMinor * rules.maxRedeemShare);
  const capPoints = Math.floor((capMinor / 100) * rules.pointsPerUnitRedeemed);
  // Redeem in whole-currency steps so the discount is a round number.
  const step = rules.pointsPerUnitRedeemed;
  return Math.floor(Math.min(balance, capPoints) / step) * step;
}
