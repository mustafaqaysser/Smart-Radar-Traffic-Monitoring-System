/** Promotion rules: percentage or fixed, minimum order, date range, usage limits, first order, branch, channel. */

export interface PromotionRule {
  code: string;
  kind: 'percent' | 'fixed';
  value: number;
  maxDiscount: number | null;
  minOrder: number;
  startsAt: Date | null;
  endsAt: Date | null;
  usageLimit: number | null;
  perCustomerLimit: number | null;
  usedCount: number;
  firstOrderOnly: boolean;
  branchIds: string[] | null;
  channels: string[] | null;
  isActive: boolean;
}

export interface PromotionContext {
  now: Date;
  subtotal: number;
  branchId: string;
  channel: string;
  isFirstOrder: boolean;
  customerUses: number;
}

export type PromotionFailure = 'inactive' | 'not_started' | 'expired' | 'min_order' | 'branch' | 'channel' | 'usage_limit' | 'customer_limit' | 'first_order';

export type PromotionResult = { ok: true; discount: number } | { ok: false; reason: PromotionFailure; minOrder?: number };

export function evaluatePromotion(rule: PromotionRule, ctx: PromotionContext): PromotionResult {
  if (!rule.isActive) return { ok: false, reason: 'inactive' };
  if (rule.startsAt && ctx.now < rule.startsAt) return { ok: false, reason: 'not_started' };
  if (rule.endsAt && ctx.now > rule.endsAt) return { ok: false, reason: 'expired' };
  if (rule.branchIds && rule.branchIds.length && !rule.branchIds.includes(ctx.branchId)) return { ok: false, reason: 'branch' };
  if (rule.channels && rule.channels.length && !rule.channels.includes(ctx.channel)) return { ok: false, reason: 'channel' };
  if (rule.usageLimit !== null && rule.usedCount >= rule.usageLimit) return { ok: false, reason: 'usage_limit' };
  if (rule.perCustomerLimit !== null && ctx.customerUses >= rule.perCustomerLimit) return { ok: false, reason: 'customer_limit' };
  if (rule.firstOrderOnly && !ctx.isFirstOrder) return { ok: false, reason: 'first_order' };
  if (ctx.subtotal < rule.minOrder) return { ok: false, reason: 'min_order', minOrder: rule.minOrder };

  let discount = rule.kind === 'percent' ? Math.floor((ctx.subtotal * rule.value) / 100) : rule.value;
  if (rule.maxDiscount !== null) discount = Math.min(discount, rule.maxDiscount);
  return { ok: true, discount: Math.max(0, Math.min(discount, ctx.subtotal)) };
}

export function normalizePromoCode(code: string): string {
  return code.trim().toUpperCase().replace(/\s+/g, '');
}
