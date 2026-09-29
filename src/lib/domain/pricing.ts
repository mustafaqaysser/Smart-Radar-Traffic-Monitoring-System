/**
 * Cart pricing. Integer minor units throughout; every rounding is explicit.
 *
 * Order of operations
 *   line = (unit price + option deltas) × quantity
 *   subtotal − promo discount − loyalty discount
 *   + delivery fee (delivery only; zone minimum checked against the subtotal)
 *   + service charge (configured channels, on the discounted subtotal)
 *   tax: included in prices (reported) or added on top (exclusive markets)
 *   + tip (never taxed, never discounted)
 *   = total; gift card balance is applied last → amount due
 */

export interface ModifierGroupDef {
  id: string;
  minSelect: number;
  maxSelect: number;
  options: { id: string; priceDelta: number; isAvailable: boolean }[];
}

export interface PricedLineInput {
  key: string;
  unitPrice: number;
  quantity: number;
  optionIds: string[];
  groups: ModifierGroupDef[];
}

export type ModifierError =
  | { code: 'required'; groupId: string; min: number }
  | { code: 'too_many'; groupId: string; max: number }
  | { code: 'unknown_option'; optionId: string }
  | { code: 'unavailable_option'; optionId: string };

/** Validates a line's option selection against its modifier groups. */
export function validateModifiers(groups: ModifierGroupDef[], optionIds: string[]): ModifierError[] {
  const errors: ModifierError[] = [];
  const known = new Map<string, { groupId: string; isAvailable: boolean }>();
  for (const g of groups) for (const o of g.options) known.set(o.id, { groupId: g.id, isAvailable: o.isAvailable });
  for (const id of optionIds) {
    const hit = known.get(id);
    if (!hit) errors.push({ code: 'unknown_option', optionId: id });
    else if (!hit.isAvailable) errors.push({ code: 'unavailable_option', optionId: id });
  }
  for (const g of groups) {
    const count = optionIds.filter((id) => known.get(id)?.groupId === g.id).length;
    if (count < g.minSelect) errors.push({ code: 'required', groupId: g.id, min: g.minSelect });
    if (count > g.maxSelect) errors.push({ code: 'too_many', groupId: g.id, max: g.maxSelect });
  }
  return errors;
}

export function lineUnitPrice(line: Pick<PricedLineInput, 'unitPrice' | 'optionIds' | 'groups'>): number {
  const deltas = new Map<string, number>();
  for (const g of line.groups) for (const o of g.options) deltas.set(o.id, o.priceDelta);
  return line.unitPrice + line.optionIds.reduce((s, id) => s + (deltas.get(id) ?? 0), 0);
}

export function lineTotal(line: PricedLineInput): number {
  return lineUnitPrice(line) * line.quantity;
}

export interface PricingConfig {
  taxRate: number;
  pricesIncludeTax: boolean;
  serviceChargeRate: number;
  serviceChargeApplies: boolean;
}

export interface PricingInput {
  lines: PricedLineInput[];
  config: PricingConfig;
  /** Delivery zone, when the channel is delivery. */
  zone?: { fee: number; minOrder: number } | null;
  /** Discount already validated by the promotions engine (minor units). */
  promoDiscount?: number;
  /** Loyalty discount already capped by the loyalty engine (minor units). */
  loyaltyDiscount?: number;
  tip?: { kind: 'percent'; value: number } | { kind: 'amount'; value: number } | null;
  /** Remaining balance of an applied gift card. */
  giftCardBalance?: number;
}

export interface PricingResult {
  subtotal: number;
  discount: number;
  loyaltyDiscount: number;
  deliveryFee: number;
  serviceCharge: number;
  tax: number;
  taxIncluded: boolean;
  tip: number;
  total: number;
  giftCardAmount: number;
  amountDue: number;
  belowMinimum: { minOrder: number; shortBy: number } | null;
  lineTotals: Record<string, number>;
}

export function roundHalfUp(value: number): number {
  return Math.sign(value) * Math.round(Math.abs(value) + Number.EPSILON);
}

export function priceCart(input: PricingInput): PricingResult {
  const lineTotals: Record<string, number> = {};
  let subtotal = 0;
  for (const line of input.lines) {
    const t = lineTotal(line);
    lineTotals[line.key] = t;
    subtotal += t;
  }
  const discount = Math.min(Math.max(0, input.promoDiscount ?? 0), subtotal);
  const loyaltyDiscount = Math.min(Math.max(0, input.loyaltyDiscount ?? 0), subtotal - discount);
  const discounted = subtotal - discount - loyaltyDiscount;

  const deliveryFee = input.zone ? input.zone.fee : 0;
  const belowMinimum = input.zone && subtotal < input.zone.minOrder ? { minOrder: input.zone.minOrder, shortBy: input.zone.minOrder - subtotal } : null;

  const serviceCharge = input.config.serviceChargeApplies ? roundHalfUp(discounted * input.config.serviceChargeRate) : 0;
  const taxable = discounted + deliveryFee + serviceCharge;
  const rate = input.config.taxRate;
  const tax = input.config.pricesIncludeTax ? roundHalfUp((taxable * rate) / (1 + rate)) : roundHalfUp(taxable * rate);

  let tip = 0;
  if (input.tip) tip = input.tip.kind === 'percent' ? roundHalfUp(subtotal * input.tip.value) : Math.max(0, Math.round(input.tip.value));

  const total = taxable + (input.config.pricesIncludeTax ? 0 : tax) + tip;
  const giftCardAmount = Math.min(Math.max(0, input.giftCardBalance ?? 0), total);
  return {
    subtotal,
    discount,
    loyaltyDiscount,
    deliveryFee,
    serviceCharge,
    tax,
    taxIncluded: input.config.pricesIncludeTax,
    tip,
    total,
    giftCardAmount,
    amountDue: total - giftCardAmount,
    belowMinimum,
    lineTotals,
  };
}
