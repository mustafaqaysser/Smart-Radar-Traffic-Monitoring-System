/** Gift card rules: status/expiry checks and partial redemption. */

export interface GiftCardState {
  balance: number;
  status: 'pending_payment' | 'scheduled' | 'active' | 'redeemed' | 'void' | 'expired';
  expiresAt: Date | null;
}

export type GiftCardFailure = 'not_found' | 'not_active' | 'expired' | 'empty' | 'void';

export function checkGiftCard(card: GiftCardState | null, now: Date): { ok: true } | { ok: false; reason: GiftCardFailure } {
  if (!card) return { ok: false, reason: 'not_found' };
  if (card.status === 'void') return { ok: false, reason: 'void' };
  if (card.status === 'expired' || (card.expiresAt && now > card.expiresAt)) return { ok: false, reason: 'expired' };
  // A scheduled card is already paid for; it only waits for its delivery email, so it can be used.
  if (card.status !== 'active' && card.status !== 'scheduled' && card.status !== 'redeemed') return { ok: false, reason: 'not_active' };
  if (card.balance <= 0) return { ok: false, reason: 'empty' };
  return { ok: true };
}

/** Applies up to `amount` from the card; returns what was applied and what remains. */
export function redeemGiftCard(balance: number, amount: number): { applied: number; remaining: number } {
  const applied = Math.max(0, Math.min(balance, Math.round(amount)));
  return { applied, remaining: balance - applied };
}

/** Formats a raw code the way it is printed: ZILL-XXXX-XXXX-XXXX. */
export function normalizeGiftCardCode(input: string): string {
  const raw = input.toUpperCase().replace(/[^A-Z0-9]/g, '');
  const body = raw.startsWith('ZILL') ? raw.slice(4) : raw;
  const groups = body.match(/.{1,4}/g) ?? [];
  return ['ZILL', ...groups].join('-');
}
