export type OrderChannel = 'delivery' | 'pickup' | 'dine_in';

export interface CartLine {
  /** Stable key: item slug + sorted option ids + note. */
  key: string;
  slug: string;
  qty: number;
  optionIds: string[];
  note: string;
}

export interface CartState {
  version: 1;
  branchSlug: string | null;
  channel: OrderChannel;
  /** Set when ordering from a table (dine-in QR). */
  tableCode: string | null;
  lines: CartLine[];
  updatedAt: number;
}

export const EMPTY_CART: CartState = { version: 1, branchSlug: null, channel: 'pickup', tableCode: null, lines: [], updatedAt: 0 };

export function lineKey(slug: string, optionIds: string[], note: string): string {
  return [slug, [...optionIds].sort().join('.'), note.trim().toLowerCase()].join('|');
}

export const MAX_LINE_QTY = 20;
