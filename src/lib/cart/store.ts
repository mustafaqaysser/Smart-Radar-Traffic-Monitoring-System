'use client';

import { useSyncExternalStore } from 'react';
import { EMPTY_CART, lineKey, MAX_LINE_QTY, type CartLine, type CartState, type OrderChannel } from './types';

const KEY = 'zill_cart_v1';
const listeners = new Set<() => void>();
let state: CartState = EMPTY_CART;
let loaded = false;

function read(): CartState {
  try {
    const raw = window.localStorage.getItem(KEY);
    if (!raw) return EMPTY_CART;
    const parsed = JSON.parse(raw) as CartState;
    if (parsed?.version !== 1 || !Array.isArray(parsed.lines)) return EMPTY_CART;
    // Carts older than three days are stale (prices, availability and hours move on).
    if (Date.now() - parsed.updatedAt > 3 * 864e5) return EMPTY_CART;
    return parsed;
  } catch {
    return EMPTY_CART;
  }
}

function ensureLoaded() {
  if (loaded || typeof window === 'undefined') return;
  loaded = true;
  state = read();
  window.addEventListener('storage', (e) => {
    if (e.key !== KEY) return;
    state = read();
    listeners.forEach((l) => l());
  });
}

function commit(next: Omit<CartState, 'updatedAt' | 'version'>) {
  state = { ...next, version: 1, updatedAt: Date.now() };
  try {
    window.localStorage.setItem(KEY, JSON.stringify(state));
  } catch {
    // Storage full or disabled: the cart still works for this page view.
  }
  listeners.forEach((l) => l());
}

function subscribe(listener: () => void) {
  ensureLoaded();
  listeners.add(listener);
  return () => listeners.delete(listener);
}

export function useCart(): CartState {
  return useSyncExternalStore(
    subscribe,
    () => {
      ensureLoaded();
      return state;
    },
    () => EMPTY_CART,
  );
}

export function useCartCount(): number {
  const cart = useCart();
  return cart.lines.reduce((n, l) => n + l.qty, 0);
}

export const cart = {
  get: (): CartState => {
    ensureLoaded();
    return state;
  },
  add(slug: string, qty: number, optionIds: string[] = [], note = '') {
    ensureLoaded();
    const key = lineKey(slug, optionIds, note);
    const existing = state.lines.find((l) => l.key === key);
    const lines: CartLine[] = existing
      ? state.lines.map((l) => (l.key === key ? { ...l, qty: Math.min(MAX_LINE_QTY, l.qty + qty) } : l))
      : [...state.lines, { key, slug, qty: Math.min(MAX_LINE_QTY, qty), optionIds: [...optionIds].sort(), note: note.trim() }];
    commit({ ...state, lines });
  },
  setQty(key: string, qty: number) {
    ensureLoaded();
    const lines = qty <= 0 ? state.lines.filter((l) => l.key !== key) : state.lines.map((l) => (l.key === key ? { ...l, qty: Math.min(MAX_LINE_QTY, qty) } : l));
    commit({ ...state, lines });
  },
  remove(key: string) {
    ensureLoaded();
    commit({ ...state, lines: state.lines.filter((l) => l.key !== key) });
  },
  clear() {
    ensureLoaded();
    commit({ ...state, lines: [], tableCode: null });
  },
  setChannel(channel: OrderChannel) {
    ensureLoaded();
    commit({ ...state, channel, tableCode: channel === 'dine_in' ? state.tableCode : null });
  },
  setBranch(branchSlug: string) {
    ensureLoaded();
    if (state.branchSlug === branchSlug) return;
    commit({ ...state, branchSlug });
  },
  setTable(branchSlug: string, tableCode: string) {
    ensureLoaded();
    commit({ ...state, branchSlug, tableCode, channel: 'dine_in' });
  },
  replace(lines: CartLine[], branchSlug: string, channel: OrderChannel) {
    ensureLoaded();
    commit({ ...state, lines, branchSlug, channel, tableCode: null });
  },
};
