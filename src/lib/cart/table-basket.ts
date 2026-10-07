'use client';

import { useSyncExternalStore } from 'react';
import { lineKey, MAX_LINE_QTY, type CartLine } from './types';

/**
 * The basket for one table: separate from the delivery/pickup basket and kept for the visit only
 * (sessionStorage), so a phone passed around the table keeps what was chosen until it is sent.
 */
const EMPTY: CartLine[] = [];
const listeners = new Set<() => void>();
const baskets = new Map<string, CartLine[]>();

const storageKey = (code: string) => `zill_table_${code}`;

function read(code: string): CartLine[] {
  try {
    const raw = window.sessionStorage.getItem(storageKey(code));
    const parsed = raw ? (JSON.parse(raw) as CartLine[]) : [];
    return Array.isArray(parsed) ? parsed.filter((l) => typeof l?.slug === 'string' && typeof l.qty === 'number') : [];
  } catch {
    return [];
  }
}

function get(code: string): CartLine[] {
  if (typeof window === 'undefined') return EMPTY;
  let lines = baskets.get(code);
  if (!lines) {
    lines = read(code);
    baskets.set(code, lines);
  }
  return lines;
}

function set(code: string, lines: CartLine[]) {
  baskets.set(code, lines);
  try {
    window.sessionStorage.setItem(storageKey(code), JSON.stringify(lines));
  } catch {
    // Storage disabled: the basket still works for this page view.
  }
  listeners.forEach((l) => l());
}

function subscribe(listener: () => void) {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

export function useTableBasket(code: string): CartLine[] {
  return useSyncExternalStore(
    subscribe,
    () => get(code),
    () => EMPTY,
  );
}

export const tableBasket = {
  add(code: string, line: { slug: string; qty: number; optionIds: string[]; note: string }) {
    const key = lineKey(line.slug, line.optionIds, line.note);
    const lines = get(code);
    const existing = lines.find((l) => l.key === key);
    set(
      code,
      existing
        ? lines.map((l) => (l.key === key ? { ...l, qty: Math.min(MAX_LINE_QTY, l.qty + line.qty) } : l))
        : [...lines, { key, slug: line.slug, qty: Math.min(MAX_LINE_QTY, line.qty), optionIds: [...line.optionIds].sort(), note: line.note.trim() }],
    );
  },
  setQty(code: string, key: string, qty: number) {
    const lines = get(code);
    set(code, qty <= 0 ? lines.filter((l) => l.key !== key) : lines.map((l) => (l.key === key ? { ...l, qty: Math.min(MAX_LINE_QTY, qty) } : l)));
  },
  clear(code: string) {
    set(code, []);
  },
};
