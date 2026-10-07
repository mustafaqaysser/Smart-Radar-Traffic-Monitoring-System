'use client';

import { lineUnitPrice } from '@/lib/domain/pricing';
import type { CartLine } from '@/lib/cart/types';
import type { ItemView } from '@/lib/menu/view';

export interface BasketLine {
  line: CartLine;
  item: ItemView | null;
  unitPrice: number;
  total: number;
  options: string[];
}

/** Basket lines with names and prices from the menu shown (the server prices them again at checkout). */
export function basketLines(lines: CartLine[], items: Record<string, ItemView>): BasketLine[] {
  return lines.map((line) => {
    const item = items[line.slug] ?? null;
    if (!item) return { line, item: null, unitPrice: 0, total: 0, options: [] };
    const groups = item.modifierGroups.map((g) => ({ id: g.id, minSelect: g.minSelect, maxSelect: g.maxSelect, options: g.options.map((o) => ({ id: o.id, priceDelta: o.priceDelta, isAvailable: o.isAvailable })) }));
    const unitPrice = lineUnitPrice({ unitPrice: item.price, optionIds: line.optionIds, groups });
    const options = line.optionIds.flatMap((id) => item.modifierGroups.flatMap((g) => g.options.filter((o) => o.id === id).map((o) => o.name)));
    return { line, item, unitPrice, total: unitPrice * line.qty, options };
  });
}
