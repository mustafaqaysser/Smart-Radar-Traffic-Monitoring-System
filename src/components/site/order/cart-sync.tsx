'use client';

import { useEffect, useState } from 'react';
import { loadCart, saveCart } from '@/lib/actions/orders';
import { cart, useCart } from '@/lib/cart/store';
import type { CartLine } from '@/lib/cart/types';

/**
 * Signed-in guests keep their basket on the server, so it follows them between devices: on arrival the saved
 * basket is merged into this browser's, then every change is saved (debounced). Guests' baskets stay local.
 */
export function CartSync({ signedIn }: { signedIn: boolean }) {
  const c = useCart();
  const [ready, setReady] = useState(false);

  useEffect(() => {
    if (!signedIn) return undefined;
    let cancelled = false;
    void loadCart().then((res) => {
      if (cancelled) return;
      if (res.ok && res.data && res.data.lines.length) {
        const local = cart.get();
        const saved = res.data;
        if (!local.lines.length) cart.replace(saved.lines, saved.branch ?? local.branchSlug ?? '', saved.channel);
        else if (!saved.branch || !local.branchSlug || saved.branch === local.branchSlug) {
          // Same house: keep every dish from both baskets (the larger quantity wins for the same line).
          const merged = new Map<string, CartLine>(local.lines.map((l) => [l.key, l]));
          for (const l of saved.lines) {
            const mine = merged.get(l.key);
            merged.set(l.key, mine ? { ...mine, qty: Math.max(mine.qty, l.qty) } : l);
          }
          cart.replace([...merged.values()], local.branchSlug ?? saved.branch ?? '', local.channel);
        }
      }
      setReady(true);
    });
    return () => {
      cancelled = true;
    };
  }, [signedIn]);

  useEffect(() => {
    if (!signedIn || !ready) return undefined;
    const timer = window.setTimeout(() => {
      void saveCart({ branch: c.branchSlug, channel: c.channel, lines: c.lines.map((l) => ({ key: l.key, slug: l.slug, qty: l.qty, optionIds: l.optionIds, note: l.note })) });
    }, 800);
    return () => window.clearTimeout(timer);
  }, [signedIn, ready, c]);

  return null;
}
