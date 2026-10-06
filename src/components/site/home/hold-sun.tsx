'use client';

import { useEffect } from 'react';

/**
 * Desktop only: while the pointer moves over the hero, it becomes the sun — the headline's shade falls away
 * from it. On leave the shade eases back to the real sun. Transform-only; off under reduced motion.
 */
export function HoldSun({ targetId }: { targetId: string }) {
  useEffect(() => {
    const section = document.getElementById(targetId);
    if (!section) return;
    const fine = window.matchMedia('(hover: hover) and (pointer: fine)');
    const reduced = window.matchMedia('(prefers-reduced-motion: reduce)');
    if (!fine.matches || reduced.matches) return;
    let frame = 0;
    let pending: PointerEvent | null = null;
    const apply = () => {
      frame = 0;
      const e = pending;
      if (!e) return;
      const title = section.querySelector<HTMLElement>('[data-hero-title]');
      const box = (title ?? section).getBoundingClientRect();
      const cx = box.left + box.width / 2;
      const cy = box.top + box.height / 2;
      const dx = cx - e.clientX;
      const dy = Math.max(cy - e.clientY, -box.height * 0.2) + box.height * 0.35;
      const n = Math.hypot(dx, dy) || 1;
      const len = Math.min(1.9, 0.6 + Math.hypot(e.clientX - cx, e.clientY - cy) / Math.max(box.width, 1));
      section.style.setProperty('--sx', (dx / n).toFixed(3));
      section.style.setProperty('--sy', (dy / n).toFixed(3));
      section.style.setProperty('--slen', len.toFixed(3));
    };
    const onMove = (e: PointerEvent) => {
      if (e.pointerType !== 'mouse') return;
      pending = e;
      if (!frame) frame = requestAnimationFrame(apply);
    };
    const onLeave = () => {
      pending = null;
      section.style.removeProperty('--sx');
      section.style.removeProperty('--sy');
      section.style.removeProperty('--slen');
    };
    section.addEventListener('pointermove', onMove);
    section.addEventListener('pointerleave', onLeave);
    return () => {
      cancelAnimationFrame(frame);
      section.removeEventListener('pointermove', onMove);
      section.removeEventListener('pointerleave', onLeave);
    };
  }, [targetId]);
  return null;
}
