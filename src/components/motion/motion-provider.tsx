'use client';

import { createContext, useContext, useEffect, useMemo, useRef, useState, type ReactNode } from 'react';
import type Lenis from 'lenis';
import { isFinePointerDesktop, prefersReducedMotion } from '@/lib/motion/tokens';

interface MotionContextValue {
  reduced: boolean;
  /** Smooth-scrolls to a target (element, selector or offset) — native scroll when Lenis is off. */
  scrollTo: (target: string | HTMLElement | number, options?: { offset?: number; immediate?: boolean }) => void;
  /** Locks page scrolling while an overlay is open (reference-counted). */
  lockScroll: (locked: boolean) => void;
}

const MotionContext = createContext<MotionContextValue>({ reduced: false, scrollTo: () => undefined, lockScroll: () => undefined });

export function useMotion(): MotionContextValue {
  return useContext(MotionContext);
}

/**
 * One place for motion:
 *  - Lenis smooth scrolling on desktop fine pointers only, never under reduced motion, synced to GSAP's ticker
 *    so ScrollTrigger chapters scrub in step with it.
 *  - A single IntersectionObserver that reveals `[data-reveal]` elements (CSS does the animation).
 */
export function MotionProvider({ children }: { children: ReactNode }) {
  const [reduced, setReduced] = useState(false);
  const lenisRef = useRef<Lenis | null>(null);
  const locks = useRef(0);

  useEffect(() => {
    const media = window.matchMedia('(prefers-reduced-motion: reduce)');
    const update = () => setReduced(media.matches);
    update();
    media.addEventListener('change', update);
    return () => media.removeEventListener('change', update);
  }, []);

  // Smooth scroll (desktop only).
  useEffect(() => {
    if (reduced || !isFinePointerDesktop()) return;
    let cancelled = false;
    let cleanup: (() => void) | undefined;
    void (async () => {
      const [{ default: LenisCtor }, { gsap }, { ScrollTrigger }] = await Promise.all([import('lenis'), import('gsap'), import('gsap/ScrollTrigger')]);
      if (cancelled) return;
      gsap.registerPlugin(ScrollTrigger);
      const lenis = new LenisCtor({ lerp: 0.11, wheelMultiplier: 0.9, smoothWheel: true, anchors: false, prevent: (node) => node.closest('[data-lenis-prevent], dialog') !== null });
      lenisRef.current = lenis;
      lenis.on('scroll', ScrollTrigger.update);
      const tick = (time: number) => lenis.raf(time * 1000);
      gsap.ticker.add(tick);
      gsap.ticker.lagSmoothing(0);
      document.documentElement.classList.add('lenis');
      cleanup = () => {
        gsap.ticker.remove(tick);
        lenis.destroy();
        lenisRef.current = null;
        document.documentElement.classList.remove('lenis');
      };
    })();
    return () => {
      cancelled = true;
      cleanup?.();
    };
  }, [reduced]);

  // Reveal observer.
  useEffect(() => {
    const root = document.documentElement;
    if (reduced || !('IntersectionObserver' in window)) {
      root.classList.add('reveal-all');
      return;
    }
    root.classList.remove('reveal-all');
    const io = new IntersectionObserver(
      (entries) => {
        for (const entry of entries) {
          if (entry.isIntersecting) {
            (entry.target as HTMLElement).dataset.revealed = 'true';
            io.unobserve(entry.target);
          }
        }
      },
      { rootMargin: '0px 0px -12% 0px', threshold: 0.08 },
    );
    const observeAll = () => document.querySelectorAll<HTMLElement>('[data-reveal]:not([data-revealed])').forEach((el) => io.observe(el));
    observeAll();
    // Pick up content rendered after navigation or lazily.
    const mo = new MutationObserver(observeAll);
    mo.observe(document.body, { childList: true, subtree: true });
    return () => {
      io.disconnect();
      mo.disconnect();
    };
  }, [reduced]);

  const value = useMemo<MotionContextValue>(() => ({
    reduced,
    lockScroll: (locked) => {
      locks.current = Math.max(0, locks.current + (locked ? 1 : -1));
      const on = locks.current > 0;
      document.documentElement.toggleAttribute('data-scroll-locked', on);
      if (on) lenisRef.current?.stop();
      else lenisRef.current?.start();
    },
    scrollTo: (target, options = {}) => {
      const lenis = lenisRef.current;
      if (lenis) {
        lenis.scrollTo(target, { offset: options.offset ?? 0, immediate: options.immediate ?? false, duration: 1.2 });
        return;
      }
      const el = typeof target === 'string' ? document.querySelector<HTMLElement>(target) : typeof target === 'number' ? null : target;
      const top = typeof target === 'number' ? target : el ? el.getBoundingClientRect().top + window.scrollY + (options.offset ?? 0) : 0;
      window.scrollTo({ top, behavior: options.immediate || prefersReducedMotion() ? 'auto' : 'smooth' });
    },
  }), [reduced]);

  return <MotionContext value={value}>{children}</MotionContext>;
}
