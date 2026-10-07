'use client';

import Image from 'next/image';
import { useEffect, useRef, useState } from 'react';
import type { ImageView } from '@/lib/menu/view';

/**
 * Desktop only: hovering a dish opens its photograph in a tall arched window that trails the cursor and casts
 * the hour's shadow. Transform-only, eased with a lerp; disabled for touch and reduced motion.
 */
export function useTrailingWindow() {
  const [image, setImage] = useState<ImageView | null>(null);
  const [enabled, setEnabled] = useState(false);
  useEffect(() => {
    const media = window.matchMedia('(hover: hover) and (pointer: fine) and (min-width: 64rem) and (prefers-reduced-motion: no-preference)');
    const update = () => setEnabled(media.matches);
    update();
    media.addEventListener('change', update);
    return () => media.removeEventListener('change', update);
  }, []);
  return { image: enabled ? image : null, enabled, show: (img: ImageView | null) => enabled && setImage(img), hide: () => setImage(null) };
}

export function TrailingWindow({ image }: { image: ImageView | null }) {
  const ref = useRef<HTMLDivElement>(null);
  const target = useRef({ x: 0, y: 0 });
  const pos = useRef({ x: 0, y: 0 });
  const visible = Boolean(image);

  useEffect(() => {
    if (!visible) return;
    let frame = 0;
    const onMove = (e: PointerEvent) => {
      target.current = { x: e.clientX, y: e.clientY };
    };
    // The window sits after the cursor in reading order: to its right in English, to its left in Arabic.
    const rtl = document.documentElement.dir === 'rtl';
    const tick = () => {
      pos.current.x += (target.current.x - pos.current.x) * 0.16;
      pos.current.y += (target.current.y - pos.current.y) * 0.16;
      const el = ref.current;
      if (el) el.style.transform = `translate3d(${rtl ? pos.current.x - 28 - el.offsetWidth : pos.current.x + 28}px, ${pos.current.y - 140}px, 0)`;
      frame = requestAnimationFrame(tick);
    };
    window.addEventListener('pointermove', onMove);
    frame = requestAnimationFrame(tick);
    return () => {
      window.removeEventListener('pointermove', onMove);
      cancelAnimationFrame(frame);
    };
  }, [visible]);

  return (
    <div ref={ref} aria-hidden="true" className="trailing-window pointer-events-none fixed top-0 z-30 w-56" data-visible={visible || undefined} style={{ left: 0 /* follows physical pointer coordinates */ }}>
      {image ? (
        <div className="arch-2x3 relative aspect-[2/3] w-full overflow-hidden bg-surface cast-shade">
          <Image key={image.src} src={image.src} alt="" fill sizes="224px" quality={70} placeholder={image.blur ? 'blur' : 'empty'} blurDataURL={image.blur ?? undefined} className="object-cover" style={{ objectPosition: `${image.focalX * 100}% ${image.focalY * 100}%` }} />
        </div>
      ) : null}
    </div>
  );
}
