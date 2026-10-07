'use client';

import Image from 'next/image';
import { useLocale, useTranslations } from 'next-intl';
import { useCallback, useEffect, useRef, useState, type CSSProperties, type KeyboardEvent } from 'react';
import { Icon } from '@/components/brand/icon';
import { formatNumber } from '@/lib/i18n/format';
import type { ImageView } from '@/lib/menu/view';
import { cn } from '@/lib/utils/cn';

export interface DialItem {
  id: string;
  image: ImageView;
  caption: string;
  hour: string;
}

/**
 * The dial gallery: photographs placed like hour marks on a sundial. Drag the ring to turn it (with inertia,
 * then it settles on the nearest photograph), or use the buttons and arrow keys. In Arabic the day runs
 * counter-clockwise and the arrow keys mirror. A plain grid is one click away.
 */
export function DialGallery({ items }: { items: DialItem[] }) {
  const t = useTranslations('pages.gallery');
  const locale = useLocale();
  const rtl = locale === 'ar';
  const dir = rtl ? -1 : 1;
  const n = items.length;
  const step = 360 / Math.max(1, n);
  const ring = useRef<HTMLDivElement>(null);
  const [rot, setRotState] = useState(0);
  const rotRef = useRef(0);
  const setRot = useCallback((value: number) => {
    rotRef.current = value;
    setRotState(value);
  }, []);
  const [grid, setGrid] = useState(false);
  const anim = useRef(0);
  const drag = useRef<{ startAngle: number; startRot: number; lastAngle: number; lastTime: number; velocity: number; moved: boolean } | null>(null);

  const active = ((Math.round((-rot * dir) / step) % n) + n) % n;
  const current = items[active];

  const reduced = () => window.matchMedia('(prefers-reduced-motion: reduce)').matches;

  const animateTo = useCallback((target: number) => {
    cancelAnimationFrame(anim.current);
    if (reduced()) {
      setRot(target);
      return;
    }
    const from = rotRef.current;
    const start = performance.now();
    const duration = 700;
    const tick = (now: number) => {
      const p = Math.min(1, (now - start) / duration);
      const e = 1 - Math.pow(1 - p, 4);
      setRot(from + (target - from) * e);
      if (p < 1) anim.current = requestAnimationFrame(tick);
    };
    anim.current = requestAnimationFrame(tick);
  }, [setRot]);

  useEffect(() => () => cancelAnimationFrame(anim.current), []);

  const snap = (r: number) => Math.round(r / step) * step;
  const go = (delta: number) => animateTo(snap(rotRef.current) - delta * step * dir);

  const angleAt = (clientX: number, clientY: number) => {
    const box = ring.current?.getBoundingClientRect();
    if (!box) return 0;
    return (Math.atan2(clientY - (box.top + box.height / 2), clientX - (box.left + box.width / 2)) * 180) / Math.PI;
  };

  const onPointerDown = (e: React.PointerEvent<HTMLDivElement>) => {
    cancelAnimationFrame(anim.current);
    const a = angleAt(e.clientX, e.clientY);
    drag.current = { startAngle: a, startRot: rotRef.current, lastAngle: a, lastTime: performance.now(), velocity: 0, moved: false };
    e.currentTarget.setPointerCapture(e.pointerId);
  };
  const onPointerMove = (e: React.PointerEvent<HTMLDivElement>) => {
    const d = drag.current;
    if (!d) return;
    const a = angleAt(e.clientX, e.clientY);
    let delta = a - d.startAngle;
    if (delta > 180) delta -= 360;
    if (delta < -180) delta += 360;
    const now = performance.now();
    let inst = a - d.lastAngle;
    if (inst > 180) inst -= 360;
    if (inst < -180) inst += 360;
    d.velocity = inst / Math.max(1, now - d.lastTime);
    d.lastAngle = a;
    d.lastTime = now;
    if (Math.abs(delta) > 2) d.moved = true;
    setRot(d.startRot + delta);
  };
  const onPointerUp = () => {
    const d = drag.current;
    drag.current = null;
    if (!d) return;
    if (reduced() || Math.abs(d.velocity) < 0.02) {
      animateTo(snap(rotRef.current));
      return;
    }
    // Inertia, then settle on the nearest photograph.
    let v = d.velocity * 16;
    let r = rotRef.current;
    const tick = () => {
      v *= 0.93;
      r += v;
      setRot(r);
      if (Math.abs(v) > 0.15) anim.current = requestAnimationFrame(tick);
      else animateTo(snap(r));
    };
    anim.current = requestAnimationFrame(tick);
  };

  const onKey = (e: KeyboardEvent<HTMLDivElement>) => {
    const next = rtl ? 'ArrowLeft' : 'ArrowRight';
    const prev = rtl ? 'ArrowRight' : 'ArrowLeft';
    if (e.key === next || e.key === 'ArrowDown') {
      e.preventDefault();
      go(1);
    } else if (e.key === prev || e.key === 'ArrowUp') {
      e.preventDefault();
      go(-1);
    } else if (e.key === 'Home') {
      e.preventDefault();
      animateTo(0);
    }
  };

  if (!current) return null;

  if (grid) {
    return (
      <div className="flex flex-col gap-8">
        <button type="button" className="t-label self-start underline underline-offset-4" onClick={() => setGrid(false)}>
          {t('dial')}
        </button>
        <ul className="grid grid-cols-2 gap-[var(--spacing-gutter)] md:grid-cols-3 lg:grid-cols-4">
          {items.map((it) => (
            <li key={it.id} className="flex flex-col gap-2">
              <div className="relative aspect-[4/5] overflow-hidden bg-surface">
                <Image src={it.image.src} alt={it.image.alt} fill sizes="(min-width: 1024px) 24vw, (min-width: 768px) 32vw, 48vw" quality={70} placeholder={it.image.blur ? 'blur' : 'empty'} blurDataURL={it.image.blur ?? undefined} className="object-cover" style={{ objectPosition: `${it.image.focalX * 100}% ${it.image.focalY * 100}%` }} />
              </div>
              <p className="t-instrument text-muted">{it.hour}</p>
              <p className="t-small">{it.caption}</p>
            </li>
          ))}
        </ul>
      </div>
    );
  }

  return (
    <div className="flex flex-col items-center gap-8">
      <div
        ref={ring}
        role="group"
        aria-roledescription="carousel"
        aria-label={t('title')}
        tabIndex={0}
        onKeyDown={onKey}
        onPointerDown={onPointerDown}
        onPointerMove={onPointerMove}
        onPointerUp={onPointerUp}
        onPointerCancel={onPointerUp}
        className="dial relative aspect-square w-[min(92vw,44rem)] touch-none select-none rounded-full focus-visible:outline-offset-8"
      >
        {/* The dial face: hour ticks around the edge. */}
        <svg viewBox="0 0 200 200" className="pointer-events-none absolute inset-0 h-full w-full" aria-hidden="true">
          <circle cx="100" cy="100" r="98" fill="none" stroke="var(--c-line)" strokeWidth="0.5" />
          {Array.from({ length: 72 }, (_, i) => {
            const a = ((i * 5 + rot) * Math.PI) / 180;
            const long = i % 6 === 0;
            const r1 = 98;
            const r2 = long ? 93 : 96;
            const x = (v: number) => Math.round(v * 100) / 100;
            return <line key={i} x1={x(100 + Math.sin(a) * r1)} y1={x(100 - Math.cos(a) * r1)} x2={x(100 + Math.sin(a) * r2)} y2={x(100 - Math.cos(a) * r2)} stroke="var(--c-muted)" strokeWidth={long ? 0.6 : 0.3} />;
          })}
          <polygon points="100,0 97,6 103,6" fill="var(--c-accent)" />
        </svg>

        {items.map((it, i) => {
          const angle = (i * step * dir + rot) * (Math.PI / 180);
          const x = Math.sin(angle) * 41;
          const y = -Math.cos(angle) * 41;
          const isActive = i === active;
          const style = { '--x': Math.round(x * 100) / 100, '--y': Math.round(y * 100) / 100 } as CSSProperties;
          return (
            <button
              key={it.id}
              type="button"
              tabIndex={-1}
              aria-hidden="true"
              onClick={() => {
                if (drag.current?.moved) return;
                let delta = ((i - active) % n + n) % n;
                if (delta > n / 2) delta -= n;
                go(delta);
              }}
              className={cn('dial-thumb absolute w-[13%] overflow-hidden', isActive && 'is-active')}
              style={style}
            >
              <span className="arch-2x3 relative block aspect-[2/3] w-full overflow-hidden bg-surface">
                <Image src={it.image.src} alt="" fill sizes="96px" quality={55} draggable={false} className="object-cover" style={{ objectPosition: `${it.image.focalX * 100}% ${it.image.focalY * 100}%` }} />
              </span>
            </button>
          );
        })}

        {/* The photograph at the top of the dial, large in the centre. */}
        <figure className="absolute inset-[26%] flex flex-col items-center justify-center gap-3">
          <div key={current.id} className="dial-center arch-3x4 relative aspect-[3/4] w-full max-w-[18rem] overflow-hidden bg-surface cast-shade">
            <Image src={current.image.src} alt={current.image.alt} fill sizes="(min-width: 768px) 288px, 48vw" quality={82} placeholder={current.image.blur ? 'blur' : 'empty'} blurDataURL={current.image.blur ?? undefined} className="object-cover" style={{ objectPosition: `${current.image.focalX * 100}% ${current.image.focalY * 100}%` }} draggable={false} />
          </div>
        </figure>
      </div>

      <div className="flex w-full max-w-xl flex-col items-center gap-4 text-center">
        <p className="t-instrument text-muted" aria-hidden="true">
          {current.hour} · {t('position', { current: formatNumber(active + 1, locale), total: formatNumber(n, locale) })}
        </p>
        <p className="t-heading-md" aria-live="polite">
          <span className="sr-only">{t('position', { current: formatNumber(active + 1, locale), total: formatNumber(n, locale) })}: </span>
          {current.caption}
        </p>
        <div className="flex items-center gap-2">
          <button type="button" onClick={() => go(-1)} className="inline-flex size-12 items-center justify-center border border-line hover-capable:hover:border-ink" aria-label={t('previous')}>
            <Icon name="arrowBack" size={20} />
          </button>
          <button type="button" onClick={() => go(1)} className="inline-flex size-12 items-center justify-center border border-line hover-capable:hover:border-ink" aria-label={t('next')}>
            <Icon name="arrow" size={20} />
          </button>
        </div>
        <p className="t-small text-muted">{t('hint')}</p>
        <button type="button" className="t-label underline underline-offset-4" onClick={() => setGrid(true)}>
          {t('grid')}
        </button>
      </div>
    </div>
  );
}
