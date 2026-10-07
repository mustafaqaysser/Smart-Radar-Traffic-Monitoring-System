'use client';

import { useEffect, useRef } from 'react';
import type { Phase } from '@/lib/brand/palette';
import type { MediaDTO } from '@/lib/queries/types';
import { MediaImage } from '@/components/site/ui/media-image';
import { isFinePointerDesktop, prefersReducedMotion } from '@/lib/motion/tokens';

export interface DayHour {
  phase: Phase;
  time: string;
  title: string;
  body: string;
  menu: string | null;
  photo: MediaDTO | null;
}

interface DayChapterProps {
  locale: string;
  eyebrow: string;
  title: string;
  skipLabel: string;
  hours: DayHour[];
}

/**
 * "A day in the courtyard": scroll carries the sun across the house from dawn to night. Each hour is a full
 * scene in that hour's own palette; on desktop the scenes are pinned and cross-fade as you scroll (native
 * scroll, scrubbed, never hijacked, pinned for two viewports). On touch screens and under reduced motion the
 * same scenes simply follow one another.
 */
export function DayChapter({ locale, eyebrow, title, skipLabel, hours }: DayChapterProps) {
  const root = useRef<HTMLElement>(null);

  useEffect(() => {
    const section = root.current;
    if (!section || prefersReducedMotion() || !isFinePointerDesktop()) return;
    let cleanup: (() => void) | undefined;
    let cancelled = false;
    void (async () => {
      const [{ gsap }, { ScrollTrigger }] = await Promise.all([import('gsap'), import('gsap/ScrollTrigger')]);
      if (cancelled) return;
      gsap.registerPlugin(ScrollTrigger);
      section.dataset.mode = 'pinned';
      const track = section.querySelector<HTMLElement>('.day-track');
      const scenes = Array.from(section.querySelectorAll<HTMLElement>('.day-scene'));
      const arm = section.querySelector<SVGGElement>('.day-sun-arm');
      const sun = section.querySelector<SVGCircleElement>('.day-sun-dot');
      const marks = Array.from(section.querySelectorAll<HTMLElement>('.day-mark'));
      if (!track || scenes.length < 2) return;
      const ctx = gsap.context(() => {
        gsap.set(scenes.slice(1), { autoAlpha: 0 });
        const tl = gsap.timeline({
          defaults: { ease: 'none' },
          scrollTrigger: { trigger: track, start: 'top top', end: 'bottom bottom', scrub: 0.5 },
        });
        scenes.forEach((scene, i) => {
          if (i === 0) return;
          const at = i - 0.6;
          tl.to(scene, { autoAlpha: 1, duration: 0.6 }, at);
          const copy = scene.querySelector('.day-copy');
          const photo = scene.querySelector('.day-photo');
          if (copy) tl.fromTo(copy, { y: 48 }, { y: 0, duration: 0.8, ease: 'power2.out' }, at);
          if (photo) tl.fromTo(photo, { clipPath: 'inset(100% 0 0 0)' }, { clipPath: 'inset(0% 0 0 0)', duration: 0.8, ease: 'power2.out' }, at + 0.1);
        });
        const last = scenes.length - 1;
        if (arm) tl.fromTo(arm, { rotate: -172 }, { rotate: -8, duration: last - 0.6, svgOrigin: '200 200' }, 0);
        if (sun) tl.to(sun, { autoAlpha: 0, duration: 0.4 }, last - 0.8);
        marks.forEach((m, i) => tl.to(m, { opacity: 1, duration: 0.2 }, Math.max(0, i - 0.6)));
      }, section);
      cleanup = () => {
        ctx.revert();
        delete section.dataset.mode;
      };
    })();
    return () => {
      cancelled = true;
      cleanup?.();
    };
  }, []);

  return (
    <section ref={root} id="day" className="day relative" aria-labelledby="day-title">
      <div className="site-grid pt-[var(--spacing-section)] pb-[var(--spacing-stack)]">
        <p className="t-label col-span-full text-muted">{eyebrow}</p>
        <h2 id="day-title" className="t-display-md col-span-full mt-4 lg:col-span-9">
          {title}
        </h2>
        <a href="#after-day" className="t-small col-span-full mt-6 justify-self-start underline decoration-line underline-offset-4">
          {skipLabel}
        </a>
      </div>
      <div className="day-track" style={{ '--day-count': hours.length } as React.CSSProperties}>
        <div className="day-stage">
          {hours.map((h, i) => (
            <article key={h.phase} data-phase={h.phase} className="day-scene bg-bg text-ink" aria-labelledby={`day-${h.phase}`} style={{ zIndex: i + 1 }}>
              <div className="site-grid h-full content-center gap-y-8 py-[var(--spacing-stack)]">
                <div className="day-copy col-span-full flex flex-col gap-4 md:col-span-4 lg:col-span-5 lg:col-start-1">
                  <p className="t-instrument text-muted">
                    <bdi>{h.time}</bdi>
                  </p>
                  <h3 id={`day-${h.phase}`} className="t-display-md">
                    {h.title}
                  </h3>
                  <p className="t-body-lg measure">{h.body}</p>
                  {h.menu ? <p className="t-label text-muted">{h.menu}</p> : null}
                </div>
                {h.photo ? (
                  <div className="day-photo col-span-3 col-start-2 md:col-span-3 md:col-start-6 lg:col-span-4 lg:col-start-8">
                    <MediaImage media={h.photo} locale={locale} sizes="(min-width: 1024px) 30vw, (min-width: 768px) 36vw, 70vw" ratio="4/5" shape="arch-4x5" />
                  </div>
                ) : null}
              </div>
            </article>
          ))}
          <div className="day-instruments pointer-events-none" aria-hidden="true">
            <svg viewBox="0 0 400 210" className="day-dial">
              <path d="M20 200 A180 180 0 0 1 380 200" fill="none" stroke="currentColor" strokeWidth="1" strokeDasharray="2 6" />
              <g className="day-sun-arm">
                <circle className="day-sun-dot" cx="380" cy="200" r="9" fill="var(--c-sun, #D9982F)" />
              </g>
            </svg>
            <ol className="day-marks">
              {hours.map((h) => (
                <li key={h.phase} className="day-mark t-instrument">
                  <bdi>{h.time}</bdi>
                </li>
              ))}
            </ol>
          </div>
        </div>
      </div>
      <div id="after-day" tabIndex={-1} />
    </section>
  );
}
