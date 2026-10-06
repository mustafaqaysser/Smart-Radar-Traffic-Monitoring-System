import { getTranslations } from 'next-intl/server';
import { ButtonLink, ArrowLink } from '@/components/site/ui/button';
import { MediaImage } from '@/components/site/ui/media-image';
import { InstrumentLine } from '@/components/site/instrument-line';
import type { MediaDTO } from '@/lib/queries/types';
import { HoldSun } from './hold-sun';
import { PalmShade } from './palm-shade';

interface HeroProps {
  locale: string;
  line: string;
  intro: string;
  servingNow: string[];
  photo: MediaDTO | null;
  photoMenu: string | null;
  reservations: boolean;
}

/**
 * Arrival: the concept line, enormous, casting the shade of the real sun over the house right now. On
 * desktop the pointer can hold the sun for a moment. Palm fronds throw their shadows across the plaster.
 */
export async function Hero({ locale, line, intro, servingNow, photo, photoMenu, reservations }: HeroProps) {
  const t = await getTranslations('home.hero');
  const tc = await getTranslations('common');
  return (
    <section id="hero" className="hero relative isolate overflow-clip" aria-labelledby="hero-title">
      <PalmShade className="hero-palms pointer-events-none absolute inset-y-0 end-0 -z-10 h-full w-[115%] max-w-none md:w-[80%]" />
      <div className="hero-lamp pointer-events-none absolute -z-10" aria-hidden="true" />
      <div className="site-grid min-h-[calc(100svh-4rem)] content-between gap-y-10 pt-6 pb-24 lg:min-h-[calc(100svh-4.5rem)] lg:pt-10 lg:pb-16">
        <InstrumentLine servingNow={servingNow} className="col-span-full text-muted" />

        <h1 id="hero-title" data-hero-title className="hero-title t-display-xl col-span-full lg:col-span-11">
          <span className="hero-shade" aria-hidden="true">
            {line}
          </span>
          <span className="hero-ink">{line}</span>
        </h1>

        <div className="col-span-full flex flex-col gap-8 md:col-span-5 lg:col-span-5 lg:self-end">
          <p className="t-body-lg measure text-ink">{intro}</p>
          <div className="flex flex-wrap items-center gap-x-8 gap-y-4">
            {reservations ? (
              <ButtonLink href="/reserve" size="lg">
                {tc('nav.reserve')}
              </ButtonLink>
            ) : null}
            <ArrowLink href="/menu">{t('servingNow')}</ArrowLink>
          </div>
        </div>

        {photo ? (
          <figure className="col-span-2 col-start-3 self-end md:col-span-3 md:col-start-6 lg:col-span-3 lg:col-start-10">
            <MediaImage media={photo} locale={locale} sizes="(min-width: 1024px) 22vw, (min-width: 768px) 34vw, 46vw" ratio="3/4" shape="arch-3x4" eager className="cast-shade" />
            {photoMenu ? <figcaption className="t-instrument mt-4 text-muted">{t('photoCaption', { menu: photoMenu })}</figcaption> : null}
          </figure>
        ) : null}
      </div>
      <HoldSun targetId="hero" />
    </section>
  );
}
