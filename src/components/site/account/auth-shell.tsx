import type { ReactNode } from 'react';
import { SplitWords } from '@/components/motion/split-words';
import { Reveal } from '@/components/motion/reveal';
import { MediaImage } from '@/components/site/ui/media-image';
import type { MediaDTO } from '@/lib/queries/types';

/** Sign in, sign up and password pages: the form on one side, a doorway in shade on the other. */
export function AuthShell({ eyebrow, title, body, image, locale, children }: { eyebrow: string; title: string; body?: string; image: MediaDTO | null; locale: string; children: ReactNode }) {
  return (
    <div className="site-grid gap-y-12 pt-10 pb-[var(--spacing-section)] lg:pt-16">
      <div className="col-span-full flex flex-col gap-8 md:col-span-5 lg:col-span-6">
        <div className="flex flex-col gap-5">
          <Reveal as="p" variant="fade" className="t-label text-muted">
            {eyebrow}
          </Reveal>
          <h1 className="t-display-lg">
            <SplitWords text={title} />
          </h1>
          {body ? <p className="t-body-lg measure">{body}</p> : null}
        </div>
        <div className="border-t border-ink pt-8">{children}</div>
      </div>
      {image ? (
        <div className="col-span-full hidden md:col-span-3 md:block lg:col-span-4 lg:col-start-9">
          <div className="sticky top-24">
            <MediaImage media={image} locale={locale} sizes="(min-width: 1024px) 30vw, 35vw" shape="arch-3x4" ratio="3/4" className="cast-shade" decorative />
          </div>
        </div>
      ) : null}
    </div>
  );
}
