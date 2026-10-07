import type { ReactNode } from 'react';
import { Monogram } from '@/components/brand/logo';

/** Shared layout for 404, 500 and offline: the monogram's shade, a title, a line, and a way on. */
export function ErrorView({ code, title, body, children }: { code?: string; title: string; body: string; children?: ReactNode }) {
  return (
    <section className="site-grid min-h-[70svh] content-center gap-y-10 py-16" aria-labelledby="error-title">
      <div className="col-span-full flex items-end gap-6 md:col-span-3">
        <Monogram decorative shade className="h-28 w-auto md:h-40" />
        {code ? (
          <p className="t-instrument text-muted" aria-hidden="true">
            {code}
          </p>
        ) : null}
      </div>
      <div className="col-span-full flex flex-col gap-6 md:col-span-5 lg:col-span-7">
        <h1 id="error-title" className="t-display-md">
          {title}
        </h1>
        <p className="t-body-lg measure">{body}</p>
        {children ? <div className="flex flex-wrap items-center gap-x-8 gap-y-4">{children}</div> : null}
      </div>
    </section>
  );
}
