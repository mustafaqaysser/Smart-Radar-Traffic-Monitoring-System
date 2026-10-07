import type { Metadata } from 'next';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import { Reveal } from '@/components/motion/reveal';
import { ArrowLink } from '@/components/site/ui/button';
import { PageHeader } from '@/components/site/ui/page-header';
import { formatNumber, quoted } from '@/lib/i18n/format';
import { tr } from '@/lib/i18n/localized';
import { getPress } from '@/lib/queries/content';
import { getSettings } from '@/lib/server/settings';
import { pageMetadata } from '@/lib/site/metadata';

export async function generateMetadata({ params }: { params: Promise<{ locale: string }> }): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: 'pages.press.meta' });
  return pageMetadata({ locale, path: '/press', title: t('title'), description: t('description'), image: 'press' });
}

export default async function PressPage({ params }: { params: Promise<{ locale: string }> }) {
  const { locale } = await params;
  setRequestLocale(locale);
  const [t, items, settings] = await Promise.all([getTranslations('pages.press'), getPress(), getSettings()]);
  const awards = items.filter((i) => i.kind === 'award');
  const articles = items.filter((i) => i.kind === 'press');
  const year = (y: number) => formatNumber(y, locale, { useGrouping: false });
  return (
    <>
      <PageHeader eyebrow={t('eyebrow')} title={t('title')} intro={<p>{t('intro')}</p>} />
      {awards.length ? (
        <section className="site-grid gap-y-8" aria-labelledby="press-awards">
          <h2 id="press-awards" className="t-label col-span-full text-muted">
            {t('awards')}
          </h2>
          <ul className="col-span-full grid gap-[var(--spacing-gutter)] md:grid-cols-2">
            {awards.map((a, i) => (
              <Reveal as="li" key={a.id} delay={i * 90} className="flex flex-col gap-4 bg-ink p-8 text-bg surface-inverse lg:p-12">
                <p className="t-instrument opacity-80">
                  {tr(a.publication, locale)} · {year(a.year)}
                </p>
                <p className="t-heading-lg">{tr(a.title, locale)}</p>
                <p className="t-body opacity-90">{quoted(tr(a.quote, locale), locale)}</p>
              </Reveal>
            ))}
          </ul>
        </section>
      ) : null}
      <section className="site-grid gap-y-8 pt-[var(--spacing-section)]" aria-labelledby="press-articles">
        <h2 id="press-articles" className="t-label col-span-full text-muted">
          {t('articles')}
        </h2>
        <ul className="col-span-full border-t border-ink">
          {articles.map((a) => (
            <li key={a.id} className="grid gap-4 border-b border-line py-10 md:grid-cols-12">
              <p className="t-instrument text-muted md:col-span-3">
                {tr(a.publication, locale)}
                <br />
                {year(a.year)}
              </p>
              <div className="flex flex-col gap-3 md:col-span-9">
                <h3 className="t-heading-md">{tr(a.title, locale)}</h3>
                <blockquote className="t-body-lg text-muted">{quoted(tr(a.quote, locale), locale)}</blockquote>
              </div>
            </li>
          ))}
        </ul>
      </section>
      <section className="site-grid gap-y-6 pt-[var(--spacing-section)]" aria-labelledby="press-contact">
        <h2 id="press-contact" className="t-heading-lg col-span-full">
          {t('contactTitle')}
        </h2>
        <p className="t-body-lg measure col-span-full">{t('contactBody')}</p>
        <div className="col-span-full flex flex-wrap items-center gap-x-8 gap-y-4">
          <a href={`mailto:${settings.contact.press}`} dir="ltr" className="t-heading-sm underline decoration-line underline-offset-4">
            {settings.contact.press}
          </a>
          <ArrowLink href="/brand">{t('brand')}</ArrowLink>
        </div>
      </section>
    </>
  );
}
