import type { Metadata } from 'next';
import { notFound } from 'next/navigation';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import { SplitWords } from '@/components/motion/split-words';
import { Link } from '@/i18n/navigation';
import { formatDateString } from '@/lib/i18n/format';
import { pageMetadata } from '@/lib/site/metadata';
import { isDemoMode } from '@/lib/site/url';
import { cn } from '@/lib/utils/cn';

const DOCS = ['privacy', 'terms', 'cookies', 'allergens', 'accessibility'] as const;
type Doc = (typeof DOCS)[number];
/** Date the legal copy was last revised (update together with src/messages/<locale>/legal.json). */
const UPDATED = '2026-10-01';

interface Section {
  heading: string;
  paragraphs: string[];
  list: string[];
}

type Params = Promise<{ locale: string; doc: string }>;

export function generateStaticParams() {
  return DOCS.map((doc) => ({ doc }));
}

export async function generateMetadata({ params }: { params: Params }): Promise<Metadata> {
  const { locale, doc } = await params;
  if (!DOCS.includes(doc as Doc)) return {};
  const t = await getTranslations({ locale, namespace: 'legal' });
  return pageMetadata({ locale, path: `/legal/${doc}`, title: t(`docs.${doc}.title`), description: t(`docs.${doc}.summary`) });
}

export default async function LegalPage({ params }: { params: Params }) {
  const { locale, doc } = await params;
  if (!DOCS.includes(doc as Doc)) notFound();
  setRequestLocale(locale);
  const t = await getTranslations('legal');
  const sections = t.raw(`docs.${doc}.sections`) as Section[];
  const anchor = (i: number) => `s${i + 1}`;
  return (
    <article className="site-grid gap-y-10 pt-10 lg:pt-16" aria-labelledby="legal-title">
      <header className="col-span-full flex flex-col gap-5 lg:col-span-8 lg:col-start-4">
        <p className="t-label text-muted">{t('meta.title')}</p>
        <h1 id="legal-title" className="t-display-lg">
          <SplitWords text={t(`docs.${doc}.title`)} />
        </h1>
        <p className="t-body-lg measure">{t(`docs.${doc}.summary`)}</p>
        <p className="t-small text-muted">{t('updated', { date: formatDateString(UPDATED, locale, { day: 'numeric', month: 'long', year: 'numeric' }) })}</p>
        {isDemoMode() ? <p className="t-small border-s-2 border-warning ps-3">{t('demo')}</p> : null}
      </header>
      <aside className="col-span-full lg:col-span-3 lg:row-span-2 lg:row-start-1">
        <nav aria-label={t('meta.title')} className="flex flex-col gap-8 lg:sticky lg:top-24">
          <ul className="flex flex-col gap-1">
            {DOCS.map((d) => (
              <li key={d}>
                <Link href={`/legal/${d}`} aria-current={d === doc ? 'page' : undefined} className={cn('t-small inline-flex min-h-10 items-center underline-offset-4', d === doc ? 'font-semibold text-accent' : 'hover-capable:hover:underline')}>
                  {t(`docs.${d}.title`)}
                </Link>
              </li>
            ))}
          </ul>
          <div>
            <p className="t-label mb-3 text-muted">{t('contents')}</p>
            <ol className="flex flex-col gap-1">
              {sections.map((s, i) => (
                <li key={s.heading}>
                  <a href={`#${anchor(i)}`} className="t-small inline-flex min-h-9 items-center text-muted hover-capable:hover:text-ink">
                    {s.heading}
                  </a>
                </li>
              ))}
            </ol>
          </div>
        </nav>
      </aside>
      <div className="prose-zill col-span-full lg:col-span-7 lg:col-start-4">
        {sections.map((s, i) => (
          <section key={s.heading} id={anchor(i)} aria-labelledby={`${anchor(i)}-h`} className="scroll-mt-24">
            <h2 id={`${anchor(i)}-h`}>{s.heading}</h2>
            {s.paragraphs.map((p) => (
              <p key={p}>{p}</p>
            ))}
            {s.list.length ? (
              <ul>
                {s.list.map((li) => (
                  <li key={li}>{li}</li>
                ))}
              </ul>
            ) : null}
          </section>
        ))}
      </div>
    </article>
  );
}
