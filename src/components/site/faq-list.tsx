'use client';

import { useTranslations } from 'next-intl';
import { useId, useMemo, useState } from 'react';
import { Icon } from '@/components/brand/icon';
import { Chip, Input } from '@/components/site/ui/field';
import { matchesSearch } from '@/lib/i18n/arabic';

export interface FaqItem {
  id: string;
  category: string;
  question: string;
  answer: string;
  /** Question and answer in every language, so either can be searched. */
  search: string;
}

/** Searchable, filterable questions in native disclosure widgets (keyboard and screen-reader friendly). */
export function FaqList({ items, categories }: { items: FaqItem[]; categories: string[] }) {
  const t = useTranslations('pages.faq');
  const id = useId();
  const [query, setQuery] = useState('');
  const [category, setCategory] = useState<string | null>(null);
  const visible = useMemo(() => items.filter((i) => (!category || i.category === category) && matchesSearch(i.search, query)), [items, category, query]);
  const grouped = categories.map((c) => ({ c, list: visible.filter((i) => i.category === c) })).filter((g) => g.list.length);

  return (
    <div className="flex flex-col gap-10">
      <div className="flex flex-col gap-6">
        <label htmlFor={`${id}-q`} className="sr-only">
          {t('search')}
        </label>
        <div className="relative max-w-xl">
          <Icon name="search" size={18} className="pointer-events-none absolute inset-y-0 start-4 my-auto text-muted" />
          <Input id={`${id}-q`} type="search" value={query} onChange={(e) => setQuery(e.target.value)} placeholder={t('search')} className="ps-11" />
        </div>
        <div className="flex flex-wrap gap-2">
          {categories.map((c) => (
            <Chip key={c} pressed={category === c} onClick={() => setCategory(category === c ? null : c)}>
              {t(`categories.${c}`)}
            </Chip>
          ))}
        </div>
      </div>
      <p className="sr-only" aria-live="polite">
        {visible.length ? '' : t('noResults')}
      </p>
      {grouped.length === 0 ? <p className="t-body text-muted">{t('noResults')}</p> : null}
      {grouped.map(({ c, list }) => (
        <section key={c} aria-labelledby={`${id}-${c}`}>
          <h2 id={`${id}-${c}`} className="t-label mb-4 text-muted">
            {t(`categories.${c}`)}
          </h2>
          <div className="border-t border-ink">
            {list.map((i) => (
              <details key={i.id} className="faq-item group border-b border-line">
                <summary className="flex min-h-16 cursor-pointer list-none items-center justify-between gap-6 py-5 [&::-webkit-details-marker]:hidden">
                  <h3 className="t-heading-sm">{i.question}</h3>
                  <span aria-hidden="true" className="shrink-0 transition-transform duration-[var(--dur-base)] ease-[var(--ease-shade)] group-open:rotate-45">
                    <Icon name="plus" size={22} />
                  </span>
                </summary>
                <p className="t-body measure pb-6">{i.answer}</p>
              </details>
            ))}
          </div>
        </section>
      ))}
    </div>
  );
}
