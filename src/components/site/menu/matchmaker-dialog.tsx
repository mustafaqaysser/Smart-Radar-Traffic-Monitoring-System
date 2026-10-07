'use client';

import { useLocale, useTranslations } from 'next-intl';
import { useState } from 'react';
import { Dialog } from '@/components/site/ui/dialog';
import { Chip } from '@/components/site/ui/field';
import { Button } from '@/components/site/ui/button';
import { Link } from '@/i18n/navigation';
import { matchDishes, type Heat, type Hunger, type MatchInput, type Mood } from '@/lib/menu/matchmaker';
import type { ItemView } from '@/lib/menu/view';
import { formatMoney } from '@/lib/i18n/format';

const HUNGER: Hunger[] = ['light', 'proper', 'share'];
const MOOD: Mood[] = ['sea', 'embers', 'garden', 'sweet', 'surprise'];
const HEAT: Heat[] = ['none', 'some', 'all'];
const cap = (s: string) => s.charAt(0).toUpperCase() + s.slice(1);

/** Three questions → three dishes from the menu on screen (after the visitor's filters). */
export function MatchmakerDialog({ open, onClose, items, onAdd }: { open: boolean; onClose: () => void; items: ItemView[]; onAdd: (item: ItemView) => void }) {
  const t = useTranslations('menu.matchmaker');
  const tc = useTranslations('common');
  const locale = useLocale();
  const [answers, setAnswers] = useState<Partial<MatchInput>>({});
  const complete = answers.hunger && answers.mood && answers.heat;
  const results = complete ? matchDishes(items, answers as MatchInput) : [];

  const question = <K extends keyof MatchInput>(key: K, label: string, options: MatchInput[K][], text: (o: MatchInput[K]) => string) => (
    <fieldset className="flex flex-col gap-3">
      <legend className="t-heading-sm mb-3">{label}</legend>
      <div className="flex flex-wrap gap-2">
        {options.map((o) => (
          <Chip key={o} pressed={answers[key] === o} onClick={() => setAnswers((a) => ({ ...a, [key]: o }))}>
            {text(o)}
          </Chip>
        ))}
      </div>
    </fieldset>
  );

  return (
    <Dialog open={open} onClose={onClose} title={t('title')} closeLabel={tc('a11y.close')} variant="sheet">
      <div className="flex flex-col gap-8">
        <p className="t-body text-muted">{t('intro')}</p>
        {question('hunger', t('hunger'), HUNGER, (o) => t(`hunger${cap(o)}`))}
        {question('mood', t('mood'), MOOD, (o) => t(`mood${cap(o)}`))}
        {question('heat', t('spice'), HEAT, (o) => t(`spice${cap(o)}`))}
        {complete ? (
          <section aria-live="polite" className="flex flex-col gap-5 border-t border-line pt-6">
            <h3 className="t-heading-md">{results.length ? t('results') : t('noResults')}</h3>
            <ol className="flex flex-col gap-5">
              {results.map(({ item, reasons }) => (
                <li key={item.slug} className="flex flex-col gap-2 border-b border-line pb-5">
                  <div className="flex items-baseline justify-between gap-4">
                    <Link href={`/menu/dish/${item.slug}`} className="t-heading-sm underline decoration-transparent underline-offset-4 hover-capable:hover:decoration-current">
                      {item.name}
                    </Link>
                    <span className="t-small tabular text-muted">
                      <bdi>{formatMoney(item.price, locale)}</bdi>
                    </span>
                  </div>
                  <p className="t-small text-muted">{reasons.map((r) => t(`reasons.${r}`)).join(' · ')}</p>
                  {item.orderable ? (
                    <Button variant="secondary" size="sm" icon={null} leadingIcon="plus" className="self-start" onClick={() => onAdd(item)}>
                      {tc('actions.add')}
                    </Button>
                  ) : null}
                </li>
              ))}
            </ol>
            <button type="button" className="t-label self-start underline underline-offset-4" onClick={() => setAnswers({})}>
              {t('again')}
            </button>
          </section>
        ) : null}
      </div>
    </Dialog>
  );
}
