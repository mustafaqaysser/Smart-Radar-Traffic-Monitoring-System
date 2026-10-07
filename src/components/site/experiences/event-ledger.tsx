'use client';

import Image from 'next/image';
import { useLocale, useTranslations } from 'next-intl';
import { useState } from 'react';
import { Chip } from '@/components/site/ui/field';
import { Link } from '@/i18n/navigation';
import { formatMoney } from '@/lib/i18n/format';
import { plural } from '@/lib/i18n/plural';
import type { ImageView } from '@/lib/menu/view';
import { cn } from '@/lib/utils/cn';

export interface EventRow {
  slug: string;
  kind: 'chefs_table' | 'tasting' | 'class' | 'gathering';
  title: string;
  summary: string;
  monthKey: string;
  monthLabel: string;
  day: string;
  weekday: string;
  timeLabel: string;
  house: string;
  seatsLeft: number;
  priceFrom: number | null;
  image: ImageView | null;
}

const KINDS = ['chefs_table', 'tasting', 'class', 'gathering'] as const;

/** The gatherings as a ledger by month, filterable by kind. */
export function EventLedger({ events }: { events: EventRow[] }) {
  const t = useTranslations('gather.experiences');
  const locale = useLocale();
  const [kind, setKind] = useState<string | null>(null);
  const kinds = KINDS.filter((k) => events.some((e) => e.kind === k));
  const shown = kind ? events.filter((e) => e.kind === kind) : events;
  const months = [...new Map(shown.map((e) => [e.monthKey, e.monthLabel])).entries()];

  return (
    <div className="flex flex-col gap-10">
      {kinds.length > 1 ? (
        <div className="flex flex-wrap gap-2" role="group" aria-label={t('calendar')}>
          <Chip pressed={kind === null} onClick={() => setKind(null)}>
            {t('all')}
          </Chip>
          {kinds.map((k) => (
            <Chip key={k} pressed={kind === k} onClick={() => setKind(k)}>
              {t(`kinds.${k}`)}
            </Chip>
          ))}
        </div>
      ) : null}
      {months.length === 0 ? <p className="t-body-lg">{kind ? t('noneFilter') : t('none')}</p> : null}
      {months.map(([key, label]) => (
        <section key={key} aria-labelledby={`m-${key}`} className="flex flex-col">
          <h2 id={`m-${key}`} className="t-heading-lg mb-4">
            {label}
          </h2>
          <ul className="border-t border-ink">
            {shown
              .filter((e) => e.monthKey === key)
              .map((e) => (
                <li key={e.slug} id={`event-${e.slug}`} data-card className="relative grid scroll-mt-24 grid-cols-[4.5rem_1fr] gap-x-5 gap-y-3 border-b border-line py-7 md:grid-cols-[6rem_1fr_10rem] md:gap-x-8">
                  <div className="flex flex-col items-start" aria-hidden="true">
                    <span className="t-display-md tabular leading-none">{e.day}</span>
                    <span className="t-label mt-2 text-muted">{e.weekday}</span>
                  </div>
                  <div className="flex min-w-0 flex-col gap-2">
                    <p className="t-label text-muted">
                      {t(`kinds.${e.kind}`)} · {e.house}
                    </p>
                    <h3 className="t-heading-lg">
                      <Link href={`/experiences/${e.slug}`} className="card-link">
                        {e.title}
                      </Link>
                    </h3>
                    <p className="t-body text-muted measure">{e.summary}</p>
                    <p className="t-small flex flex-wrap gap-x-3">
                      <span className="tabular">{e.timeLabel}</span>
                      <span aria-hidden="true">·</span>
                      <span className={cn(e.seatsLeft === 0 && 'text-danger')}>{e.seatsLeft === 0 ? t('soldOut') : t('seatsLeft', plural(e.seatsLeft, locale))}</span>
                      {e.priceFrom !== null ? (
                        <>
                          <span aria-hidden="true">·</span>
                          <span className="tabular">{e.priceFrom === 0 ? t('free') : t('from', { price: formatMoney(e.priceFrom, locale) })}</span>
                        </>
                      ) : null}
                    </p>
                  </div>
                  {e.image ? (
                    <div className="arch-3x4 relative hidden aspect-[3/4] overflow-hidden bg-surface md:block">
                      <Image src={e.image.src} alt="" fill sizes="160px" quality={55} loading={e.slug === shown[0]?.slug ? 'eager' : 'lazy'} className="object-cover" style={{ objectPosition: `${e.image.focalX * 100}% ${e.image.focalY * 100}%` }} />
                    </div>
                  ) : null}
                </li>
              ))}
          </ul>
        </section>
      ))}
    </div>
  );
}
