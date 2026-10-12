'use client';

import { ChevronLeft, ChevronRight } from 'lucide-react';
import { useTranslations } from 'next-intl';
import { useRouter } from 'next/navigation';
import { useId } from 'react';
import { Button } from '../ui/button';
import { Input } from '../ui/input';

/** Previous / today / next and a date field; `query` carries the other parameters (view, house). */
export function DayNav({ date, today, prev, next, path, query, label }: { date: string; today: string; prev: string; next: string; path: string; query: string; label: string }) {
  const hrefFor = (d: string) => `${path}?${query}${query ? '&' : ''}date=${d}`;
  const t = useTranslations('admin.reservations.nav');
  const router = useRouter();
  const id = useId();
  return (
    <div className="flex flex-wrap items-center gap-1.5">
      <Button size="icon-sm" variant="ghost" aria-label={t('previous')} onClick={() => router.push(hrefFor(prev))}>
        <ChevronLeft className="rtl:-scale-x-100" />
      </Button>
      <Button size="sm" variant={date === today ? 'secondary' : 'outline'} onClick={() => router.push(hrefFor(today))}>
        {t('today')}
      </Button>
      <Button size="icon-sm" variant="ghost" aria-label={t('next')} onClick={() => router.push(hrefFor(next))}>
        <ChevronRight className="rtl:-scale-x-100" />
      </Button>
      <label htmlFor={`${id}-date`} className="sr-only">
        {label}
      </label>
      <Input id={`${id}-date`} type="date" value={date} onChange={(e) => e.target.value && router.push(hrefFor(e.target.value))} className="w-auto" dir="ltr" />
    </div>
  );
}
