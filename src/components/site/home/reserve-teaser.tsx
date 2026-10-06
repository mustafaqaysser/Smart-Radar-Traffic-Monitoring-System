'use client';

import { useLocale, useTranslations } from 'next-intl';
import { useId, useState } from 'react';
import { useRouter } from '@/i18n/navigation';
import { Button } from '@/components/site/ui/button';
import { Label, Select } from '@/components/site/ui/field';
import { formatDateString } from '@/lib/i18n/format';
import { plural } from '@/lib/i18n/plural';

interface ReserveTeaserProps {
  branches: { slug: string; name: string }[];
  selected: string | null;
  dates: string[];
  maxParty: number;
}

/** Starts a booking from the home page: house, day and party size, then the full flow takes over. */
export function ReserveTeaser({ branches, selected, dates, maxParty }: ReserveTeaserProps) {
  const t = useTranslations('reserve.fields');
  const th = useTranslations('home.reserve');
  const tu = useTranslations('common.units');
  const locale = useLocale();
  const router = useRouter();
  const id = useId();
  const [branch, setBranch] = useState(selected ?? branches[0]?.slug ?? '');
  const [date, setDate] = useState(dates[0] ?? '');
  const [party, setParty] = useState(2);
  return (
    <form
      className="grid gap-6 md:grid-cols-4 md:items-end"
      onSubmit={(e) => {
        e.preventDefault();
        router.push({ pathname: '/reserve', query: { branch, date, party: String(party) } });
      }}
    >
      <div>
        <Label htmlFor={`${id}-branch`}>{t('branch')}</Label>
        <Select id={`${id}-branch`} value={branch} onChange={(e) => setBranch(e.target.value)}>
          {branches.map((b) => (
            <option key={b.slug} value={b.slug}>
              {b.name}
            </option>
          ))}
        </Select>
      </div>
      <div>
        <Label htmlFor={`${id}-date`}>{t('date')}</Label>
        <Select id={`${id}-date`} value={date} onChange={(e) => setDate(e.target.value)}>
          {dates.map((d) => (
            <option key={d} value={d}>
              {formatDateString(d, locale, { weekday: 'long', day: 'numeric', month: 'long' })}
            </option>
          ))}
        </Select>
      </div>
      <div>
        <Label htmlFor={`${id}-party`}>{t('party')}</Label>
        <Select id={`${id}-party`} value={party} onChange={(e) => setParty(Number(e.target.value))}>
          {Array.from({ length: maxParty }, (_, i) => i + 1).map((n) => (
            <option key={n} value={n}>
              {tu('guests', plural(n, locale))}
            </option>
          ))}
        </Select>
      </div>
      <Button type="submit" size="lg" icon="arrow">
        {th('cta')}
      </Button>
    </form>
  );
}
