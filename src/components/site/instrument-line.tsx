'use client';

import { useLocale, useTranslations } from 'next-intl';
import { compassPoint } from '@/lib/time/sun';
import { formatClock, formatDurationMinutes, formatList, formatNumber } from '@/lib/i18n/format';
import { cn } from '@/lib/utils/cn';
import { useAtmosphere } from './atmosphere-provider';

/**
 * The live instrument: "Jeddah · 16:42 · sun 28° WSW — the long shade · now serving: coffee & sweets".
 * Reads the sun computed for the selected house; times are in the house's time zone.
 */
export function InstrumentLine({ servingNow, className, showServing = true }: { servingNow: string[]; className?: string; showServing?: boolean }) {
  const { branch, state } = useAtmosphere();
  const t = useTranslations('common');
  const locale = useLocale();
  if (!branch) return null;
  const at = new Date(state.at);
  const direction = t(`compass.${compassPoint(state.azimuth)}`);
  const sun =
    state.altitude > 0
      ? t('instrument.sun', { altitude: formatNumber(Math.round(state.altitude), locale), direction })
      : `${t('instrument.sunBelow')} · ${t('instrument.lamp')}`;
  const serving = servingNow.length ? t('instrument.serving', { menus: formatList(servingNow, locale) }) : t('instrument.nothing');
  return (
    <p className={cn('t-instrument flex flex-wrap items-center gap-x-3 gap-y-1', className)} aria-label={t('instrument.label', { city: branch.city })}>
      <span>{branch.city}</span>
      <span aria-hidden="true">·</span>
      <time dateTime={state.at} className="tabular">
        <bdi>{formatClock(at, locale, branch.timeZone)}</bdi>
      </time>
      <span aria-hidden="true">·</span>
      <span>{sun}</span>
      <span aria-hidden="true">—</span>
      <span>{t(`phase.${state.phase}`)}</span>
      {state.minutesToSunset !== null && state.minutesToSunset > 0 && state.minutesToSunset <= 90 ? (
        <>
          <span aria-hidden="true">·</span>
          <span>{t('instrument.sunsetIn', { duration: formatDurationMinutes(state.minutesToSunset, locale) })}</span>
        </>
      ) : null}
      {showServing ? (
        <>
          <span aria-hidden="true">·</span>
          <span>{serving}</span>
        </>
      ) : null}
    </p>
  );
}
