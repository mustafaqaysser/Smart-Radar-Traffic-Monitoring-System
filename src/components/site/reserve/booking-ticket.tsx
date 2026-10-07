'use client';

import Image from 'next/image';
import { useLocale, useTranslations } from 'next-intl';
import type { CSSProperties } from 'react';
import { Icon } from '@/components/brand/icon';
import { atmosphereCssVars, computeAtmosphere } from '@/lib/brand/atmosphere';
import { formatClock, formatDateString, formatMoney } from '@/lib/i18n/format';
import { plural } from '@/lib/i18n/plural';
import type { FlowBranch, PublicSlot } from '@/lib/reserve/types';
import { cn } from '@/lib/utils/cn';
import { formatCountdown } from './use-countdown';

interface BookingTicketProps {
  branch: FlowBranch | null;
  date: string | null;
  party: number | null;
  slot: PublicSlot | null;
  area: string | null;
  occasion: string | null;
  deposit: number;
  secondsLeft: number | null;
  holdSeconds: number;
  className?: string;
}

/**
 * The booking taking shape: the house, the day and — once a time is chosen — the light at that hour. The card
 * takes the palette of the chosen hour and its arch casts the shade the sun will cast then.
 */
export function BookingTicket({ branch, date, party, slot, area, occasion, deposit, secondsLeft, holdSeconds, className }: BookingTicketProps) {
  const t = useTranslations('reserve');
  const tu = useTranslations('common.units');
  const locale = useLocale();
  const atmosphere = slot && branch ? computeAtmosphere(new Date(slot.startsAt), branch) : null;
  const style = atmosphere ? (atmosphereCssVars(atmosphere) as CSSProperties) : undefined;

  const rows = [
    branch ? { label: t('fields.branch'), value: branch.name } : null,
    date ? { label: t('fields.date'), value: formatDateString(date, locale, { weekday: 'long', day: 'numeric', month: 'long' }) } : null,
    party ? { label: t('fields.party'), value: tu('guests', plural(party, locale)) } : null,
    slot && area ? { label: t('summary.seating'), value: t(`areas.${area}`) } : null,
    occasion && occasion !== 'none' ? { label: t('summary.occasion'), value: t(`occasions.${occasion}`) } : null,
    deposit > 0 ? { label: t('summary.deposit'), value: formatMoney(deposit, locale) } : null,
  ].filter((r): r is { label: string; value: string } => r !== null);

  return (
    <section
      aria-label={t('summary.title')}
      data-phase={atmosphere?.phase}
      style={style}
      className={cn('flex flex-col gap-6 bg-raised p-6 text-ink transition-[background-color,color] duration-[var(--dur-horizon)] ease-[var(--ease-sun)]', className)}
    >
      {branch?.image ? (
        <div className="arch-4x5 relative aspect-[4/5] w-full max-w-[15rem] self-center overflow-hidden bg-surface cast-shade">
          <Image src={branch.image.src} alt={branch.image.alt} fill sizes="240px" quality={70} placeholder={branch.image.blur ? 'blur' : 'empty'} blurDataURL={branch.image.blur ?? undefined} className="object-cover" style={{ objectPosition: `${branch.image.focalX * 100}% ${branch.image.focalY * 100}%` }} />
        </div>
      ) : null}

      {slot && branch ? (
        <div className="flex flex-col gap-2 text-center">
          <p className="t-display-md tabular">
            <bdi>{formatClock(new Date(slot.startsAt), locale, branch.timeZone)}</bdi>
          </p>
          {atmosphere ? <p className="t-body text-muted">{t(`light.${atmosphere.phase}`)}</p> : null}
        </div>
      ) : (
        <p className="t-body text-center text-muted">{t('summary.empty')}</p>
      )}

      {rows.length ? (
        <dl className="flex flex-col border-t border-line">
          {rows.map((r) => (
            <div key={r.label} className="flex items-baseline justify-between gap-4 border-b border-line py-3">
              <dt className="t-label text-muted">{r.label}</dt>
              <dd className="t-body text-end">{r.value}</dd>
            </div>
          ))}
        </dl>
      ) : null}

      {secondsLeft !== null ? (
        <div className="flex flex-col gap-2" role="timer" aria-live="off">
          <p className="t-small flex items-center gap-2">
            <Icon name="clock" size={16} />
            {t('hold.held', { time: formatCountdown(secondsLeft, locale) })}
          </p>
          <span aria-hidden="true" className="block h-px w-full bg-line">
            <span className="block h-px w-full origin-left bg-accent transition-transform duration-1000 ease-linear rtl:origin-right" style={{ transform: `scaleX(${Math.max(0, Math.min(1, secondsLeft / holdSeconds))})` }} />
          </span>
        </div>
      ) : null}
    </section>
  );
}
