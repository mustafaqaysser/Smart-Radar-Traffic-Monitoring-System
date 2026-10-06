import { openState, type HoursInput } from '@/lib/domain/hours';
import { formatClock, formatDate } from '@/lib/i18n/format';
import { addDays, toDateString } from '@/lib/time/zoned';

export interface OpenStatus {
  open: boolean;
  /** Pre-formatted, translated detail ("Until 1:00 am" / "Opens tomorrow at 7:00 am"). */
  detail: string | null;
}

type Translate = (key: string, values?: Record<string, string>) => string;

/** Open/closed state of a house with a human detail line, in the house's time zone. */
export function describeOpenStatus(hours: HoursInput, now: Date, locale: string, t: Translate): OpenStatus {
  const state = openState(hours, now);
  if (state.open && state.closesAt) return { open: true, detail: t('closesAt', { time: formatClock(state.closesAt, locale, hours.timeZone) }) };
  if (!state.opensAt) return { open: false, detail: null };
  const tz = hours.timeZone;
  const today = toDateString(now, tz);
  const day = toDateString(state.opensAt, tz);
  const time = formatClock(state.opensAt, locale, tz);
  const when =
    day === today
      ? t('opensToday', { time })
      : day === addDays(today, 1)
        ? t('opensTomorrow', { time })
        : t('opensOn', { day: formatDate(state.opensAt, locale, tz, { weekday: 'long' }), time });
  return { open: false, detail: t('opensAt', { when }) };
}
