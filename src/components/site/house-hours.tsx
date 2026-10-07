import { getTranslations } from 'next-intl/server';
import { weekAhead, type HoursInput } from '@/lib/domain/hours';
import { formatDateString, formatWallTime } from '@/lib/i18n/format';
import { tr } from '@/lib/i18n/localized';
import type { BranchDTO } from '@/lib/queries/types';
import { formatTime } from '@/lib/time/zoned';
import { cn } from '@/lib/utils/cn';

/** Seven days of opening hours from today, in the house's time zone, with holidays and seasons marked. */
export async function HouseHours({ branch, hours, today, locale, days = 7 }: { branch: BranchDTO; hours: HoursInput; today: string; locale: string; days?: number }) {
  const t = await getTranslations('visit.locations');
  const week = weekAhead(hours, today, days);
  return (
    <table className="t-small w-full">
      <caption className="sr-only">{t('weekTitle')}</caption>
      <tbody>
        {week.map((d) => {
          const special = d.special ? branch.specials.find((s) => s.date === d.date) : undefined;
          return (
            <tr key={d.date} className={cn('border-b border-line align-top', d.date === today && 'font-semibold')}>
              <th scope="row" className="py-3 pe-4 text-start font-[inherit]">
                {d.date === today ? `${t('today')} · ` : ''}
                {formatDateString(d.date, locale, { weekday: 'long', day: 'numeric', month: 'short' })}
                {special ? <span className="block text-accent">{tr(special.name, locale)}</span> : null}
                {d.seasonal && !special ? <span className="block text-muted">{t('seasonal')}</span> : null}
              </th>
              <td className="py-3 text-end">
                {d.closed
                  ? t('closed')
                  : d.ranges.map((r) => (
                      <bdi key={r.start} className="block tabular">
                        {formatWallTime(formatTime(r.start % 1440), locale)} – {formatWallTime(formatTime(r.end % 1440), locale)}
                      </bdi>
                    ))}
                {special?.reservationsBlocked ? <span className="block text-muted">{t('noReservations')}</span> : null}
              </td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}
