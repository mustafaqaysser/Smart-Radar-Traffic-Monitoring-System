import Link from 'next/link';
import { formatDateString, formatNumber, formatWeekday } from '@/lib/i18n/format';
import { addDays, weekdayOf } from '@/lib/time/zoned';
import { cn } from '@/lib/utils/cn';

/**
 * A month of bookings: each day shows its bookings and covers, shaded by how full it is; a day opens its
 * timeline. Weeks start on Sunday, as the working week does here.
 */
export function MonthCalendar({ month, counts, today, locale, labels, branchParam }: { month: string; counts: Record<string, { bookings: number; covers: number }>; today: string; locale: string; labels: { bookings: (n: string) => string; covers: (n: string) => string; weekdays: string }; branchParam: string }) {
  const first = `${month}-01`;
  const lead = weekdayOf(first);
  const [y, m] = month.split('-').map(Number) as [number, number];
  const daysInMonth = new Date(Date.UTC(y, m, 0)).getUTCDate();
  const cells = Array.from({ length: Math.ceil((lead + daysInMonth) / 7) * 7 }, (_, i) => (i < lead || i >= lead + daysInMonth ? null : addDays(first, i - lead)));
  const max = Math.max(1, ...Object.values(counts).map((c) => c.covers));
  return (
    <div className="overflow-x-auto">
      <table className="w-full min-w-[42rem] table-fixed border-collapse">
        <caption className="sr-only">{formatDateString(first, locale, { month: 'long', year: 'numeric' })}</caption>
        <thead>
          <tr>
            {Array.from({ length: 7 }, (_, d) => (
              <th key={d} scope="col" abbr={formatWeekday(d, locale, 'long')} className="px-2 pb-2 text-start text-xs font-medium text-muted">
                {formatWeekday(d, locale, 'short')}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {Array.from({ length: cells.length / 7 }, (_, w) => (
            <tr key={w}>
              {cells.slice(w * 7, w * 7 + 7).map((date, i) => {
                if (!date) return <td key={i} className="border border-line bg-surface/40" />;
                const c = counts[date];
                const share = c ? c.covers / max : 0;
                return (
                  <td key={date} className="border border-line p-0 align-top">
                    <Link
                      href={`/admin/reservations?view=timeline&date=${date}${branchParam}`}
                      className={cn('flex h-24 flex-col gap-0.5 p-2 no-underline transition-colors hover-capable:hover:bg-surface', date === today && 'outline-2 -outline-offset-2 outline-accent', date < today && 'text-muted')}
                      style={c ? { backgroundColor: `color-mix(in oklab, var(--c-accent) ${Math.round(4 + share * 22)}%, transparent)` } : undefined}
                    >
                      <span className="text-[0.8125rem] font-semibold tabular">{formatNumber(Number(date.slice(8)), locale)}</span>
                      {c ? (
                        <>
                          <span className="text-xs">{labels.bookings(formatNumber(c.bookings, locale))}</span>
                          <span className="text-xs text-muted">{labels.covers(formatNumber(c.covers, locale))}</span>
                        </>
                      ) : null}
                    </Link>
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
