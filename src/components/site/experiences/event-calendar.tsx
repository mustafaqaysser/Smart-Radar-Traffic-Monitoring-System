import { formatDateString, formatNumber, formatWeekday } from '@/lib/i18n/format';
import { weekdayOf } from '@/lib/time/zoned';
import { cn } from '@/lib/utils/cn';

const pad = (n: number) => String(n).padStart(2, '0');

/**
 * Month grids with the days that have a gathering marked (each links to it in the ledger). Read-only: a way
 * to see the season at a glance.
 */
export function EventCalendar({ months, days, today, locale, label }: { months: string[]; days: Record<string, string>; today: string; locale: string; label: string }) {
  const rtl = locale === 'ar';
  return (
    <nav aria-label={label} className="grid gap-8 sm:grid-cols-2 lg:grid-cols-1">
      {months.map((month) => {
        const [y, m] = month.split('-').map(Number) as [number, number];
        const count = new Date(Date.UTC(y, m, 0)).getUTCDate();
        const lead = weekdayOf(`${month}-01`);
        const cells = [...Array.from({ length: lead }, () => null), ...Array.from({ length: count }, (_, i) => `${month}-${pad(i + 1)}`)];
        return (
          <div key={month} className="flex flex-col gap-3">
            <p className="t-heading-sm">{formatDateString(`${month}-01`, locale, { month: 'long', year: 'numeric' })}</p>
            <div className="grid grid-cols-7 gap-1 text-center">
              {Array.from({ length: 7 }, (_, i) => (
                <span key={`w${i}`} className="t-label pb-1 text-muted" aria-hidden="true">
                  {formatWeekday(i, locale, rtl ? 'narrow' : 'short')}
                </span>
              ))}
              {cells.map((date, i) => {
                if (!date) return <span key={`e${i}`} />;
                const slug = days[date];
                const n = formatNumber(Number(date.slice(8)), locale);
                return slug ? (
                  <a key={date} href={`#event-${slug}`} className="t-small tabular relative flex aspect-square items-center justify-center bg-ink text-bg hover-capable:hover:bg-accent hover-capable:hover:text-on-accent" aria-label={formatDateString(date, locale, { weekday: 'long', day: 'numeric', month: 'long' })}>
                    {n}
                  </a>
                ) : (
                  <span key={date} aria-hidden="true" className={cn('t-small tabular flex aspect-square items-center justify-center', date < today ? 'text-muted/50' : 'text-muted', date === today && 'underline decoration-accent underline-offset-4')}>
                    {n}
                  </span>
                );
              })}
            </div>
          </div>
        );
      })}
    </nav>
  );
}

export function monthsBetween(first: string, last: string): string[] {
  const out: string[] = [];
  let d = `${first.slice(0, 7)}-01`;
  while (d.slice(0, 7) <= last.slice(0, 7)) {
    out.push(d.slice(0, 7));
    const [y, m] = d.split('-').map(Number) as [number, number];
    d = m === 12 ? `${y + 1}-01-01` : `${y}-${pad(m + 1)}-01`;
    if (out.length > 3) break;
  }
  return out;
}

