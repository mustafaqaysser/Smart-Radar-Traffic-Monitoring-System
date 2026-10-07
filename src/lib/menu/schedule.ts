import type { MenuWindow } from '@/lib/db/schema';
import { formatList, formatWallTime, formatWeekday } from '@/lib/i18n/format';

/** "Every day · 7:00 am – 11:30 am" / "Wednesday and Thursday · 7:30 pm – 10:00 pm", one line per window. */
export function describeSchedule(windows: MenuWindow[], locale: string, everyDay: string): string[] {
  return windows.map((w) => {
    const days = w.weekdays.length === 7 ? everyDay : formatList([...w.weekdays].sort((a, b) => a - b).map((d) => formatWeekday(d, locale)), locale);
    return `${days} · ${formatWallTime(w.start, locale)} – ${formatWallTime(w.end, locale)}`;
  });
}
