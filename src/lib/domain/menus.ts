/**
 * Which menus are served when. Pure functions; all wall-clock values are in the branch's time zone.
 * A window whose end is not after its start runs past midnight and belongs to the day it started.
 */
import { addDays, getZonedParts, parseTime, toDateString, weekdayOf } from '../time/zoned';

export interface MenuWindow {
  weekdays: number[];
  start: string;
  end: string;
}

export interface ScheduledMenu {
  id: string;
  slug: string;
  schedule: MenuWindow[];
  branchIds: string[] | null;
  seasonalModeId: string | null;
}

export interface SeasonWindow {
  id: string;
  startDate: string;
  endDate: string;
  isEnabled: boolean;
}

/** Seasonal modes active on a local date. */
export function activeSeasons<T extends SeasonWindow>(modes: T[], date: string): T[] {
  return modes.filter((m) => m.isEnabled && m.startDate <= date && date <= m.endDate);
}

/** Whether a menu exists at all for this branch and season (independent of the hour). */
export function menuAvailable(menu: ScheduledMenu, branchId: string | null, activeSeasonIds: ReadonlySet<string>): boolean {
  if (branchId && menu.branchIds && !menu.branchIds.includes(branchId)) return false;
  if (menu.seasonalModeId && !activeSeasonIds.has(menu.seasonalModeId)) return false;
  return true;
}

function windowSpan(w: MenuWindow): { start: number; end: number } {
  const start = parseTime(w.start);
  let end = parseTime(w.end);
  if (end <= start) end += 1440;
  return { start, end };
}

/** True when the menu is being served at the instant (an empty schedule means all day). */
export function isServing(menu: Pick<ScheduledMenu, 'schedule'>, now: Date, timeZone: string): boolean {
  if (menu.schedule.length === 0) return true;
  const p = getZonedParts(now, timeZone);
  const today = toDateString(now, timeZone);
  const minutes = p.hour * 60 + p.minute;
  const candidates: { weekday: number; at: number }[] = [
    { weekday: weekdayOf(today), at: minutes },
    { weekday: weekdayOf(addDays(today, -1)), at: minutes + 1440 },
  ];
  return menu.schedule.some((w) => {
    const span = windowSpan(w);
    return candidates.some((c) => w.weekdays.includes(c.weekday) && span.start <= c.at && c.at < span.end);
  });
}

/** Minutes until the menu next starts (within a week), or null. 0 when serving now. */
export function minutesUntilServing(menu: Pick<ScheduledMenu, 'schedule'>, now: Date, timeZone: string): number | null {
  if (isServing(menu, now, timeZone)) return 0;
  if (menu.schedule.length === 0) return 0;
  const p = getZonedParts(now, timeZone);
  const today = toDateString(now, timeZone);
  const nowMinutes = p.hour * 60 + p.minute;
  let best: number | null = null;
  for (let d = 0; d <= 7; d++) {
    const weekday = weekdayOf(addDays(today, d));
    for (const w of menu.schedule) {
      if (!w.weekdays.includes(weekday)) continue;
      const delta = d * 1440 + windowSpan(w).start - nowMinutes;
      if (delta > 0 && (best === null || delta < best)) best = delta;
    }
  }
  return best;
}

/** The menus served right now for a branch, in their given order. */
export function servingNow<T extends ScheduledMenu>(menus: T[], branchId: string | null, now: Date, timeZone: string, activeSeasonIds: ReadonlySet<string>): T[] {
  return menus.filter((m) => menuAvailable(m, branchId, activeSeasonIds) && isServing(m, now, timeZone));
}
