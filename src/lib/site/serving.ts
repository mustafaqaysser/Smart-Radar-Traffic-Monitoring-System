import 'server-only';
import { cache } from 'react';
import { activeSeasons, servingNow } from '@/lib/domain/menus';
import { tr } from '@/lib/i18n/localized';
import { getMenuCatalog } from '@/lib/queries/catalog';
import { getSeasonalModes } from '@/lib/queries/branches';
import type { BranchDTO, MenuDTO, SeasonalModeDTO } from '@/lib/queries/types';
import { toDateString } from '@/lib/time/zoned';

export interface ServingContext {
  now: Date;
  today: string;
  seasons: SeasonalModeDTO[];
  activeSeasonIds: Set<string>;
  menus: MenuDTO[];
  serving: MenuDTO[];
}

/** Menus available at a house today and the ones being served right now (house time zone). */
export const getServingContext = cache(async (branch: BranchDTO | null): Promise<ServingContext> => {
  const [catalog, modes] = await Promise.all([getMenuCatalog(), getSeasonalModes()]);
  const now = new Date();
  const tz = branch?.timeZone ?? 'Asia/Riyadh';
  const today = toDateString(now, tz);
  const seasons = activeSeasons(modes, today);
  const activeSeasonIds = new Set(seasons.map((s) => s.id));
  const branchId = branch?.id ?? null;
  const menus = catalog.menus.filter((m) => (!branchId || !m.branchIds || m.branchIds.includes(branchId)) && (!m.seasonalModeId || activeSeasonIds.has(m.seasonalModeId)));
  // Seasonal menus replace the regular day while their season is on (e.g. iftar and suhoor in Ramadan).
  const seasonal = menus.filter((m) => m.seasonalModeId);
  const pool = seasonal.length ? [...seasonal, ...menus.filter((m) => !m.seasonalModeId && m.schedule.length === 0)] : menus;
  return { now, today, seasons, activeSeasonIds, menus: pool, serving: servingNow(pool, branchId, now, tz, activeSeasonIds) };
});

/** Names of the menus being served now, excluding all-day lists (drinks, children) unless nothing else is on. */
export function servingNames(ctx: ServingContext, locale: string): string[] {
  const timed = ctx.serving.filter((m) => m.schedule.length > 0);
  return (timed.length ? timed : []).map((m) => tr(m.hour ?? m.name, locale));
}
