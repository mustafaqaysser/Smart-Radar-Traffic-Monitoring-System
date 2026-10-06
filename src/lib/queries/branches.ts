import 'server-only';
import { asc, eq } from 'drizzle-orm';
import { cache } from 'react';
import restaurantConfig from '@config';
import { db } from '@/lib/db/client';
import * as s from '@/lib/db/schema';
import type { HoursInput } from '@/lib/domain/hours';
import { cached, TAGS } from './cache';
import { toMediaDTO } from './media';
import type { BranchDTO, SeasonalModeDTO } from './types';

async function loadBranches(): Promise<BranchDTO[]> {
  const [rows, hours, specials, periods, zones, mediaRows] = await Promise.all([
    db.select().from(s.branches).where(eq(s.branches.isActive, true)).orderBy(asc(s.branches.sortOrder)),
    db.select().from(s.openingHours),
    db.select().from(s.specialHours),
    db.select().from(s.servicePeriods).orderBy(asc(s.servicePeriods.sortOrder)),
    db.select().from(s.deliveryZones).where(eq(s.deliveryZones.isActive, true)).orderBy(asc(s.deliveryZones.sortOrder)),
    db.select().from(s.media),
  ]);
  return rows.map((b) => ({
    id: b.id,
    slug: b.slug,
    name: b.name,
    shortName: b.shortName,
    city: b.city,
    district: b.district,
    address: b.address,
    story: b.story,
    timeZone: b.timeZone,
    lat: b.lat,
    lng: b.lng,
    phone: b.phone,
    whatsapp: b.whatsapp,
    email: b.email,
    parking: b.parking,
    accessibility: b.accessibility,
    hero: toMediaDTO(mediaRows.find((m) => m.id === b.heroImageId)),
    reservationSettings: b.reservationSettings,
    orderingSettings: b.orderingSettings,
    reservationsEnabled: b.reservationsEnabled,
    deliveryEnabled: b.deliveryEnabled,
    pickupEnabled: b.pickupEnabled,
    dineInEnabled: b.dineInEnabled,
    orderingPaused: b.orderingPaused,
    busyMode: b.busyMode,
    venueHours: hours.filter((h) => h.branchId === b.id && h.kind === 'venue').map((h) => ({ weekday: h.weekday, opens: h.opens, closes: h.closes })),
    kitchenHours: hours.filter((h) => h.branchId === b.id && h.kind === 'kitchen').map((h) => ({ weekday: h.weekday, opens: h.opens, closes: h.closes })),
    specials: specials
      .filter((sp) => sp.branchId === b.id)
      .map((sp) => ({ date: sp.date, closed: sp.closed, ranges: sp.ranges, label: sp.label.en ?? sp.label.ar ?? '', reservationsBlocked: sp.reservationsBlocked })),
    periods: periods
      .filter((p) => p.branchId === b.id)
      .map((p) => ({ key: p.key, name: p.name, weekdays: p.weekdays, start: p.start, end: p.end, maxCoversPerSlot: p.maxCoversPerSlot })),
    zones: zones
      .filter((z) => z.branchId === b.id)
      .map((z) => ({ id: z.id, name: z.name, kind: z.kind, areas: z.areas ?? [], radiusKm: z.radiusKm, fee: z.fee, minOrder: z.minOrder, etaMinutes: z.etaMinutes })),
  }));
}

export const getBranches = cached(loadBranches, 'branches', [TAGS.branches]);

export const getBranch = cache(async (slugOrId: string): Promise<BranchDTO | null> => {
  const all = await getBranches();
  return all.find((b) => b.slug === slugOrId || b.id === slugOrId) ?? null;
});

/** The flagship (or first) branch — the default sun for the atmosphere and the default location. */
export const getFlagshipBranch = cache(async (): Promise<BranchDTO | null> => {
  const all = await getBranches();
  return all.find((b) => b.slug === restaurantConfig.flagshipBranchSlug) ?? all[0] ?? null;
});

async function loadSeasonalModes(): Promise<SeasonalModeDTO[]> {
  const rows = await db.select().from(s.seasonalModes).orderBy(asc(s.seasonalModes.startDate));
  return rows.map((m) => ({
    id: m.id,
    slug: m.slug,
    kind: m.kind,
    name: m.name,
    banner: m.banner,
    startDate: m.startDate,
    endDate: m.endDate,
    theme: m.theme,
    hours: m.hours,
    isEnabled: m.isEnabled,
  }));
}

export const getSeasonalModes = cached(loadSeasonalModes, 'seasonal-modes', [TAGS.seasonal]);

/** Hours input for a branch: weekly hours, specials and any enabled seasonal replacement hours. */
export function hoursFor(branch: BranchDTO, modes: SeasonalModeDTO[], kind: 'venue' | 'kitchen' = 'venue'): HoursInput {
  return {
    timeZone: branch.timeZone,
    weekly: kind === 'venue' ? branch.venueHours : branch.kitchenHours,
    specials: branch.specials,
    seasonal: modes
      .filter((m) => m.isEnabled && m.hours?.[branch.id])
      .map((m) => ({ startDate: m.startDate, endDate: m.endDate, ranges: m.hours?.[branch.id] ?? [] })),
  };
}
