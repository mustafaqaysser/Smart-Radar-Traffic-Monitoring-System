import 'server-only';
import { and, eq } from 'drizzle-orm';
import restaurantConfig from '@config';
import { db } from '@/lib/db/client';
import { diningTables } from '@/lib/db/schema';
import { scheduleForDate } from '@/lib/domain/hours';
import { tr } from '@/lib/i18n/localized';
import { imageView } from '@/lib/menu/view';
import { getBranches, getSeasonalModes, hoursFor } from '@/lib/queries/branches';
import { getMediaIndex } from '@/lib/queries/catalog';
import type { DayState, FlowSetup } from '@/lib/reserve/types';
import { featureEnabled } from '@/lib/server/settings';
import { addDays, weekdayOf } from '@/lib/time/zoned';
import { bookingWindow, DEPOSIT_WINDOW_MINUTES } from './reservations';

export async function reserveSetup(locale: string, now = new Date()): Promise<FlowSetup> {
  const [branches, modes, media, privateDining] = await Promise.all([getBranches(), getSeasonalModes(), getMediaIndex(), featureEnabled('privateDining')]);
  const open = branches.filter((b) => b.reservationsEnabled);
  const tables = await db
    .select({ branchId: diningTables.branchId, area: diningTables.area })
    .from(diningTables)
    .where(and(eq(diningTables.reservable, true), eq(diningTables.isActive, true)));
  const fallbackImage = media['place-courtyard-sun'] ?? null;

  return {
    branches: open.map((b) => {
      const { first, last } = bookingWindow(b, now);
      const hours = hoursFor(b, modes);
      const days: Record<string, DayState> = {};
      const notes: Record<string, string> = {};
      for (let d = first; d <= last; d = addDays(d, 1)) {
        const schedule = scheduleForDate(hours, d);
        const special = b.specials.find((sp) => sp.date === d);
        if (special) notes[d] = tr(special.name, locale);
        const weekday = weekdayOf(d);
        if (schedule.closed || !b.periods.some((p) => p.weekdays.includes(weekday))) days[d] = 'closed';
        else if (special?.reservationsBlocked) days[d] = 'blocked';
        else days[d] = 'open';
      }
      const areaOrder = ['courtyard', 'liwan', 'roof', 'private'];
      return {
        slug: b.slug,
        name: tr(b.name, locale),
        shortName: tr(b.shortName, locale),
        city: tr(b.city, locale),
        address: tr(b.address, locale),
        phone: b.phone,
        timeZone: b.timeZone,
        lat: b.lat,
        lng: b.lng,
        image: imageView(b.hero ?? fallbackImage, locale),
        periods: b.periods.map((p) => ({ key: p.key, name: tr(p.name, locale) })),
        areas: [...new Set(tables.filter((t) => t.branchId === b.id).map((t) => t.area))].sort((x, y) => areaOrder.indexOf(x) - areaOrder.indexOf(y)),
        first,
        last,
        days,
        notes,
      };
    }),
    maxPartyOnline: restaurantConfig.reservations.maxPartyOnline,
    deposit: { minParty: restaurantConfig.reservations.deposit.minParty, perGuest: restaurantConfig.reservations.deposit.perGuest * 100 },
    holdMinutes: restaurantConfig.reservations.holdMinutes,
    cutoffHours: restaurantConfig.reservations.modifyCutoffHours,
    bookingWindowDays: restaurantConfig.reservations.bookingWindowDays,
    depositWindowMinutes: DEPOSIT_WINDOW_MINUTES,
    privateDining,
  };
}
