import 'server-only';
import { isServing } from '@/lib/domain/menus';
import { formatWallTime } from '@/lib/i18n/format';
import { tr } from '@/lib/i18n/localized';
import { itemView, visibleItems, type ItemView } from '@/lib/menu/view';
import { getMenuCatalog } from '@/lib/queries/catalog';
import type { BranchDTO } from '@/lib/queries/types';
import { getSettings } from '@/lib/server/settings';
import { getServingContext } from '@/lib/site/serving';
import { weekdayOf } from '@/lib/time/zoned';
import type { CheckoutBranch } from './types';

export interface OrderMenu {
  slug: string;
  name: string;
  /** Today's serving windows, formatted ("12:30 pm–3:30 pm"), or null for all day. */
  window: string | null;
  servingNow: boolean;
  categories: { slug: string; name: string; items: string[] }[];
}

export interface OrderMenuData {
  menus: OrderMenu[];
  /** Every orderable dish (so a basket line from another hour still has a name and price). */
  items: Record<string, ItemView>;
  /** Dishes suggested to complete a meal (drinks, sweets). */
  upsell: string[];
}

/** The menus a house can take orders from today, in the visitor's language. */
export async function orderMenuData(branch: BranchDTO, locale: string): Promise<OrderMenuData> {
  const [catalog, settings, serving] = await Promise.all([getMenuCatalog(), getSettings(), getServingContext(branch)]);
  const orderable = visibleItems(catalog, settings.features.alcohol).filter((i) => i.orderable);
  const items: Record<string, ItemView> = {};
  for (const item of orderable) items[item.slug] = itemView(item, catalog, branch.id, locale);
  const weekday = weekdayOf(serving.today);
  const menus: OrderMenu[] = serving.menus
    .filter((m) => !m.isTasting)
    .filter((m) => m.schedule.length === 0 || m.schedule.some((w) => w.weekdays.includes(weekday)))
    .map((m) => {
      const windows = m.schedule.filter((w) => w.weekdays.includes(weekday)).map((w) => `${formatWallTime(w.start, locale)}–${formatWallTime(w.end, locale)}`);
      return {
        slug: m.slug,
        name: tr(m.name, locale),
        window: windows.length ? windows.join(' · ') : null,
        servingNow: isServing(m, serving.now, branch.timeZone),
        categories: m.categories.map((c) => ({ slug: `${m.slug}-${c.slug}`, name: tr(c.name, locale), items: c.items.filter((slug) => items[slug]) })).filter((c) => c.items.length > 0),
      };
    })
    .filter((m) => m.categories.length > 0)
    // What is being served now comes first.
    .sort((a, b) => Number(b.servingNow) - Number(a.servingNow));
  return { menus, items, upsell: orderable.filter((i) => i.upsell).map((i) => i.slug) };
}

export async function checkoutBranch(branch: BranchDTO, locale: string): Promise<CheckoutBranch> {
  const settings = await getSettings();
  return {
    slug: branch.slug,
    name: tr(branch.name, locale),
    shortName: tr(branch.shortName, locale),
    address: tr(branch.address, locale),
    phone: branch.phone,
    lat: branch.lat,
    lng: branch.lng,
    timeZone: branch.timeZone,
    channels: { delivery: settings.features.delivery && branch.deliveryEnabled, pickup: settings.features.pickup && branch.pickupEnabled },
    zones: branch.zones.map((z) => ({ id: z.id, name: tr(z.name, locale), kind: z.kind, areas: z.areas.map((a) => tr(a, locale)), radiusKm: z.radiusKm, fee: z.fee, minOrder: z.minOrder, etaMinutes: z.etaMinutes })),
  };
}
