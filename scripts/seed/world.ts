/**
 * Seeds the static world: media, branches, hours, tables, zones, menus, modifiers, content and settings.
 * IDs are deterministic (e.g. `it_tamees-foul`, `br_al-balad`) so admin links, tests and docs are stable.
 */
import { readFileSync, existsSync } from 'node:fs';
import { and, eq, type SQL } from 'drizzle-orm';
import type { SQLiteColumn } from 'drizzle-orm/sqlite-core';
import { join } from 'node:path';
import * as s from '../../src/lib/db/schema';
import type { LocalizedText } from '../../src/lib/i18n/localized';
import { addDays, localToUtc, parseTime, toDateString } from '../../src/lib/time/zoned';
import { branches as branchSeeds } from './content/branches';
import { items as itemSeeds, menus as menuSeeds, modifierGroups, ramadanMenus } from './content/menu';
import { journalPosts } from './content/journal';
import { faqs } from './content/faq';
import { team } from './content/team';
import { reviews } from './content/reviews';
import { press } from './content/press';
import { jobs } from './content/careers';
import { events } from './content/events';
import { privateRooms, packages } from './content/private-dining';
import { seasonalModes } from './content/seasonal';
import { loyaltyRewards } from './content/loyalty';
import { promotions } from './content/promotions';
import { gallery } from './content/gallery';
import type { Seeder } from './types';

export const ids = {
  branch: (slug: string) => `br_${slug}`,
  item: (slug: string) => `it_${slug}`,
  menu: (slug: string) => `mn_${slug}`,
  category: (menu: string, cat: string) => `mc_${menu}_${cat}`,
  group: (key: string) => `mg_${key}`,
  option: (group: string, key: string) => `mo_${group}_${key}`,
  media: (name: string) => `md_${name}`,
  table: (branch: string, label: string) => `tb_${branch}_${label}`,
  event: (slug: string) => `ev_${slug}`,
  seasonal: (slug: string) => `sm_${slug}`,
};

interface ManifestEntry {
  kind: 'image' | 'video';
  src: string;
  width: number;
  height: number;
  blur: string;
  focal: [number, number];
  alt: LocalizedText;
  credit: s.MediaCredit;
  poster?: string;
  sources?: { src: string; type: string }[];
}

export function loadManifest(): Record<string, ManifestEntry> {
  const path = join(process.cwd(), 'src', 'content', 'media.json');
  return existsSync(path) ? (JSON.parse(readFileSync(path, 'utf8')) as Record<string, ManifestEntry>) : {};
}

const riyals = (sar: number) => Math.round(sar * 100);

/** Next date (from `from`) whose Umm al-Qura month/day match. */
function nextHijri(from: string, month: number, day: number): string {
  const fmt = new Intl.DateTimeFormat('en-u-ca-islamic-umalqura', { timeZone: 'UTC', month: 'numeric', day: 'numeric' });
  for (let i = 0; i < 420; i++) {
    const date = addDays(from, i);
    const [y, m, d] = date.split('-').map(Number) as [number, number, number];
    const parts = Object.fromEntries(fmt.formatToParts(new Date(Date.UTC(y, m - 1, d, 12))).map((p) => [p.type, p.value]));
    if (Number(parts.month) === month && Number(parts.day) === day) return date;
  }
  throw new Error('Hijri date not found');
}

export const seedWorld: Seeder = async ({ db, now, log, warn }) => {
  const today = toDateString(now, 'Asia/Riyadh');
  const manifest = loadManifest();
  const mediaId = (name: string | undefined | null): string | null => {
    if (!name) return null;
    if (!manifest[name]) {
      warn(`media not found in manifest: ${name}`);
      return null;
    }
    return ids.media(name);
  };

  // ——— media ———
  const mediaRows = Object.entries(manifest).map(([name, m]) => ({
    id: ids.media(name),
    src: m.src,
    kind: m.kind,
    width: m.width,
    height: m.height,
    blurDataUrl: m.blur,
    focalX: m.focal[0],
    focalY: m.focal[1],
    alt: m.alt,
    credit: m.credit,
    poster: m.poster ?? null,
    sources: m.sources ?? null,
  }));
  for (let i = 0; i < mediaRows.length; i += 100) await db.insert(s.media).values(mediaRows.slice(i, i + 100));
  log(`media: ${mediaRows.length}`);

  // ——— branches ———
  for (const [index, b] of branchSeeds.entries()) {
    const branchId = ids.branch(b.slug);
    await db.insert(s.branches).values({
      id: branchId,
      slug: b.slug,
      name: b.name,
      shortName: b.shortName,
      city: b.city,
      district: b.district,
      address: b.address,
      story: b.story,
      timeZone: 'Asia/Riyadh',
      lat: b.lat,
      lng: b.lng,
      phone: b.phone,
      whatsapp: b.whatsapp,
      email: b.email,
      parking: b.parking,
      accessibility: b.accessibility,
      heroImageId: mediaId(b.heroImage),
      reservationSettings: b.reservation,
      orderingSettings: b.ordering,
      sortOrder: index,
    });
    const hourRows = [
      ...b.venueHours.flatMap((h) => h.weekdays.map((weekday) => ({ kind: 'venue' as const, weekday, opens: h.opens, closes: h.closes }))),
      ...b.kitchenHours.flatMap((h) => h.weekdays.map((weekday) => ({ kind: 'kitchen' as const, weekday, opens: h.opens, closes: h.closes }))),
    ];
    await db.insert(s.openingHours).values(hourRows.map((h, i) => ({ id: `oh_${b.slug}_${i}`, branchId, ...h })));
    await db.insert(s.servicePeriods).values(
      b.periods.map((p, i) => ({
        id: `sp_${b.slug}_${p.key}`,
        branchId,
        key: p.key,
        name: p.name,
        weekdays: p.weekdays,
        start: p.start,
        end: p.end,
        maxCoversPerSlot: p.maxCovers ?? null,
        sortOrder: i,
      })),
    );
    await db.insert(s.diningTables).values(
      b.tables.map((t, i) => ({
        id: ids.table(b.slug, t.label),
        branchId,
        code: `${b.codePrefix}-${t.label}-${tableSuffix(b.slug, t.label)}`,
        label: t.label,
        area: t.area,
        minSeats: t.min,
        maxSeats: t.max,
        combineGroup: t.group,
        reservable: t.reservable ?? true,
        sortOrder: i,
      })),
    );
    await db.insert(s.deliveryZones).values(
      b.zones.map((z, i) => ({
        id: `dz_${b.slug}_${i}`,
        branchId,
        name: z.name,
        kind: z.kind,
        areas: z.areas ?? null,
        radiusKm: z.radiusKm ?? null,
        fee: riyals(z.fee),
        minOrder: riyals(z.minOrder),
        etaMinutes: z.eta,
        sortOrder: i,
      })),
    );
    if (b.specials.length) {
      await db.insert(s.specialHours).values(
        b.specials.map((sp, i) => ({
          id: `sh_${b.slug}_${i}`,
          branchId,
          date: addDays(today, sp.inDays),
          label: sp.label,
          closed: sp.closed,
          ranges: sp.ranges ?? null,
          reservationsBlocked: sp.reservationsBlocked ?? false,
        })),
      );
    }
  }
  log(`branches: ${branchSeeds.length}`);

  // ——— modifiers ———
  for (const [i, g] of modifierGroups.entries()) {
    await db.insert(s.modifierGroups).values({ id: ids.group(g.key), key: g.key, name: g.name, minSelect: g.min, maxSelect: g.max, sortOrder: i });
    await db.insert(s.modifierOptions).values(
      g.options.map((o, j) => ({ id: ids.option(g.key, o.key), groupId: ids.group(g.key), name: o.name, priceDelta: riyals(o.delta), isDefault: o.default ?? false, sortOrder: j })),
    );
  }

  // ——— items ———
  for (const [i, it] of itemSeeds.entries()) {
    await db.insert(s.menuItems).values({
      id: ids.item(it.slug),
      slug: it.slug,
      name: it.name,
      description: it.description,
      story: it.story ?? null,
      ingredients: it.ingredients ?? null,
      price: riyals(it.price),
      calories: it.calories ?? null,
      spiceLevel: it.spice ?? 0,
      allergens: it.allergens,
      dietary: it.dietary,
      imageId: mediaId(it.image),
      isSignature: it.signature ?? false,
      orderable: it.orderable ?? true,
      upsell: it.upsell ?? false,
      prepMinutes: it.prep ?? 15,
      pairings: it.pairings ?? null,
      tags: it.tags ?? null,
      sortOrder: i,
    });
    for (const b of branchSeeds) {
      const available = !it.branches || it.branches.includes(b.slug);
      await db.insert(s.itemBranches).values({ itemId: ids.item(it.slug), branchId: ids.branch(b.slug), available, soldOut: false });
    }
    if (it.modifiers?.length) {
      await db.insert(s.itemModifierGroups).values(it.modifiers.map((g, j) => ({ itemId: ids.item(it.slug), groupId: ids.group(g), sortOrder: j })));
    }
  }
  // Live sold-out state for the demo: one dish at each house.
  await db.update(s.itemBranches).set({ soldOut: true }).where(sqlEq(s.itemBranches.itemId, ids.item('eggs-east-wall'), s.itemBranches.branchId, ids.branch('al-balad')));
  await db.update(s.itemBranches).set({ soldOut: true }).where(sqlEq(s.itemBranches.itemId, ids.item('octopus-tahini'), s.itemBranches.branchId, ids.branch('wadi-hanifah')));
  log(`items: ${itemSeeds.length}`);

  // ——— seasonal modes (Umm al-Qura dates for Ramadan and Eid) ———
  const ramadanStart = nextHijri(today, 9, 1);
  const ramadanEnd = addDays(nextHijri(ramadanStart, 10, 1), -1);
  const eidStart = addDays(ramadanEnd, 1);
  for (const m of seasonalModes) {
    let start = addDays(today, m.startInDays);
    let end = addDays(today, m.endInDays);
    if (m.kind === 'ramadan') [start, end] = [ramadanStart, ramadanEnd];
    if (m.kind === 'eid') [start, end] = [eidStart, addDays(eidStart, 2)];
    if (m.kind === 'new_year') {
      const year = Number(today.slice(0, 4));
      start = `${year}-12-31`;
      end = `${year}-12-31`;
    }
    const hours =
      m.kind === 'ramadan'
        ? Object.fromEntries(branchSeeds.map((b) => [ids.branch(b.slug), [{ opens: '17:00', closes: '03:30' }]]))
        : null;
    await db.insert(s.seasonalModes).values({ id: ids.seasonal(m.slug), slug: m.slug, kind: m.kind, name: m.name, banner: m.banner, startDate: start, endDate: end, theme: m.theme, hours, isEnabled: m.enabled });
  }

  // ——— menus ———
  const allMenus = [...menuSeeds.map((m) => ({ menu: m, seasonal: null as string | null })), ...ramadanMenus.map((m) => ({ menu: m, seasonal: ids.seasonal('ramadan') }))];
  for (const [i, { menu, seasonal }] of allMenus.entries()) {
    const menuId = ids.menu(menu.slug);
    await db.insert(s.menus).values({
      id: menuId,
      slug: menu.slug,
      kind: menu.kind,
      name: menu.name,
      hour: menu.hour,
      description: menu.description,
      schedule: menu.schedule,
      branchIds: null,
      seasonalModeId: seasonal,
      isTasting: menu.isTasting ?? false,
      tastingPrice: menu.tastingPrice ? riyals(menu.tastingPrice) : null,
      imageId: mediaId(menu.image),
      sortOrder: i,
    });
    for (const [j, cat] of menu.categories.entries()) {
      const categoryId = ids.category(menu.slug, cat.slug);
      await db.insert(s.menuCategories).values({ id: categoryId, menuId, slug: cat.slug, name: cat.name, description: cat.description ?? null, sortOrder: j });
      await db.insert(s.categoryItems).values(cat.items.map((slug, k) => ({ categoryId, itemId: ids.item(slug), sortOrder: k })));
    }
  }
  log(`menus: ${allMenus.length}`);

  // ——— editorial content ———
  await db.insert(s.journalPosts).values(
    journalPosts.map((p) => ({
      id: `jp_${p.slug}`,
      slug: p.slug,
      kind: p.kind,
      title: p.title,
      excerpt: p.excerpt,
      body: p.body,
      coverImageId: mediaId(p.cover),
      author: p.author,
      readingMinutes: p.readingMinutes,
      recipe: p.recipe ?? null,
      publishedAt: new Date(now.getTime() - p.publishedDaysAgo * 864e5),
    })),
  );
  await db.insert(s.faqs).values(faqs.map((f, i) => ({ id: `fq_${i}`, category: f.category, question: f.question, answer: f.answer, sortOrder: i })));
  await db.insert(s.teamMembers).values(
    team.map((m, i) => ({ id: `tm_${i}`, name: m.name, role: m.role, bio: m.bio, imageId: mediaId(m.image), branchId: m.branch ? ids.branch(m.branch) : null, sortOrder: i })),
  );
  await db.insert(s.pressItems).values(press.map((p, i) => ({ id: `pr_${i}`, kind: p.kind, publication: p.publication, title: p.title, quote: p.quote, year: p.year, sortOrder: i })));
  await db.insert(s.jobPostings).values(
    jobs.map((j) => ({ id: `jb_${j.slug}`, slug: j.slug, title: j.title, branchId: j.branch ? ids.branch(j.branch) : null, employment: j.employment, summary: j.summary, description: j.description })),
  );
  await db.insert(s.reviews).values(
    reviews.map((r, i) => ({
      id: `rv_${i}`,
      branchId: ids.branch(r.branch),
      name: r.name,
      email: `${r.name.split(' ')[0]?.toLowerCase().replace(/[^a-z]/g, '') || 'guest'}.${i}@guest.test`,
      rating: r.rating,
      title: r.title ?? null,
      body: r.body,
      locale: r.locale,
      visitDate: addDays(today, -r.daysAgo - 1),
      status: r.status,
      response: r.response ?? null,
      featured: r.featured ?? false,
      createdAt: new Date(now.getTime() - r.daysAgo * 864e5),
    })),
  );
  await db.insert(s.galleryItems).values(
    gallery.filter((g) => manifest[g.media]).map((g, i) => ({ id: `gl_${i}`, mediaId: ids.media(g.media), caption: g.caption, hour: g.hour, sortOrder: i })),
  );
  log(`content: ${journalPosts.length} posts, ${faqs.length} faqs, ${team.length} team, ${reviews.length} reviews, ${press.length} press, ${jobs.length} jobs`);

  // ——— events ———
  for (const e of events) {
    const date = addDays(today, e.daysFromNow);
    const startsAt = localToUtc(date, parseTime(e.startTime), 'Asia/Riyadh', true) as Date;
    await db.insert(s.events).values({
      id: ids.event(e.slug),
      slug: e.slug,
      kind: e.kind,
      title: e.title,
      summary: e.summary,
      body: e.body,
      branchId: ids.branch(e.branch),
      startsAt,
      endsAt: new Date(startsAt.getTime() + e.durationMinutes * 60000),
      capacity: e.capacity,
      imageId: mediaId(e.image),
    });
    await db.insert(s.eventTicketTypes).values(
      e.tickets.map((t, i) => ({ id: `et_${e.slug}_${i}`, eventId: ids.event(e.slug), name: t.name, description: t.description ?? null, price: t.price, capacity: t.capacity ?? null, sortOrder: i })),
    );
  }

  // ——— private dining ———
  await db.insert(s.privateRooms).values(
    privateRooms.map((r, i) => ({ id: `pd_${r.slug}`, slug: r.slug, branchId: ids.branch(r.branch), name: r.name, description: r.description, seated: r.seated, standing: r.standing, features: r.features, imageId: mediaId(r.image), sortOrder: i })),
  );
  await db.insert(s.privatePackages).values(packages.map((p, i) => ({ id: `pk_${i}`, name: p.name, description: p.description, pricePerGuest: p.pricePerGuest, minGuests: p.minGuests, kind: p.kind, sortOrder: i })));

  // ——— loyalty & promotions ———
  await db.insert(s.loyaltyRewards).values(
    loyaltyRewards.map((r, i) => ({ id: `lr_${i}`, name: r.name, description: r.description, pointsCost: r.pointsCost, value: r.value, minTier: r.minTier, sortOrder: i })),
  );
  await db.insert(s.promotions).values(
    promotions.map((p, i) => ({
      id: `pm_${i}`,
      code: p.code,
      description: p.description,
      kind: p.kind,
      value: p.value,
      maxDiscount: p.maxDiscount,
      minOrder: p.minOrder,
      startsAt: p.startsInDays === null ? null : new Date(now.getTime() + p.startsInDays * 864e5),
      endsAt: p.endsInDays === null ? null : new Date(now.getTime() + p.endsInDays * 864e5),
      usageLimit: p.usageLimit,
      perCustomerLimit: p.perCustomerLimit,
      firstOrderOnly: p.firstOrderOnly,
      branchIds: p.branches ? p.branches.map(ids.branch) : null,
      channels: p.channels,
      isActive: p.active,
    })),
  );
  log(`events: ${events.length}, rooms: ${privateRooms.length}, rewards: ${loyaltyRewards.length}, promos: ${promotions.length}`);
};

/** Stable, non-guessable-looking table code suffix. */
function tableSuffix(branch: string, label: string): string {
  const alphabet = '23456789ABCDEFGHJKMNPQRSTUVWXYZ';
  let h = 2166136261;
  for (const ch of `${branch}:${label}:zill`) h = Math.imul(h ^ ch.charCodeAt(0), 16777619) >>> 0;
  let out = '';
  for (let i = 0; i < 4; i++) {
    out += alphabet[h % alphabet.length];
    h = Math.floor(h / alphabet.length) + 7919 * (i + 1);
  }
  return out;
}

function sqlEq(c1: SQLiteColumn, v1: string, c2: SQLiteColumn, v2: string): SQL {
  return and(eq(c1, v1), eq(c2, v2)) as SQL;
}
