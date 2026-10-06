import 'server-only';
import { asc } from 'drizzle-orm';
import { db } from '@/lib/db/client';
import * as s from '@/lib/db/schema';
import { cached, TAGS } from './cache';
import { mediaMap } from './media';
import type { BranchItemState, MediaDTO, MenuCatalog, MenuDTO, MenuItemDTO, ModifierGroupDTO } from './types';

async function loadCatalog(): Promise<MenuCatalog> {
  const [menus, categories, links, items, itemBranches, groups, options, itemGroups, mediaRows, branches] = await Promise.all([
    db.select().from(s.menus).orderBy(asc(s.menus.sortOrder)),
    db.select().from(s.menuCategories).orderBy(asc(s.menuCategories.sortOrder)),
    db.select().from(s.categoryItems).orderBy(asc(s.categoryItems.sortOrder)),
    db.select().from(s.menuItems).orderBy(asc(s.menuItems.sortOrder)),
    db.select().from(s.itemBranches),
    db.select().from(s.modifierGroups).orderBy(asc(s.modifierGroups.sortOrder)),
    db.select().from(s.modifierOptions).orderBy(asc(s.modifierOptions.sortOrder)),
    db.select().from(s.itemModifierGroups).orderBy(asc(s.itemModifierGroups.sortOrder)),
    db.select().from(s.media),
    db.select({ id: s.branches.id }).from(s.branches),
  ]);
  const mediaById = mediaMap(mediaRows);

  const groupsById = new Map<string, ModifierGroupDTO>();
  for (const g of groups) {
    groupsById.set(g.id, {
      id: g.id,
      key: g.key,
      name: g.name,
      minSelect: g.minSelect,
      maxSelect: g.maxSelect,
      options: options
        .filter((o) => o.groupId === g.id)
        .map((o) => ({ id: o.id, name: o.name, priceDelta: o.priceDelta, isDefault: o.isDefault, isAvailable: o.isAvailable })),
    });
  }

  const itemById = new Map<string, MenuItemDTO>();
  const bySlug: Record<string, MenuItemDTO> = {};
  for (const it of items) {
    const perBranch: Record<string, BranchItemState> = {};
    for (const b of branches) perBranch[b.id] = { available: true, soldOut: false, price: it.price };
    for (const ib of itemBranches) {
      if (ib.itemId !== it.id) continue;
      perBranch[ib.branchId] = { available: ib.available, soldOut: ib.soldOut, price: ib.priceOverride ?? it.price };
    }
    const dto: MenuItemDTO = {
      id: it.id,
      slug: it.slug,
      name: it.name,
      description: it.description,
      story: it.story,
      ingredients: it.ingredients,
      price: it.price,
      calories: it.calories,
      spiceLevel: it.spiceLevel,
      allergens: it.allergens,
      dietary: it.dietary,
      image: it.imageId ? (mediaById.get(it.imageId) ?? null) : null,
      isSignature: it.isSignature,
      isAlcoholic: it.isAlcoholic,
      orderable: it.orderable,
      upsell: it.upsell,
      prepMinutes: it.prepMinutes,
      pairings: it.pairings ?? [],
      tags: it.tags ?? [],
      modifierGroups: itemGroups
        .filter((l) => l.itemId === it.id)
        .map((l) => groupsById.get(l.groupId))
        .filter((g): g is ModifierGroupDTO => Boolean(g)),
      branches: perBranch,
      isActive: it.isActive,
    };
    itemById.set(it.id, dto);
    bySlug[it.slug] = dto;
  }

  const menuDTOs: MenuDTO[] = menus
    .filter((m) => m.isActive)
    .map((m) => ({
      id: m.id,
      slug: m.slug,
      kind: m.kind,
      name: m.name,
      hour: m.hour,
      description: m.description,
      schedule: m.schedule,
      branchIds: m.branchIds,
      seasonalModeId: m.seasonalModeId,
      isTasting: m.isTasting,
      tastingPrice: m.tastingPrice,
      image: m.imageId ? (mediaById.get(m.imageId) ?? null) : null,
      categories: categories
        .filter((c) => c.menuId === m.id)
        .map((c) => ({
          id: c.id,
          slug: c.slug,
          name: c.name,
          description: c.description,
          items: links
            .filter((l) => l.categoryId === c.id)
            .map((l) => itemById.get(l.itemId))
            .filter((i): i is MenuItemDTO => Boolean(i && i.isActive))
            .map((i) => i.slug),
        })),
    }));

  return { menus: menuDTOs, items: bySlug };
}

/** The whole menu catalogue (menus → categories → item slugs, items keyed by slug). */
export const getMenuCatalog = cached(loadCatalog, 'menu-catalog', [TAGS.catalog]);

async function loadMediaIndex(): Promise<Record<string, MediaDTO>> {
  const rows = await db.select().from(s.media);
  const out: Record<string, MediaDTO> = {};
  for (const [id, dto] of mediaMap(rows)) out[id.replace(/^md_/, '')] = dto;
  return out;
}

/** Every catalogued photograph and video, keyed by its media name (e.g. 'place-arch-shadow'). */
export const getMediaIndex = cached(loadMediaIndex, 'media-index', [TAGS.catalog]);
