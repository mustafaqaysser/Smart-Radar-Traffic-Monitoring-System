import type { Allergen, DietaryTag, MenuKind, MenuWindow } from '@/lib/db/schema';
import { normalizeForSearch } from '@/lib/i18n/arabic';
import { tr } from '@/lib/i18n/localized';
import type { MediaDTO, MenuCatalog, MenuDTO, MenuItemDTO } from '@/lib/queries/types';

/** Locale-resolved, serialisable shapes sent to the client menu (only what the page renders). */

export interface ImageView {
  src: string;
  width: number;
  height: number;
  blur: string | null;
  focalX: number;
  focalY: number;
  alt: string;
}

export interface ModifierOptionView {
  id: string;
  name: string;
  priceDelta: number;
  isDefault: boolean;
  isAvailable: boolean;
}

export interface ModifierGroupView {
  id: string;
  name: string;
  minSelect: number;
  maxSelect: number;
  options: ModifierOptionView[];
}

export interface ItemView {
  slug: string;
  name: string;
  description: string;
  price: number;
  soldOut: boolean;
  available: boolean;
  orderable: boolean;
  signature: boolean;
  spice: number;
  calories: number | null;
  allergens: Allergen[];
  dietary: DietaryTag[];
  image: ImageView | null;
  /** Normalised text in every language (so an English query works on the Arabic page and vice versa). */
  search: string;
  /** Slugs of every category the dish appears in (used by the matchmaker). */
  categories: string[];
  modifierGroups: ModifierGroupView[];
}

export interface MenuView {
  slug: string;
  kind: MenuKind;
  name: string;
  hour: string | null;
  description: string | null;
  schedule: MenuWindow[];
  isTasting: boolean;
  tastingPrice: number | null;
  seasonal: boolean;
  categories: { slug: string; name: string; description: string | null; items: string[] }[];
}

export function imageView(media: MediaDTO | null, locale: string): ImageView | null {
  if (!media) return null;
  return { src: media.src, width: media.width, height: media.height, blur: media.blur, focalX: media.focalX, focalY: media.focalY, alt: tr(media.alt, locale) };
}

export function itemView(item: MenuItemDTO, catalog: MenuCatalog, branchId: string | null, locale: string): ItemView {
  const state = branchId ? item.branches[branchId] : undefined;
  const categories = catalog.menus.flatMap((m) => m.categories.filter((c) => c.items.includes(item.slug)).map((c) => c.slug));
  return {
    slug: item.slug,
    name: tr(item.name, locale),
    description: tr(item.description, locale),
    price: state?.price ?? item.price,
    soldOut: state?.soldOut ?? false,
    available: state?.available ?? true,
    orderable: item.orderable,
    signature: item.isSignature,
    spice: item.spiceLevel,
    calories: item.calories,
    allergens: item.allergens,
    dietary: item.dietary,
    image: imageView(item.image, locale),
    search: normalizeForSearch(
      [item.name, item.description, item.ingredients]
        .flatMap((v) => (v ? Object.values(v) : []))
        .filter(Boolean)
        .join(' '),
    ),
    categories: [...new Set(categories)],
    modifierGroups: item.modifierGroups.map((g) => ({
      id: g.id,
      name: tr(g.name, locale),
      minSelect: g.minSelect,
      maxSelect: g.maxSelect,
      options: g.options.map((o) => ({ id: o.id, name: tr(o.name, locale), priceDelta: o.priceDelta, isDefault: o.isDefault, isAvailable: o.isAvailable })),
    })),
  };
}

export function menuView(menu: MenuDTO, locale: string): MenuView {
  return {
    slug: menu.slug,
    kind: menu.kind,
    name: tr(menu.name, locale),
    hour: menu.hour ? tr(menu.hour, locale) : null,
    description: menu.description ? tr(menu.description, locale) : null,
    schedule: menu.schedule,
    isTasting: menu.isTasting,
    tastingPrice: menu.tastingPrice,
    seasonal: Boolean(menu.seasonalModeId),
    categories: menu.categories.map((c) => ({ slug: c.slug, name: tr(c.name, locale), description: c.description ? tr(c.description, locale) : null, items: c.items })),
  };
}

/** Items visible on the public menu: active and (unless the platform allows it) alcohol-free. */
export function visibleItems(catalog: MenuCatalog, allowAlcohol: boolean): MenuItemDTO[] {
  return Object.values(catalog.items).filter((i) => i.isActive && (allowAlcohol || !i.isAlcoholic));
}
