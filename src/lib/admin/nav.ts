import { can, type Permission } from '@/lib/auth/permissions';
import type { Role } from '@/lib/db/schema';

export type AdminNavIcon =
  | 'dashboard'
  | 'orders'
  | 'kds'
  | 'reservations'
  | 'tables'
  | 'menu'
  | 'customers'
  | 'reviews'
  | 'newsletter'
  | 'inquiries'
  | 'events'
  | 'giftCards'
  | 'promotions'
  | 'loyalty'
  | 'content'
  | 'journal'
  | 'media'
  | 'careers'
  | 'privateDining'
  | 'locations'
  | 'seasonal'
  | 'reports'
  | 'settings'
  | 'staff'
  | 'notifications'
  | 'audit'
  | 'outbox';

export interface AdminNavItem {
  key: AdminNavIcon;
  href: string;
  /** Shown when the role has any of these (empty: every staff member). */
  anyOf: Permission[];
  /** Second key after "g" (g o → orders). */
  shortcut?: string;
}

export interface AdminNavGroup {
  key: 'today' | 'menu' | 'guests' | 'commerce' | 'content' | 'business' | 'system';
  items: AdminNavItem[];
}

/** The back-office map. Every entry is filtered by the staff member's role before it is shown. */
export const ADMIN_NAV: AdminNavGroup[] = [
  {
    key: 'today',
    items: [
      { key: 'dashboard', href: '/admin', anyOf: ['dashboard:view'], shortcut: 'd' },
      { key: 'orders', href: '/admin/orders', anyOf: ['orders:view'], shortcut: 'o' },
      { key: 'kds', href: '/admin/kds', anyOf: ['kds:view'], shortcut: 'k' },
      { key: 'reservations', href: '/admin/reservations', anyOf: ['reservations:view'], shortcut: 'r' },
      { key: 'tables', href: '/admin/tables', anyOf: ['tables:respond'], shortcut: 't' },
    ],
  },
  {
    key: 'menu',
    items: [{ key: 'menu', href: '/admin/menu', anyOf: ['menu:edit', 'menu:availability'], shortcut: 'm' }],
  },
  {
    key: 'guests',
    items: [
      { key: 'customers', href: '/admin/customers', anyOf: ['customers:view'], shortcut: 'c' },
      { key: 'reviews', href: '/admin/reviews', anyOf: ['reviews:moderate'] },
      { key: 'inquiries', href: '/admin/inquiries', anyOf: ['inquiries:manage'], shortcut: 'i' },
      { key: 'newsletter', href: '/admin/newsletter', anyOf: ['newsletter:manage'] },
    ],
  },
  {
    key: 'commerce',
    items: [
      { key: 'events', href: '/admin/events', anyOf: ['events:manage', 'tickets:checkin'], shortcut: 'e' },
      { key: 'giftCards', href: '/admin/gift-cards', anyOf: ['giftcards:manage'], shortcut: 'g' },
      { key: 'promotions', href: '/admin/promotions', anyOf: ['promotions:manage'] },
      { key: 'loyalty', href: '/admin/loyalty', anyOf: ['loyalty:manage'] },
    ],
  },
  {
    key: 'content',
    items: [
      { key: 'content', href: '/admin/content', anyOf: ['content:edit'] },
      { key: 'journal', href: '/admin/journal', anyOf: ['content:edit'], shortcut: 'j' },
      { key: 'media', href: '/admin/media', anyOf: ['content:edit'] },
      { key: 'privateDining', href: '/admin/private-dining', anyOf: ['content:edit'] },
      { key: 'careers', href: '/admin/careers', anyOf: ['careers:manage'] },
    ],
  },
  {
    key: 'business',
    items: [
      { key: 'locations', href: '/admin/locations', anyOf: ['locations:manage'], shortcut: 'l' },
      { key: 'seasonal', href: '/admin/seasonal', anyOf: ['seasonal:manage'] },
      { key: 'reports', href: '/admin/reports', anyOf: ['reports:view'], shortcut: 'p' },
      { key: 'settings', href: '/admin/settings', anyOf: ['settings:manage'], shortcut: 's' },
      { key: 'staff', href: '/admin/staff', anyOf: ['staff:manage'] },
    ],
  },
  {
    key: 'system',
    items: [
      { key: 'notifications', href: '/admin/notifications', anyOf: [], shortcut: 'n' },
      { key: 'audit', href: '/admin/audit', anyOf: ['audit:view'] },
      { key: 'outbox', href: '/admin/outbox', anyOf: ['outbox:view'] },
    ],
  },
];

export function canSee(role: Role, item: Pick<AdminNavItem, 'anyOf'>): boolean {
  return item.anyOf.length === 0 || item.anyOf.some((p) => can(role, p));
}

/** The navigation a role sees (groups without a visible entry are dropped). */
export function navFor(role: Role): AdminNavGroup[] {
  return ADMIN_NAV.map((g) => ({ ...g, items: g.items.filter((i) => canSee(role, i)) })).filter((g) => g.items.length > 0);
}
