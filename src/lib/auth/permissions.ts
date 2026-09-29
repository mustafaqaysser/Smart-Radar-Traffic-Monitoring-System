import type { Role } from '@/lib/db/schema';

/** Every permission the back-office checks. Enforced on the server in every action and route. */
export const PERMISSIONS = [
  'dashboard:view',
  'orders:view',
  'orders:manage',
  'kds:view',
  'reservations:view',
  'reservations:manage',
  'tables:respond',
  'menu:edit',
  'menu:availability',
  'customers:view',
  'customers:manage',
  'events:manage',
  'tickets:checkin',
  'giftcards:manage',
  'promotions:manage',
  'loyalty:manage',
  'reviews:moderate',
  'newsletter:manage',
  'content:edit',
  'inquiries:manage',
  'careers:manage',
  'locations:manage',
  'seasonal:manage',
  'settings:manage',
  'settings:owner',
  'staff:manage',
  'reports:view',
  'audit:view',
  'outbox:view',
] as const;
export type Permission = (typeof PERMISSIONS)[number];

const ALL = new Set<Permission>(PERMISSIONS);

export const ROLE_PERMISSIONS: Record<Role, ReadonlySet<Permission>> = {
  owner: ALL,
  manager: new Set(PERMISSIONS.filter((p) => p !== 'settings:owner')),
  host: new Set<Permission>(['dashboard:view', 'orders:view', 'reservations:view', 'reservations:manage', 'tables:respond', 'customers:view', 'tickets:checkin', 'inquiries:manage']),
  kitchen: new Set<Permission>(['orders:view', 'orders:manage', 'kds:view', 'menu:availability']),
  waiter: new Set<Permission>(['orders:view', 'reservations:view', 'tables:respond', 'menu:availability']),
  editor: new Set<Permission>(['menu:edit', 'events:manage', 'reviews:moderate', 'newsletter:manage', 'content:edit']),
  customer: new Set<Permission>(),
};

export const STAFF_ROLES: readonly Role[] = ['owner', 'manager', 'host', 'kitchen', 'waiter', 'editor'];

export function can(role: Role | null | undefined, permission: Permission): boolean {
  if (!role) return false;
  return ROLE_PERMISSIONS[role]?.has(permission) ?? false;
}

export function isStaff(role: Role | null | undefined): boolean {
  return !!role && STAFF_ROLES.includes(role);
}

/** Where a staff member lands after signing in. */
export function homeFor(role: Role): string {
  if (role === 'kitchen') return '/admin/kds';
  if (role === 'waiter') return '/admin/tables';
  if (role === 'editor') return '/admin/menu';
  return '/admin';
}
