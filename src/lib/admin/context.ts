import 'server-only';
import { cache } from 'react';
import { cookies } from 'next/headers';
import { getLocale } from 'next-intl/server';
import restaurantConfig from '@config';
import { requirePagePermission, requireStaffPage, type CurrentUser } from '@/lib/auth/session';
import type { Permission } from '@/lib/auth/permissions';
import { getBranches } from '@/lib/queries/branches';
import type { BranchDTO } from '@/lib/queries/types';

export const ADMIN_BRANCH_COOKIE = 'zill_admin_branch';
export const ADMIN_THEME_COOKIE = 'zill_admin_theme';
export const ADMIN_SOUND_COOKIE = 'zill_admin_sound';

export const ADMIN_THEMES = ['system', 'light', 'dark'] as const;
export type AdminTheme = (typeof ADMIN_THEMES)[number];

export async function adminTheme(): Promise<AdminTheme> {
  const value = (await cookies()).get(ADMIN_THEME_COOKIE)?.value;
  return (ADMIN_THEMES as readonly string[]).includes(value ?? '') ? (value as AdminTheme) : 'system';
}

export interface AdminScope {
  branches: BranchDTO[];
  /** The house being looked at, or null for every house. */
  branch: BranchDTO | null;
  /** Staff assigned to one house cannot switch. */
  locked: boolean;
  /** Zone for dates and times on screen (the house's, or the restaurant's default for "every house"). */
  timeZone: string;
  /** Branch ids the staff member may see. */
  branchIds: string[];
}

/** Which houses a staff member sees: their own when assigned to one, otherwise the switcher's choice. */
export const getAdminScope = cache(async (user: CurrentUser): Promise<AdminScope> => {
  const branches = await getBranches();
  const own = user.branchId ? branches.find((b) => b.id === user.branchId) : undefined;
  if (own) return { branches: [own], branch: own, locked: true, timeZone: own.timeZone, branchIds: [own.id] };
  const chosen = (await cookies()).get(ADMIN_BRANCH_COOKIE)?.value;
  const branch = branches.find((b) => b.id === chosen) ?? null;
  return { branches, branch, locked: false, timeZone: branch?.timeZone ?? restaurantConfig.defaultTimeZone, branchIds: branch ? [branch.id] : branches.map((b) => b.id) };
});

/**
 * A house for screens that always show exactly one (timeline, floor, KDS): the `?branch=` in the link if allowed,
 * else the switcher's house, else the flagship.
 */
export function singleBranch(scope: AdminScope, requested?: string | null): BranchDTO {
  const pick = requested ? scope.branches.find((b) => b.id === requested || b.slug === requested) : undefined;
  return pick ?? scope.branch ?? scope.branches.find((b) => b.slug === restaurantConfig.flagshipBranchSlug) ?? (scope.branches[0] as BranchDTO);
}

export interface AdminPageContext {
  user: CurrentUser;
  locale: string;
  scope: AdminScope;
}

/** Every admin page starts here: signed-in staff with the permission (or a redirect), language and house scope. */
export async function adminPage(permission?: Permission): Promise<AdminPageContext> {
  const user = permission ? await requirePagePermission(permission) : await requireStaffPage();
  const [locale, scope] = await Promise.all([getLocale(), getAdminScope(user)]);
  return { user, locale, scope };
}
