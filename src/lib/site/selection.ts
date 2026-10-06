import 'server-only';
import { cookies } from 'next/headers';
import { cache } from 'react';
import { getBranch, getFlagshipBranch } from '@/lib/queries/branches';
import type { BranchDTO } from '@/lib/queries/types';

/** The visitor's chosen house (slug). Drives the live sun, the menu of the hour and ordering. */
export const BRANCH_COOKIE = 'zill_branch';

export const getSelectedBranch = cache(async (): Promise<BranchDTO | null> => {
  const slug = (await cookies()).get(BRANCH_COOKIE)?.value;
  if (slug) {
    const branch = await getBranch(slug);
    if (branch) return branch;
  }
  return getFlagshipBranch();
});
