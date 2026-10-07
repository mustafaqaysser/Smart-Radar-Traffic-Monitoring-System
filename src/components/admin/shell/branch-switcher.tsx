'use client';

import { Check, ChevronDown, Store } from 'lucide-react';
import { useTranslations } from 'next-intl';
import { setAdminBranch } from '@/lib/actions/admin/shell';
import { Button } from '../ui/button';
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuLabel, DropdownMenuSeparator, DropdownMenuTrigger } from '../ui/dropdown-menu';
import { useAdminAction } from '../use-action';

/** Which house the screens show. Staff assigned to one house see its name only. */
export function BranchSwitcher({ branches, branchId, locked }: { branches: { id: string; name: string }[]; branchId: string | null; locked: boolean }) {
  const t = useTranslations('admin.shell.branch');
  const [pending, run] = useAdminAction();
  const current = branches.find((b) => b.id === branchId);
  const label = current?.name ?? t('all');
  if (locked || branches.length < 2) {
    return (
      <span className="hidden items-center gap-1.5 px-2 text-[0.8125rem] text-muted sm:inline-flex">
        <Store className="size-4" aria-hidden="true" />
        {label}
      </span>
    );
  }
  const choose = (id: string) => run(() => setAdminBranch(id), { success: t('switched', { name: branches.find((b) => b.id === id)?.name ?? t('all') }) });
  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button variant="ghost" size="sm" aria-busy={pending} className="gap-1.5 px-2">
          <Store className="text-muted" aria-hidden="true" />
          <span className="sr-only">{t('label')}: </span>
          <span className="max-w-[9rem] truncate">{label}</span>
          <ChevronDown className="size-3.5 text-muted" aria-hidden="true" />
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent>
        <DropdownMenuLabel>{t('label')}</DropdownMenuLabel>
        <DropdownMenuItem onSelect={() => choose('all')}>
          <Check className={branchId ? 'invisible' : undefined} aria-hidden="true" />
          {t('all')}
        </DropdownMenuItem>
        <DropdownMenuSeparator />
        {branches.map((b) => (
          <DropdownMenuItem key={b.id} onSelect={() => choose(b.id)}>
            <Check className={b.id === branchId ? undefined : 'invisible'} aria-hidden="true" />
            {b.name}
          </DropdownMenuItem>
        ))}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
