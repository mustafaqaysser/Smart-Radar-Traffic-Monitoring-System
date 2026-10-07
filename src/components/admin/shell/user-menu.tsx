'use client';

import { ExternalLink, Keyboard, Languages, LogOut, Monitor, Moon, Sun, UserRound } from 'lucide-react';
import { useTranslations } from 'next-intl';
import { useRouter } from 'next/navigation';
import { authClient } from '@/lib/auth/client';
import { setAdminLocale, setAdminTheme } from '@/lib/actions/admin/shell';
import type { AdminTheme } from '@/lib/admin/context';
import type { Role } from '@/lib/db/schema';
import { Button } from '../ui/button';
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuLabel, DropdownMenuRadioGroup, DropdownMenuRadioItem, DropdownMenuSeparator, DropdownMenuTrigger } from '../ui/dropdown-menu';
import { useAdminAction } from '../use-action';

/** Account menu: language, appearance, shortcuts, the public site, and signing out. */
export function UserMenu({ user, locale, theme, onShortcuts }: { user: { name: string; email: string; role: Role }; locale: string; theme: AdminTheme; onShortcuts: () => void }) {
  const t = useTranslations('admin.shell.user');
  const tr = useTranslations('admin.shell.roles');
  const router = useRouter();
  const [, run] = useAdminAction();
  const initials = user.name.trim().split(/\s+/).map((p) => p[0]).slice(0, 2).join('');
  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button variant="ghost" size="icon" aria-label={t('label', { name: user.name })}>
          <span aria-hidden="true" className="inline-flex size-7 items-center justify-center rounded-full bg-ink text-[0.6875rem] font-semibold text-bg">
            {initials || <UserRound className="size-4" />}
          </span>
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent className="w-60">
        <div className="px-2 py-1.5">
          <p className="truncate text-[0.875rem] font-medium">{user.name}</p>
          <p className="truncate text-xs text-muted" dir="ltr">
            {user.email}
          </p>
          <p className="mt-0.5 text-xs text-muted">{tr(user.role)}</p>
        </div>
        <DropdownMenuSeparator />
        <DropdownMenuLabel className="flex items-center gap-1.5">
          <Languages className="size-3.5" aria-hidden="true" />
          {t('language')}
        </DropdownMenuLabel>
        <DropdownMenuRadioGroup value={locale} onValueChange={(value) => run(() => setAdminLocale(value))}>
          <DropdownMenuRadioItem value="ar" lang="ar">
            العربية
          </DropdownMenuRadioItem>
          <DropdownMenuRadioItem value="en" lang="en">
            English
          </DropdownMenuRadioItem>
        </DropdownMenuRadioGroup>
        <DropdownMenuSeparator />
        <DropdownMenuLabel>{t('appearance')}</DropdownMenuLabel>
        <DropdownMenuRadioGroup
          value={theme}
          onValueChange={(value) => {
            document.documentElement.dataset.theme = value;
            void run(() => setAdminTheme(value));
          }}
        >
          <DropdownMenuRadioItem value="system">
            <Monitor aria-hidden="true" />
            {t('themes.system')}
          </DropdownMenuRadioItem>
          <DropdownMenuRadioItem value="light">
            <Sun aria-hidden="true" />
            {t('themes.light')}
          </DropdownMenuRadioItem>
          <DropdownMenuRadioItem value="dark">
            <Moon aria-hidden="true" />
            {t('themes.dark')}
          </DropdownMenuRadioItem>
        </DropdownMenuRadioGroup>
        <DropdownMenuSeparator />
        <DropdownMenuItem onSelect={() => router.push('/admin/profile')}>
          <UserRound aria-hidden="true" />
          {t('profile')}
        </DropdownMenuItem>
        <DropdownMenuItem onSelect={onShortcuts}>
          <Keyboard aria-hidden="true" />
          {t('shortcuts')}
        </DropdownMenuItem>
        <DropdownMenuItem onSelect={() => window.open(`/${locale}`, '_blank', 'noopener')}>
          <ExternalLink aria-hidden="true" />
          {t('viewSite')}
        </DropdownMenuItem>
        <DropdownMenuSeparator />
        <DropdownMenuItem
          tone="danger"
          onSelect={async () => {
            await authClient.signOut();
            router.replace('/admin/sign-in');
            router.refresh();
          }}
        >
          <LogOut aria-hidden="true" />
          {t('signOut')}
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
