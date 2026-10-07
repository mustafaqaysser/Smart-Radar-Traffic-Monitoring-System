'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { Dialog as DialogPrimitive } from 'radix-ui';
import { Menu, Search, X } from 'lucide-react';
import { useLocale, useTranslations } from 'next-intl';
import { useState, type ReactNode } from 'react';
import { Monogram } from '@/components/brand/logo';
import type { AdminTheme } from '@/lib/admin/context';
import type { LivePulse } from '@/lib/admin/live';
import type { AdminNavGroup } from '@/lib/admin/nav';
import type { Role } from '@/lib/db/schema';
import { formatNumber } from '@/lib/i18n/format';
import { cn } from '@/lib/utils/cn';
import { Kbd } from '../ui/kbd';
import { BranchSwitcher } from './branch-switcher';
import { CommandPalette } from './command-palette';
import { LiveProvider, LiveRefresh, useLive } from './live';
import { NAV_ICONS } from './nav-icons';
import { NotificationsMenu, type NotificationView } from './notifications-menu';
import { ShortcutsDialog, useShortcuts } from './shortcuts';
import { UserMenu } from './user-menu';

export interface ShellProps {
  nav: AdminNavGroup[];
  user: { name: string; email: string; role: Role };
  branches: { id: string; name: string }[];
  branchId: string | null;
  locked: boolean;
  locale: string;
  theme: AdminTheme;
  siteName: string;
  pulse: LivePulse;
  notifications: NotificationView[];
  children: ReactNode;
}

function isActive(pathname: string, href: string) {
  return href === '/admin' ? pathname === '/admin' : pathname === href || pathname.startsWith(`${href}/`);
}

/** Counts shown beside navigation entries: new orders, open table calls, unread notices. */
function useNavCounts(): Partial<Record<string, number>> {
  const live = useLive();
  return {
    orders: live?.orders?.placed.length ?? 0,
    tables: live?.requests?.open ?? 0,
    notifications: live?.notifications.unread ?? 0,
  };
}

function NavList({ nav, onNavigate }: { nav: AdminNavGroup[]; onNavigate?: () => void }) {
  const t = useTranslations('admin.shell.nav');
  const pathname = usePathname();
  const counts = useNavCounts();
  const locale = useLocale();
  return (
    <nav aria-label={t('label')} className="flex flex-col gap-5">
      {nav.map((group) => (
        <div key={group.key}>
          <p className="mb-1 px-3 text-[0.6875rem] font-medium text-muted">{t(`groups.${group.key}`)}</p>
          <ul className="flex flex-col gap-px">
            {group.items.map((item) => {
              const Icon = NAV_ICONS[item.key];
              const active = isActive(pathname, item.href);
              const count = counts[item.key] ?? 0;
              return (
                <li key={item.key}>
                  <Link
                    href={item.href}
                    onClick={onNavigate}
                    aria-current={active ? 'page' : undefined}
                    className={cn(
                      'group flex h-8 items-center gap-2.5 rounded-hair px-3 text-[0.875rem] text-ink/85 no-underline transition-colors hover-capable:hover:bg-surface hover-capable:hover:text-ink pointer-coarse:h-11',
                      active && 'bg-surface font-medium text-ink',
                    )}
                  >
                    <Icon className={cn('size-4 text-muted', active && 'text-accent')} aria-hidden="true" />
                    <span className="min-w-0 flex-1 truncate">{t(`items.${item.key}`)}</span>
                    {count > 0 ? (
                      <span className={cn('min-w-5 rounded-pill px-1.5 text-center text-[0.6875rem] font-medium leading-5 tabular', item.key === 'notifications' ? 'bg-surface text-ink' : 'bg-accent text-on-accent')}>
                        {formatNumber(count, locale)}
                        <span className="sr-only"> {t(`counts.${item.key}`)}</span>
                      </span>
                    ) : null}
                  </Link>
                </li>
              );
            })}
          </ul>
        </div>
      ))}
    </nav>
  );
}

function Brand({ siteName }: { siteName: string }) {
  const t = useTranslations('admin.shell');
  return (
    <Link href="/admin" className="flex items-center gap-2.5 no-underline">
      <Monogram className="h-7 w-auto text-ink" decorative />
      <span className="flex flex-col leading-tight">
        <span className="text-[0.9375rem] font-semibold">{siteName}</span>
        <span className="text-[0.6875rem] text-muted">{t('backOffice')}</span>
      </span>
    </Link>
  );
}

export function AdminShell(props: ShellProps) {
  return (
    <LiveProvider initial={props.pulse}>
      <ShellFrame {...props} />
    </LiveProvider>
  );
}

function ShellFrame({ nav, user, branches, branchId, locked, locale, theme, siteName, notifications, children }: ShellProps) {
  const t = useTranslations('admin.shell');
  const [mobileOpen, setMobileOpen] = useState(false);
  const [paletteOpen, setPaletteOpen] = useState(false);
  const [helpOpen, setHelpOpen] = useState(false);
  const flatNav = nav.flatMap((g) => g.items);
  useShortcuts({ nav: flatNav, openPalette: () => setPaletteOpen(true), openHelp: () => setHelpOpen(true) });

  return (
    <div className="min-h-dvh lg:grid lg:grid-cols-[15rem_minmax(0,1fr)]">
      <a href="#admin-main" className="skip-link">
        {t('skip')}
      </a>
      <LiveRefresh topics={['notifications']} />
      <aside data-admin-chrome className="sticky top-0 hidden h-dvh flex-col border-e border-line bg-raised lg:flex">
        <div className="flex h-14 shrink-0 items-center border-b border-line px-4">
          <Brand siteName={siteName} />
        </div>
        <div className="min-h-0 flex-1 overflow-y-auto px-2 py-4 scrollbar-thin">
          <NavList nav={nav} />
        </div>
        <div className="border-t border-line px-4 py-3 text-xs text-muted">
          <p className="truncate font-medium text-ink">{user.name}</p>
          <p className="truncate">{t(`roles.${user.role}`)}</p>
        </div>
      </aside>

      <div className="flex min-w-0 flex-col">
        <header data-admin-chrome className="sticky top-0 z-40 flex h-14 items-center gap-2 border-b border-line bg-bg/95 px-3 backdrop-blur supports-[backdrop-filter]:bg-bg/85 sm:px-5">
          <DialogPrimitive.Root open={mobileOpen} onOpenChange={setMobileOpen}>
            <DialogPrimitive.Trigger className="inline-flex size-9 items-center justify-center rounded-hair hover-capable:hover:bg-surface lg:hidden pointer-coarse:size-11" aria-label={t('openMenu')}>
              <Menu className="size-5" />
            </DialogPrimitive.Trigger>
            <DialogPrimitive.Portal>
              <DialogPrimitive.Overlay className="fixed inset-0 z-50 bg-[rgba(18,17,25,0.45)] lg:hidden" />
              <DialogPrimitive.Content className="fixed inset-y-0 start-0 z-50 flex w-[17rem] max-w-[85vw] flex-col border-e border-line bg-raised focus:outline-none animate-[from-left_var(--dur-base)_var(--ease-shade)] rtl:animate-[from-right_var(--dur-base)_var(--ease-shade)] lg:hidden" aria-describedby={undefined}>
                <div className="flex h-14 shrink-0 items-center justify-between border-b border-line px-4">
                  <DialogPrimitive.Title asChild>
                    <div>
                      <Brand siteName={siteName} />
                    </div>
                  </DialogPrimitive.Title>
                  <DialogPrimitive.Close className="-me-2 inline-flex size-10 items-center justify-center rounded-hair hover-capable:hover:bg-surface" aria-label={t('closeMenu')}>
                    <X className="size-5" />
                  </DialogPrimitive.Close>
                </div>
                <div className="min-h-0 flex-1 overflow-y-auto px-2 py-4">
                  <NavList nav={nav} onNavigate={() => setMobileOpen(false)} />
                </div>
              </DialogPrimitive.Content>
            </DialogPrimitive.Portal>
          </DialogPrimitive.Root>

          <button
            type="button"
            onClick={() => setPaletteOpen(true)}
            className="flex h-9 min-w-0 max-w-sm flex-1 items-center gap-2 rounded-hair border border-line bg-raised px-3 text-start text-[0.875rem] text-muted transition-colors hover-capable:hover:border-field pointer-coarse:h-11"
            aria-keyshortcuts="Control+K Meta+K"
          >
            <Search className="size-4" aria-hidden="true" />
            <span className="min-w-0 flex-1 truncate">{t('search')}</span>
            <Kbd className="hidden sm:inline-flex">⌘K</Kbd>
          </button>

          <div className="ms-auto flex items-center gap-1">
            <BranchSwitcher branches={branches} branchId={branchId} locked={locked} />
            <NotificationsMenu items={notifications} locale={locale} />
            <UserMenu user={user} locale={locale} theme={theme} onShortcuts={() => setHelpOpen(true)} />
          </div>
        </header>

        <main id="admin-main" tabIndex={-1} className="min-w-0 flex-1 px-4 py-5 focus:outline-none sm:px-6 sm:py-6 xl:px-8">
          {children}
        </main>
      </div>

      <CommandPalette open={paletteOpen} onOpenChange={setPaletteOpen} nav={nav} />
      <ShortcutsDialog open={helpOpen} onOpenChange={setHelpOpen} nav={flatNav} />
    </div>
  );
}
