import type { ReactNode } from 'react';
import restaurantConfig from '@config';
import { adminPage, adminTheme } from '@/lib/admin/context';
import { livePulse } from '@/lib/admin/live';
import { navFor } from '@/lib/admin/nav';
import { isUnread, visibleNotifications } from '@/lib/admin/notifications';
import { tr } from '@/lib/i18n/localized';
import { AdminShell } from '@/components/admin/shell/admin-shell';

/** Signed-in staff only: navigation for their role, their houses, live counts and the latest notices. */
export default async function ShellLayout({ children }: { children: ReactNode }) {
  const { user, locale, scope } = await adminPage();
  const [theme, pulse, notices] = await Promise.all([adminTheme(), livePulse(user, scope), visibleNotifications(user, scope, 8)]);
  return (
    <AdminShell
      nav={navFor(user.role)}
      user={{ name: user.name, email: user.email, role: user.role }}
      branches={scope.branches.map((b) => ({ id: b.id, name: tr(b.shortName, locale) }))}
      branchId={scope.branch?.id ?? null}
      locked={scope.locked}
      locale={locale}
      theme={theme}
      siteName={tr(restaurantConfig.name, locale)}
      pulse={pulse}
      notifications={notices.map((n) => ({ id: n.id, title: tr(n.title, locale), body: n.body ? tr(n.body, locale) : null, href: n.href, at: n.createdAt.toISOString(), unread: isUnread(n, user.id) }))}
    >
      {children}
    </AdminShell>
  );
}
