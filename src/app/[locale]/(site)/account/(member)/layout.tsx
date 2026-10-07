import type { ReactNode } from 'react';
import { setRequestLocale } from 'next-intl/server';
import { AccountNav, SignOutButton, type AccountSection } from '@/components/site/account/account-nav';
import { redirect } from '@/i18n/navigation';
import { getCurrentUser } from '@/lib/auth/session';
import { homeFor, isStaff } from '@/lib/auth/permissions';
import { getSettings } from '@/lib/server/settings';

/** Every account page needs a signed-in guest; the sections follow the restaurant's feature flags. */
export default async function MemberLayout({ children, params }: { children: ReactNode; params: Promise<{ locale: string }> }) {
  const { locale } = await params;
  setRequestLocale(locale);
  const [user, settings] = await Promise.all([getCurrentUser(), getSettings()]);
  if (!user) redirect({ href: { pathname: '/account/sign-in', query: { next: '/account' } }, locale });
  const f = settings.features;
  const hidden: AccountSection[] = [
    ...(!f.ordering ? (['orders', 'addresses'] as const) : []),
    ...(!f.loyalty ? (['loyalty'] as const) : []),
    ...(!f.giftCards ? (['giftCards'] as const) : []),
    ...(!f.reservations && !f.events ? (['reservations'] as const) : []),
  ];
  return (
    <div className="site-grid gap-y-10 pt-8 pb-[var(--spacing-section)] lg:pt-14">
      <aside className="col-span-full lg:col-span-3">
        <AccountNav hidden={hidden} staffHref={user && isStaff(user.role) ? homeFor(user.role) : null} />
      </aside>
      <div className="col-span-full flex min-w-0 flex-col gap-12 lg:col-span-8 lg:col-start-5">
        {children}
        <SignOutButton className="self-start lg:hidden" />
      </div>
    </div>
  );
}
