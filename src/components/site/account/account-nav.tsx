'use client';

import { useTranslations } from 'next-intl';
import { useTransition } from 'react';
import { Icon, type IconName } from '@/components/brand/icon';
import { Link, usePathname, useRouter } from '@/i18n/navigation';
import { authClient } from '@/lib/auth/client';
import { cn } from '@/lib/utils/cn';

export type AccountSection = 'overview' | 'profile' | 'dietary' | 'addresses' | 'favourites' | 'orders' | 'reservations' | 'giftCards' | 'loyalty' | 'preferences' | 'privacy';

const ITEMS: { key: AccountSection; href: string; icon: IconName }[] = [
  { key: 'overview', href: '/account', icon: 'sun' },
  { key: 'reservations', href: '/account/reservations', icon: 'calendar' },
  { key: 'orders', href: '/account/orders', icon: 'bag' },
  { key: 'favourites', href: '/account/favourites', icon: 'heart' },
  { key: 'dietary', href: '/account/dietary', icon: 'leaf' },
  { key: 'addresses', href: '/account/addresses', icon: 'pin' },
  { key: 'loyalty', href: '/account/loyalty', icon: 'star' },
  { key: 'giftCards', href: '/account/gift-cards', icon: 'gift' },
  { key: 'profile', href: '/account/profile', icon: 'user' },
  { key: 'preferences', href: '/account/preferences', icon: 'mail' },
  { key: 'privacy', href: '/account/privacy', icon: 'lock' },
];

/** Account sections: a ledger down the side on wide screens, a scrolling row of pills on phones. */
export function AccountNav({ hidden, staffHref }: { hidden: AccountSection[]; staffHref: string | null }) {
  const t = useTranslations('account.nav');
  const pathname = usePathname();
  const router = useRouter();
  const [pending, start] = useTransition();
  const items = ITEMS.filter((i) => !hidden.includes(i.key));
  const active = (href: string) => (href === '/account' ? pathname === '/account' : pathname === href || pathname.startsWith(`${href}/`));

  const signOut = () =>
    start(async () => {
      await authClient.signOut();
      router.replace('/');
      router.refresh();
    });

  return (
    <nav aria-label={t('label')} className="flex flex-col gap-6 lg:sticky lg:top-24">
      <ul className="-mx-[var(--spacing-margin)] flex snap-x gap-2 overflow-x-auto px-[var(--spacing-margin)] pb-2 [scrollbar-width:none] lg:mx-0 lg:flex-col lg:gap-0 lg:overflow-visible lg:border-t lg:border-ink lg:px-0 lg:pb-0">
        {items.map((i) => {
          const current = active(i.href);
          return (
            <li key={i.key} className="shrink-0 snap-start lg:border-b lg:border-line">
              <Link
                href={i.href}
                aria-current={current ? 'page' : undefined}
                className={cn(
                  'inline-flex min-h-11 items-center gap-3 rounded-pill border px-4 text-[0.9375rem] whitespace-nowrap transition-colors lg:flex lg:w-full lg:rounded-none lg:border-0 lg:px-0 lg:py-3',
                  current ? 'border-ink bg-ink text-bg lg:bg-transparent lg:text-accent' : 'border-line hover-capable:hover:border-ink lg:hover-capable:hover:text-accent',
                )}
              >
                <Icon name={i.icon} size={18} />
                {t(i.key)}
              </Link>
            </li>
          );
        })}
      </ul>
      <div className="hidden flex-col items-start gap-3 lg:flex">
        {staffHref ? (
          <a href={staffHref} className="t-small inline-flex min-h-11 items-center gap-2 underline decoration-line underline-offset-4">
            <Icon name="table" size={18} />
            {t('admin')}
          </a>
        ) : null}
        <button type="button" onClick={signOut} disabled={pending} className="t-small inline-flex min-h-11 items-center gap-2 underline decoration-line underline-offset-4">
          <Icon name="logout" size={18} />
          {pending ? t('signingOut') : t('signOut')}
        </button>
      </div>
    </nav>
  );
}

/** Sign out on small screens, where the side column is a row of pills. */
export function SignOutButton({ className }: { className?: string }) {
  const t = useTranslations('account.nav');
  const router = useRouter();
  const [pending, start] = useTransition();
  return (
    <button
      type="button"
      disabled={pending}
      onClick={() =>
        start(async () => {
          await authClient.signOut();
          router.replace('/');
          router.refresh();
        })
      }
      className={cn('t-small inline-flex min-h-11 items-center gap-2 underline decoration-line underline-offset-4', className)}
    >
      <Icon name="logout" size={18} />
      {pending ? t('signingOut') : t('signOut')}
    </button>
  );
}
