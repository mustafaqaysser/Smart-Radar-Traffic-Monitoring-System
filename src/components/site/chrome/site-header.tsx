'use client';

import { useLocale, useTranslations } from 'next-intl';
import { useEffect, useRef, useState } from 'react';
import { Icon } from '@/components/brand/icon';
import { Wordmark } from '@/components/brand/logo';
import { ReserveDockLink } from '@/components/site/reserve/reserve-dock-link';
import { buttonClasses } from '@/components/site/ui/button';
import { Link, usePathname } from '@/i18n/navigation';
import { useCartCount } from '@/lib/cart/store';
import type { FeatureFlag } from '@/lib/config/define';
import { formatNumber } from '@/lib/i18n/format';
import { plural } from '@/lib/i18n/plural';
import { cn } from '@/lib/utils/cn';
import { LocaleSwitch } from './locale-switch';
import { HEADER_LINKS, visible } from './nav-config';
import { NavOverlay } from './nav-overlay';
import type { BranchOption } from './branch-switch';

export interface HeaderProps {
  features: Partial<Record<FeatureFlag, boolean>>;
  branches: BranchOption[];
  selectedBranch: string | null;
  signedIn: boolean;
  servingNow: string[];
}

const HIDE_DOCK = ['/reserve', '/order', '/t/', '/gift-cards', '/account', '/brand'];

export function SiteHeader({ features, branches, selectedBranch, signedIn, servingNow }: HeaderProps) {
  const t = useTranslations('common');
  const locale = useLocale();
  const pathname = usePathname();
  const count = useCartCount();
  const [open, setOpen] = useState(false);
  const [hidden, setHidden] = useState(false);
  const [scrolled, setScrolled] = useState(false);
  const last = useRef(0);

  // Hide while scrolling down, return on the way up (never while focus is inside the header).
  useEffect(() => {
    const onScroll = () => {
      const y = window.scrollY;
      const header = document.getElementById('site-header');
      const focusInside = header?.contains(document.activeElement) ?? false;
      setScrolled(y > 8);
      setHidden(!focusInside && y > 160 && y > last.current + 4);
      if (y < last.current - 4 || y <= 160) setHidden(false);
      last.current = y;
    };
    onScroll();
    window.addEventListener('scroll', onScroll, { passive: true });
    return () => window.removeEventListener('scroll', onScroll);
  }, []);

  // Close the overlay on navigation (state adjusted during render, not in an effect).
  const [openedAt, setOpenedAt] = useState(pathname);
  if (openedAt !== pathname) {
    setOpenedAt(pathname);
    setOpen(false);
  }

  const links = visible(HEADER_LINKS, features);
  const showDock = !HIDE_DOCK.some((p) => pathname.startsWith(p)) && (features.reservations !== false || features.ordering !== false);
  const isCurrent = (href: string) => pathname === href || pathname.startsWith(`${href}/`);

  return (
    <>
      <header
        id="site-header"
        style={{ viewTransitionName: 'site-header' }}
        className={cn(
          'sticky top-0 z-40 bg-bg transition-[transform,border-color] duration-[var(--dur-calm)] ease-[var(--ease-shade)]',
          'border-b',
          scrolled ? 'border-line' : 'border-transparent',
          hidden && !open ? '-translate-y-full' : 'translate-y-0',
        )}
        onFocusCapture={() => setHidden(false)}
      >
        <div className="site-wrap flex h-16 items-center gap-6 lg:h-[4.5rem]">
          <Link href="/" className="-my-2 flex shrink-0 items-center py-2" aria-label={t('meta.siteName')}>
            <Wordmark locale={locale} decorative sunTint className="h-9 w-auto lg:h-10" />
          </Link>

          <nav aria-label={t('a11y.mainNav')} className="hidden flex-1 lg:block">
            <ul className="flex items-center gap-1 xl:gap-3">
              {links.map((l) => (
                <li key={l.href}>
                  <Link
                    href={l.href}
                    aria-current={isCurrent(l.href) ? 'page' : undefined}
                    className="relative inline-flex min-h-11 items-center px-3 text-[1.0625rem] after:absolute after:inset-x-3 after:bottom-2 after:h-px after:origin-[var(--origin)] after:scale-x-0 after:bg-ink after:transition-transform after:duration-[var(--dur-base)] after:ease-[var(--ease-shade)] after:content-[''] hover-capable:hover:after:scale-x-100 aria-[current=page]:after:scale-x-100 [--origin:left] rtl:[--origin:right]"
                  >
                    {t(`nav.${l.key}`)}
                  </Link>
                </li>
              ))}
            </ul>
          </nav>

          <div className="ms-auto flex items-center gap-1 lg:ms-0 lg:gap-2">
            <LocaleSwitch className="hidden sm:inline-flex" />
            <Link href="/account" className="hidden size-11 items-center justify-center sm:inline-flex" aria-label={signedIn ? t('nav.account') : t('nav.signIn')}>
              <Icon name="user" size={22} />
            </Link>
            {features.ordering !== false ? (
              <Link href="/order/cart" className="relative inline-flex size-11 items-center justify-center" aria-label={t('nav.cartCount', plural(count, locale))}>
                <Icon name="bag" size={22} />
                {count > 0 ? (
                  <span aria-hidden="true" className="tabular absolute end-0.5 top-1 inline-flex min-w-5 items-center justify-center rounded-pill bg-accent px-1 text-[0.6875rem] leading-5 text-on-accent">
                    {formatNumber(count, locale)}
                  </span>
                ) : null}
              </Link>
            ) : null}
            {features.reservations !== false ? (
              <Link href="/reserve" className={buttonClasses('primary', 'sm', 'ms-2 hidden lg:inline-flex')}>
                {t('nav.reserve')}
              </Link>
            ) : null}
            <button type="button" className="-me-2 inline-flex size-11 items-center justify-center" aria-expanded={open} aria-controls="site-nav-overlay" aria-label={t('a11y.openMenu')} onClick={() => setOpen(true)}>
              <Icon name="menu" size={24} />
            </button>
          </div>
        </div>
      </header>

      <NavOverlay open={open} onClose={() => setOpen(false)} features={features} branches={branches} selectedBranch={selectedBranch} signedIn={signedIn} servingNow={servingNow} />

      {showDock ? (
        <div className="fixed inset-x-0 bottom-0 z-30 grid grid-cols-2 border-t border-line bg-bg pb-[env(safe-area-inset-bottom)] lg:hidden" style={{ viewTransitionName: 'site-dock' }}>
          {features.reservations !== false ? (
            <ReserveDockLink className="flex min-h-14 items-center justify-center gap-2 bg-accent text-on-accent">
              <Icon name="calendar" size={20} />
              <span className="font-label text-[0.75rem] tracking-[0.14em] uppercase rtl:text-base rtl:tracking-normal rtl:normal-case">{t('nav.reserve')}</span>
            </ReserveDockLink>
          ) : null}
          {features.ordering !== false ? (
            <Link href="/order" className="flex min-h-14 items-center justify-center gap-2">
              <Icon name="bag" size={20} />
              <span className="font-label text-[0.75rem] tracking-[0.14em] uppercase rtl:text-base rtl:tracking-normal rtl:normal-case">{t('nav.order')}</span>
            </Link>
          ) : null}
        </div>
      ) : null}
    </>
  );
}
