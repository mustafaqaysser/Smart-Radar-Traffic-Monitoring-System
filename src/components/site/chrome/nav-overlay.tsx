'use client';

import { useTranslations } from 'next-intl';
import { useEffect, useRef } from 'react';
import { Icon } from '@/components/brand/icon';
import { Monogram } from '@/components/brand/logo';
import { useMotion } from '@/components/motion/motion-provider';
import { Link, usePathname } from '@/i18n/navigation';
import type { FeatureFlag } from '@/lib/config/define';
import { cn } from '@/lib/utils/cn';
import { BranchSwitch, type BranchOption } from './branch-switch';
import { LocaleSwitch } from './locale-switch';
import { NAV_GROUPS, visible } from './nav-config';
import { InstrumentLine } from '../instrument-line';

interface NavOverlayProps {
  open: boolean;
  onClose: () => void;
  features: Partial<Record<FeatureFlag, boolean>>;
  branches: BranchOption[];
  selectedBranch: string | null;
  signedIn: boolean;
  servingNow: string[];
}

const LEAD = new Set(['menu', 'reserve', 'order', 'experiences']);

/** Full-screen navigation: the shade slides across the page and the house's map of links appears in it. */
export function NavOverlay({ open, onClose, features, branches, selectedBranch, signedIn, servingNow }: NavOverlayProps) {
  const t = useTranslations('common');
  const ref = useRef<HTMLDialogElement>(null);
  const pathname = usePathname();
  const { lockScroll } = useMotion();

  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    if (open && !el.open) {
      el.showModal();
      lockScroll(true);
      return () => lockScroll(false);
    }
    if (!open && el.open) el.close();
    return undefined;
  }, [open, lockScroll]);

  return (
    <dialog
      id="site-nav-overlay"
      ref={ref}
      aria-label={t('a11y.mainNav')}
      onCancel={(e) => {
        e.preventDefault();
        onClose();
      }}
      className="nav-overlay m-0 h-dvh max-h-none w-screen max-w-none overflow-hidden bg-ink p-0 text-bg"
    >
      <div className="flex h-full flex-col overflow-y-auto" data-lenis-prevent>
        <div className="site-wrap flex h-16 shrink-0 items-center justify-between lg:h-[4.5rem]">
          <Monogram decorative className="h-9 w-auto" />
          <div className="flex items-center gap-2">
            <LocaleSwitch />
            <button type="button" onClick={onClose} className="-me-2 inline-flex size-11 items-center justify-center" aria-label={t('a11y.closeMenu')}>
              <Icon name="close" size={24} />
            </button>
          </div>
        </div>

        <nav aria-label={t('a11y.mainNav')} className="site-grid flex-1 content-start gap-y-12 pt-6 pb-12 lg:pt-12">
          {NAV_GROUPS.map((group, gi) => {
            const items = visible(group.items, features);
            if (!items.length) return null;
            return (
              <section key={group.key} className="nav-group col-span-full md:col-span-4 lg:col-span-3" style={{ '--i': gi } as React.CSSProperties}>
                <h2 className="t-label mb-5 opacity-70">{t(`nav.groups.${group.key}`)}</h2>
                <ul className="space-y-1">
                  {items.map((item) => {
                    const current = pathname === item.href || pathname.startsWith(`${item.href}/`);
                    return (
                      <li key={item.href}>
                        <Link
                          href={item.href}
                          onClick={onClose}
                          aria-current={current ? 'page' : undefined}
                          className={cn(
                            'group inline-flex items-baseline gap-3 py-1 underline decoration-transparent underline-offset-[0.2em] transition-[text-decoration-color] hover-capable:hover:decoration-current aria-[current=page]:decoration-current',
                            LEAD.has(item.key) ? 't-heading-lg' : 't-heading-sm',
                          )}
                        >
                          {t(`nav.${item.key}`)}
                        </Link>
                      </li>
                    );
                  })}
                </ul>
              </section>
            );
          })}
        </nav>

        <div className="site-grid shrink-0 gap-y-6 border-t border-current/20 py-8">
          <div className="col-span-full md:col-span-4 lg:col-span-5">
            <BranchSwitch branches={branches} selected={selectedBranch} />
          </div>
          <div className="col-span-full flex flex-col justify-end gap-4 md:col-span-4 lg:col-span-7 lg:items-end">
            <InstrumentLine servingNow={servingNow} className="opacity-80 lg:justify-end" />
            <Link href="/account" onClick={onClose} className="inline-flex min-h-11 items-center gap-2">
              <Icon name="user" size={20} />
              <span>{signedIn ? t('nav.account') : t('nav.signIn')}</span>
            </Link>
          </div>
        </div>
      </div>
    </dialog>
  );
}
