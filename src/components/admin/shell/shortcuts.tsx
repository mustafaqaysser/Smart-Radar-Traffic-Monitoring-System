'use client';

import { useTranslations } from 'next-intl';
import { useRouter } from 'next/navigation';
import { useEffect, useRef } from 'react';
import type { AdminNavItem } from '@/lib/admin/nav';
import { Dialog, DialogContent } from '../ui/dialog';
import { Kbd } from '../ui/kbd';

function typing(target: EventTarget | null): boolean {
  const el = target as HTMLElement | null;
  if (!el) return false;
  return el.isContentEditable || ['INPUT', 'TEXTAREA', 'SELECT'].includes(el.tagName) || Boolean(el.closest('[role="dialog"],[role="menu"],[role="listbox"]'));
}

/**
 * Keyboard navigation for the whole back-office: ⌘K / Ctrl+K opens the palette, "/" searches the current table
 * (or opens the palette), "g" then a letter jumps to a screen, and "?" lists everything.
 */
export function useShortcuts({ nav, openPalette, openHelp }: { nav: AdminNavItem[]; openPalette: () => void; openHelp: () => void }) {
  const router = useRouter();
  const pendingG = useRef<number | null>(null);
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') {
        e.preventDefault();
        openPalette();
        return;
      }
      if (e.metaKey || e.ctrlKey || e.altKey || typing(e.target)) return;
      if (e.key === '/') {
        const search = document.querySelector<HTMLInputElement>('[data-table-search]');
        e.preventDefault();
        if (search) search.focus();
        else openPalette();
        return;
      }
      if (e.key === '?') {
        e.preventDefault();
        openHelp();
        return;
      }
      if (pendingG.current !== null && Date.now() - pendingG.current < 1200) {
        pendingG.current = null;
        const item = nav.find((n) => n.shortcut === e.key.toLowerCase());
        if (item) {
          e.preventDefault();
          router.push(item.href);
        }
        return;
      }
      if (e.key === 'g') pendingG.current = Date.now();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [nav, openPalette, openHelp, router]);
}

export function ShortcutsDialog({ open, onOpenChange, nav }: { open: boolean; onOpenChange: (open: boolean) => void; nav: AdminNavItem[] }) {
  const t = useTranslations('admin.shell.shortcuts');
  const tn = useTranslations('admin.shell.nav.items');
  const general: [string[], string][] = [
    [['⌘', 'K'], t('palette')],
    [['/'], t('search')],
    [['?'], t('help')],
    [['Esc'], t('close')],
  ];
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent title={t('title')} description={t('description')}>
        <h3 className="mb-2 text-xs font-medium text-muted">{t('general')}</h3>
        <ul className="mb-5 flex flex-col gap-1.5">
          {general.map(([keys, label]) => (
            <li key={label} className="flex items-center justify-between gap-4 text-[0.875rem]">
              <span>{label}</span>
              <span className="flex gap-1" dir="ltr">
                {keys.map((k) => (
                  <Kbd key={k}>{k}</Kbd>
                ))}
              </span>
            </li>
          ))}
        </ul>
        <h3 className="mb-2 text-xs font-medium text-muted">{t('goTo')}</h3>
        <ul className="grid gap-1.5 sm:grid-cols-2 sm:gap-x-6">
          {nav
            .filter((n) => n.shortcut)
            .map((n) => (
              <li key={n.key} className="flex items-center justify-between gap-4 text-[0.875rem]">
                <span>{tn(n.key)}</span>
                <span className="flex gap-1" dir="ltr">
                  <Kbd>G</Kbd>
                  <Kbd>{n.shortcut?.toUpperCase()}</Kbd>
                </span>
              </li>
            ))}
        </ul>
      </DialogContent>
    </Dialog>
  );
}
