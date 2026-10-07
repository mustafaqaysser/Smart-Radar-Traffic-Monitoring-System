'use client';

import { Command } from 'cmdk';
import { CalendarDays, Gift, ReceiptText, Search, Ticket, UserRound, type LucideIcon } from 'lucide-react';
import { useTranslations } from 'next-intl';
import { useRouter } from 'next/navigation';
import { Dialog as DialogPrimitive } from 'radix-ui';
import { useEffect, useState } from 'react';
import { adminSearch, type SearchHit } from '@/lib/actions/admin/shell';
import type { AdminNavGroup } from '@/lib/admin/nav';
import { matchesSearch } from '@/lib/i18n/arabic';
import { NAV_ICONS } from './nav-icons';

const HIT_ICONS: Record<SearchHit['kind'], LucideIcon> = { order: ReceiptText, reservation: CalendarDays, customer: UserRound, giftCard: Gift, ticket: Ticket };

const itemClass =
  'flex min-h-9 cursor-default select-none items-center gap-2.5 rounded-hair px-2.5 py-1.5 text-[0.875rem] outline-none data-[selected=true]:bg-surface data-[disabled=true]:opacity-50 [&_svg]:size-4 [&_svg]:text-muted pointer-coarse:min-h-11';

/**
 * Jump anywhere and find any record: screens the role can open, and live search over order numbers, booking
 * codes, guests, gift cards and tickets. Arabic search ignores hamza and diacritic differences.
 */
export function CommandPalette({ open, onOpenChange, nav }: { open: boolean; onOpenChange: (open: boolean) => void; nav: AdminNavGroup[] }) {
  const t = useTranslations('admin.shell.palette');
  const tn = useTranslations('admin.shell.nav');
  const router = useRouter();
  const [query, setQuery] = useState('');
  const [hits, setHits] = useState<SearchHit[]>([]);
  const [searching, setSearching] = useState(false);

  useEffect(() => {
    const q = query.trim();
    if (q.length < 2) return;
    let cancelled = false;
    const timer = window.setTimeout(async () => {
      setSearching(true);
      const result = await adminSearch(q).catch(() => null);
      if (cancelled) return;
      setSearching(false);
      setHits(result?.ok ? result.data : []);
    }, 220);
    return () => {
      cancelled = true;
      window.clearTimeout(timer);
    };
  }, [query]);

  const go = (href: string) => {
    onOpenChange(false);
    setQuery('');
    setHits([]);
    router.push(href);
  };

  const q = query.trim();
  const screens = nav
    .map((g) => ({ ...g, items: g.items.filter((i) => !q || matchesSearch(`${tn(`items.${i.key}`)} ${tn(`groups.${g.key}`)} ${i.href}`, q)) }))
    .filter((g) => g.items.length);
  const records = q.length >= 2 ? hits : [];

  return (
    <DialogPrimitive.Root
      open={open}
      onOpenChange={(v) => {
        onOpenChange(v);
        if (!v) {
          setQuery('');
          setHits([]);
        }
      }}
    >
      <DialogPrimitive.Portal>
        <DialogPrimitive.Overlay className="fixed inset-0 z-50 bg-[rgba(18,17,25,0.45)]" />
        <DialogPrimitive.Content className="cast fixed start-1/2 top-[12vh] z-50 w-[calc(100vw-2rem)] max-w-xl -translate-x-1/2 overflow-hidden rounded-soft border border-line bg-raised focus:outline-none rtl:translate-x-1/2 animate-[rise-in_var(--dur-base)_var(--ease-shade)]" aria-describedby={undefined}>
          <DialogPrimitive.Title className="sr-only">{t('title')}</DialogPrimitive.Title>
          <Command label={t('title')} shouldFilter={false} loop>
            <div className="flex items-center gap-2 border-b border-line px-3">
              <Search className="size-4 text-muted" aria-hidden="true" />
              <Command.Input value={query} onValueChange={setQuery} placeholder={t('placeholder')} className="h-12 w-full bg-transparent text-[0.9375rem] outline-none placeholder:text-muted" />
            </div>
            <Command.List className="max-h-[min(60vh,28rem)] overflow-y-auto p-1.5 scrollbar-thin">
              <Command.Empty className="px-3 py-8 text-center text-[0.875rem] text-muted">{searching ? t('searching') : t('empty')}</Command.Empty>
              {records.length ? (
                <Command.Group heading={t('records')} className="[&_[cmdk-group-heading]]:px-2.5 [&_[cmdk-group-heading]]:py-1.5 [&_[cmdk-group-heading]]:text-xs [&_[cmdk-group-heading]]:text-muted">
                  {records.map((hit) => {
                    const Icon = HIT_ICONS[hit.kind];
                    return (
                      <Command.Item key={`${hit.kind}-${hit.id}`} value={`${hit.kind}-${hit.id}`} onSelect={() => go(hit.href)} className={itemClass}>
                        <Icon aria-hidden="true" />
                        <span className="font-medium">
                          <bdi>{hit.title}</bdi>
                        </span>
                        <span className="min-w-0 flex-1 truncate text-xs text-muted">
                          <bdi>{hit.subtitle}</bdi>
                        </span>
                        <span className="text-[0.6875rem] text-muted">{t(`kinds.${hit.kind}`)}</span>
                      </Command.Item>
                    );
                  })}
                </Command.Group>
              ) : null}
              {screens.map((group) => (
                <Command.Group key={group.key} heading={tn(`groups.${group.key}`)} className="[&_[cmdk-group-heading]]:px-2.5 [&_[cmdk-group-heading]]:py-1.5 [&_[cmdk-group-heading]]:text-xs [&_[cmdk-group-heading]]:text-muted">
                  {group.items.map((item) => {
                    const Icon = NAV_ICONS[item.key];
                    return (
                      <Command.Item key={item.key} value={item.href} onSelect={() => go(item.href)} className={itemClass}>
                        <Icon aria-hidden="true" />
                        {tn(`items.${item.key}`)}
                      </Command.Item>
                    );
                  })}
                </Command.Group>
              ))}
            </Command.List>
            <div className="flex items-center gap-3 border-t border-line px-3 py-2 text-[0.6875rem] text-muted">
              <span>{t('hint')}</span>
            </div>
          </Command>
        </DialogPrimitive.Content>
      </DialogPrimitive.Portal>
    </DialogPrimitive.Root>
  );
}
