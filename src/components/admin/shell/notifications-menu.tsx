'use client';

import Link from 'next/link';
import { Bell } from 'lucide-react';
import { useTranslations } from 'next-intl';
import { useState } from 'react';
import { markNotificationsRead } from '@/lib/actions/admin/shell';
import { formatNumber } from '@/lib/i18n/format';
import { cn } from '@/lib/utils/cn';
import { Button } from '../ui/button';
import { Popover, PopoverContent, PopoverTrigger } from '../ui/popover';
import { useAdminAction } from '../use-action';
import { useLive } from './live';
import { RelativeTime } from './relative-time';

export interface NotificationView {
  id: string;
  title: string;
  body: string | null;
  href: string | null;
  at: string;
  unread: boolean;
}

/** The bell: the latest notices for this staff member, with a live unread count. */
export function NotificationsMenu({ items, locale }: { items: NotificationView[]; locale: string }) {
  const t = useTranslations('admin.shell.notifications');
  const live = useLive();
  const [open, setOpen] = useState(false);
  const [, run] = useAdminAction();
  const unread = live?.notifications.unread ?? items.filter((n) => n.unread).length;
  const openItem = (n: NotificationView) => {
    setOpen(false);
    if (n.unread) void run(() => markNotificationsRead([n.id]));
  };
  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button variant="ghost" size="icon" className="relative" aria-label={unread ? t('labelUnread', { count: unread, n: formatNumber(unread, locale) }) : t('label')}>
          <Bell className="size-[1.125rem]" />
          {unread ? <span aria-hidden="true" className="absolute end-1.5 top-1.5 size-2 rounded-full bg-accent ring-2 ring-bg" /> : null}
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-[22rem] max-w-[calc(100vw-1.5rem)] p-0">
        <div className="flex items-center justify-between border-b border-line px-4 py-2.5">
          <p className="text-[0.875rem] font-semibold">{t('title')}</p>
          {unread ? (
            <Button variant="link" size="sm" onClick={() => run(() => markNotificationsRead('all'))}>
              {t('markAll')}
            </Button>
          ) : null}
        </div>
        {items.length ? (
          <ul className="max-h-[60vh] overflow-y-auto scrollbar-thin">
            {items.map((n) => {
              const inner = (
                <>
                  <span aria-hidden="true" className={cn('mt-1.5 size-2 shrink-0 rounded-full', n.unread ? 'bg-accent' : 'bg-transparent')} />
                  <span className="min-w-0 flex-1">
                    <span className={cn('block text-[0.8125rem]', n.unread && 'font-medium')}>
                      {n.title}
                      {n.unread ? <span className="sr-only"> ({t('new')})</span> : null}
                    </span>
                    {n.body ? (
                      <span className="block truncate text-xs text-muted">
                        <bdi>{n.body}</bdi>
                      </span>
                    ) : null}
                    <RelativeTime iso={n.at} className="block text-[0.6875rem] text-muted" />
                  </span>
                </>
              );
              return (
                <li key={n.id} className="border-b border-line last:border-0">
                  {n.href ? (
                    <Link href={n.href} onClick={() => openItem(n)} className="flex gap-2.5 px-4 py-2.5 no-underline hover-capable:hover:bg-surface">
                      {inner}
                    </Link>
                  ) : (
                    <div className="flex gap-2.5 px-4 py-2.5">{inner}</div>
                  )}
                </li>
              );
            })}
          </ul>
        ) : (
          <p className="px-4 py-8 text-center text-[0.8125rem] text-muted">{t('empty')}</p>
        )}
        <div className="border-t border-line px-4 py-2">
          <Link href="/admin/notifications" onClick={() => setOpen(false)} className="text-[0.8125rem] text-link">
            {t('all')}
          </Link>
        </div>
      </PopoverContent>
    </Popover>
  );
}
