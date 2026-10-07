'use client';

import Link from 'next/link';
import { CalendarDays, Gift, HandPlatter, MessageSquareQuote, ReceiptText, Ticket, type LucideIcon } from 'lucide-react';
import { useTranslations } from 'next-intl';
import type { ActivityItem } from '@/lib/admin/dashboard';
import { RelativeTime } from '../shell/relative-time';
import { EmptyState } from '../ui/page-header';

const ICONS: Record<ActivityItem['kind'], LucideIcon> = { order: ReceiptText, reservation: CalendarDays, request: HandPlatter, review: MessageSquareQuote, giftCard: Gift, ticket: Ticket };

/** What just happened, newest first; codes and names are isolated so they read correctly in either direction. */
export function ActivityFeed({ items, timeZone }: { items: ActivityItem[]; timeZone: string }) {
  const t = useTranslations('admin.dashboard.activity');
  if (!items.length) return <EmptyState title={t('empty')} />;
  return (
    <ol className="divide-y divide-line">
      {items.map((item) => {
        const Icon = ICONS[item.kind];
        // First-strong isolates (the plain-text <bdi>) keep codes and names from reordering the sentence.
        const values = Object.fromEntries(Object.entries(item.values).map(([k, v]) => [k, `\u2068${String(v)}\u2069`]));
        const body = (
          <>
            <Icon className="mt-0.5 size-4 shrink-0 text-muted" aria-hidden="true" />
            <span className="min-w-0 flex-1">
              <span className="block text-[0.8125rem]">{t(item.key, values)}</span>
              <RelativeTime iso={item.at} timeZone={timeZone} className="block text-[0.6875rem] text-muted" />
            </span>
          </>
        );
        return (
          <li key={item.id}>
            {item.href ? (
              <Link href={item.href} className="flex gap-3 px-5 py-2.5 no-underline hover-capable:hover:bg-surface/60">
                {body}
              </Link>
            ) : (
              <div className="flex gap-3 px-5 py-2.5">{body}</div>
            )}
          </li>
        );
      })}
    </ol>
  );
}
