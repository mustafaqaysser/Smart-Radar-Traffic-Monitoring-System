'use client';

import Link from 'next/link';
import { Ban, ChevronDown, ExternalLink, MoreHorizontal, Printer } from 'lucide-react';
import { useLocale, useTranslations } from 'next-intl';
import { useId, useState } from 'react';
import { advanceOrder } from '@/lib/actions/admin/orders';
import type { OrderCard } from '@/lib/admin/orders';
import { DECLINE_REASONS, PREP_CHOICES, primaryNext, type DeclineReason } from '@/lib/admin/order-flow';
import type { OrderStatus } from '@/lib/db/schema';
import { formatNumber } from '@/lib/i18n/format';
import { cn } from '@/lib/utils/cn';
import { Button } from '../ui/button';
import { Dialog, DialogContent, DialogFooter } from '../ui/dialog';
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuLabel, DropdownMenuSeparator, DropdownMenuTrigger } from '../ui/dropdown-menu';
import { Textarea } from '../ui/input';
import { useAdminAction } from '../use-action';

type Card = Pick<OrderCard, 'id' | 'number' | 'status' | 'channel' | 'allowed' | 'prepMinutes'>;

/** The label for moving an order to `next` (completing reads differently for delivery, pickup and the table). */
export function useStepLabel() {
  const t = useTranslations('admin.orders.actions');
  return (next: OrderStatus, channel: Card['channel']) => (next === 'completed' ? t(`completed.${channel}`) : next === 'ready' && channel === 'dine_in' ? t('readyServe') : t(next));
}

/** Decline (from new) or cancel (later): a preset reason in the guest's language, or the team's own words. */
function StopDialog({ order, mode, open, onOpenChange }: { order: Card; mode: 'rejected' | 'cancelled'; open: boolean; onOpenChange: (open: boolean) => void }) {
  const t = useTranslations('admin.orders.stop');
  const tr = useTranslations('admin.orders.reasons');
  const id = useId();
  const [reason, setReason] = useState<DeclineReason>('capacity');
  const [other, setOther] = useState('');
  const [pending, run] = useAdminAction();
  const submit = () =>
    run(() => advanceOrder({ orderId: order.id, next: mode, reasonKey: reason, reason: reason === 'other' ? other : undefined }), {
      success: t(`${mode}.done`, { number: order.number }),
      onSuccess: () => onOpenChange(false),
    });
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent title={t(`${mode}.title`, { number: order.number })} description={t('description')}>
        <fieldset>
          <legend className="mb-2 text-[0.8125rem] font-medium">{t('reason')}</legend>
          <div className="flex flex-col gap-1.5">
            {DECLINE_REASONS.map((key) => (
              <label key={key} className={cn('flex cursor-pointer items-start gap-2.5 rounded-hair border px-3 py-2 text-[0.875rem]', reason === key ? 'border-ink bg-surface' : 'border-line')}>
                <input type="radio" name={`${id}-reason`} value={key} checked={reason === key} onChange={() => setReason(key)} className="mt-1 accent-[var(--c-ink)]" />
                <span>{tr(key)}</span>
              </label>
            ))}
          </div>
        </fieldset>
        {reason === 'other' ? (
          <div className="mt-3">
            <label htmlFor={`${id}-other`} className="mb-1.5 block text-[0.8125rem] font-medium">
              {t('otherLabel')}
            </label>
            <Textarea id={`${id}-other`} value={other} onChange={(e) => setOther(e.target.value)} maxLength={300} rows={3} dir="auto" required />
            <p className="mt-1 text-xs text-muted">{t('otherHint')}</p>
          </div>
        ) : null}
        <DialogFooter>
          <Button onClick={() => onOpenChange(false)} disabled={pending}>
            {t('keep')}
          </Button>
          <Button variant="danger" onClick={submit} disabled={pending || (reason === 'other' && !other.trim())} aria-busy={pending}>
            {t(`${mode}.confirm`)}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

/**
 * The buttons on an order: the main next step (accepting offers the kitchen's minutes), declining or cancelling
 * with a reason, the full order and its kitchen ticket. Hidden for roles that may only look.
 */
export function OrderActions({ order, canManage, size = 'sm', showOpen = true, className }: { order: Card; canManage: boolean; size?: 'sm' | 'md'; showOpen?: boolean; className?: string }) {
  const t = useTranslations('admin.orders.actions');
  const ts = useTranslations('admin.orders.status');
  const locale = useLocale();
  const stepLabel = useStepLabel();
  const [pending, run] = useAdminAction();
  const [stop, setStop] = useState<'rejected' | 'cancelled' | null>(null);
  const next = primaryNext(order.status, order.channel, order.allowed);
  const defaultPrep = order.prepMinutes ?? 20;
  const go = (status: OrderStatus, prepMinutes?: number) => run(() => advanceOrder({ orderId: order.id, next: status, prepMinutes }), { success: t('moved', { number: order.number, step: ts(status) }) });
  const minutes = (n: number) => formatNumber(n, locale);
  const others = order.allowed.filter((s) => s !== next && s !== 'rejected' && s !== 'cancelled');

  return (
    <div className={cn('flex flex-wrap items-center gap-1.5', className)}>
      {canManage && next === 'accepted' ? (
        <div className="flex">
          <Button variant="primary" size={size} disabled={pending} onClick={() => go('accepted', defaultPrep)} className="rounded-e-none">
            {t('acceptFor', { n: minutes(defaultPrep) })}
          </Button>
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="primary" size={size === 'sm' ? 'icon-sm' : 'icon'} disabled={pending} className="rounded-s-none border-s border-on-accent/30" aria-label={t('prepChoose')}>
                <ChevronDown />
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="start">
              <DropdownMenuLabel>{t('prepChoose')}</DropdownMenuLabel>
              {PREP_CHOICES.map((n) => (
                <DropdownMenuItem key={n} onSelect={() => go('accepted', n)}>
                  {t('acceptFor', { n: minutes(n) })}
                </DropdownMenuItem>
              ))}
            </DropdownMenuContent>
          </DropdownMenu>
        </div>
      ) : canManage && next ? (
        <Button variant="primary" size={size} disabled={pending} onClick={() => go(next)}>
          {stepLabel(next, order.channel)}
        </Button>
      ) : null}
      {canManage && order.allowed.includes('rejected') ? (
        <Button variant="danger-outline" size={size} disabled={pending} onClick={() => setStop('rejected')}>
          {t('decline')}
        </Button>
      ) : null}
      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <Button variant="ghost" size={size === 'sm' ? 'icon-sm' : 'icon'} aria-label={t('more', { number: order.number })}>
            <MoreHorizontal />
          </Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent>
          {showOpen ? (
            <DropdownMenuItem asChild>
              <Link href={`/admin/orders/${order.number}`}>
                <ExternalLink aria-hidden="true" />
                {t('open')}
              </Link>
            </DropdownMenuItem>
          ) : null}
          <DropdownMenuItem onSelect={() => window.open(`/admin/orders/${order.number}/ticket?print=1`, '_blank', 'noopener')}>
            <Printer aria-hidden="true" />
            {t('print')}
          </DropdownMenuItem>
          {canManage && others.length ? <DropdownMenuSeparator /> : null}
          {canManage
            ? others.map((s) => (
                <DropdownMenuItem key={s} onSelect={() => go(s)}>
                  {stepLabel(s, order.channel)}
                </DropdownMenuItem>
              ))
            : null}
          {canManage && order.allowed.includes('cancelled') ? (
            <>
              <DropdownMenuSeparator />
              <DropdownMenuItem tone="danger" onSelect={() => setStop('cancelled')}>
                <Ban aria-hidden="true" />
                {t('cancel')}
              </DropdownMenuItem>
            </>
          ) : null}
        </DropdownMenuContent>
      </DropdownMenu>
      {stop ? <StopDialog order={order} mode={stop} open onOpenChange={(open) => !open && setStop(null)} /> : null}
    </div>
  );
}
