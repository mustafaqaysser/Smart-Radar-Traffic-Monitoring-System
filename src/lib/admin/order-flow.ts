import type { OrderChannel, OrderStatus } from '@/lib/db/schema';

/** Reasons offered when declining or cancelling an order; each is emailed to the guest in their language. */
export const DECLINE_REASONS = ['capacity', 'unavailable', 'area', 'closing', 'other'] as const;
export type DeclineReason = (typeof DECLINE_REASONS)[number];

/** The board's columns. */
export const BOARD_COLUMNS: { key: 'new' | 'kitchen' | 'out'; statuses: OrderStatus[] }[] = [
  { key: 'new', statuses: ['placed'] },
  { key: 'kitchen', statuses: ['accepted', 'preparing'] },
  { key: 'out', statuses: ['ready', 'out_for_delivery'] },
];

/** The main forward step offered for an order (the others live in its menu). */
export function primaryNext(status: OrderStatus, channel: OrderChannel, allowed: OrderStatus[]): OrderStatus | null {
  const preferred: Partial<Record<OrderStatus, OrderStatus[]>> = {
    placed: ['accepted'],
    accepted: ['preparing'],
    preparing: channel === 'delivery' ? ['out_for_delivery', 'ready'] : ['ready'],
    ready: channel === 'delivery' ? ['out_for_delivery'] : ['completed'],
    out_for_delivery: ['completed'],
  };
  return preferred[status]?.find((s) => allowed.includes(s)) ?? null;
}

/** Prep-time choices offered when accepting (minutes). */
export const PREP_CHOICES = [10, 15, 20, 25, 30, 45, 60] as const;

/** Badge tone for each order status. */
export const STATUS_TONE: Record<OrderStatus, 'neutral' | 'accent' | 'success' | 'warning' | 'danger' | 'sun' | 'outline'> = {
  pending_payment: 'outline',
  placed: 'accent',
  accepted: 'sun',
  preparing: 'sun',
  ready: 'success',
  out_for_delivery: 'success',
  completed: 'neutral',
  rejected: 'danger',
  cancelled: 'danger',
};
