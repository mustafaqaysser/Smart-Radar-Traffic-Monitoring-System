'use server';

import { eq } from 'drizzle-orm';
import { getTranslations } from 'next-intl/server';
import { z } from 'zod';
import { getAdminScope } from '@/lib/admin/context';
import { DECLINE_REASONS } from '@/lib/admin/order-flow';
import { db } from '@/lib/db/client';
import * as s from '@/lib/db/schema';
import { TAGS } from '@/lib/queries/cache';
import { audit } from '@/lib/server/audit';
import { allowedNext, setOrderStatus } from '@/lib/server/orders';
import { fail, ok, type ActionResult } from '../result';
import { actorOf, staffAction } from './guard';

const advanceSchema = z.object({
  orderId: z.string().min(1).max(64),
  next: z.enum(s.ORDER_STATUSES),
  prepMinutes: z.number().int().min(5).max(180).optional(),
  reasonKey: z.enum(DECLINE_REASONS).optional(),
  reason: z.string().trim().max(300).optional(),
});

/**
 * Moves an order to its next step from the board, the kitchen display or the order page. Declining or
 * cancelling needs a reason (it is emailed to the guest); accepting may set the kitchen's minutes.
 */
export async function advanceOrder(input: z.input<typeof advanceSchema>): Promise<ActionResult<{ status: s.OrderStatus }>> {
  return staffAction('orders:manage', advanceSchema, input, async (d, user) => {
    const scope = await getAdminScope(user);
    const order = await db.query.orders.findFirst({ where: eq(s.orders.id, d.orderId) });
    if (!order || !scope.branchIds.includes(order.branchId)) return fail('notFound');
    if (!allowedNext(order).includes(d.next)) return fail('transition');
    let reason: string | null = null;
    if (d.next === 'rejected' || d.next === 'cancelled') {
      // A preset reason is written in the guest's language, because it goes into their email.
      if (d.reasonKey && d.reasonKey !== 'other') reason = (await getTranslations({ locale: order.locale, namespace: 'admin.orders.reasons' }))(d.reasonKey);
      else reason = d.reason || null;
      if (!reason) return fail('validation', { reason: 'required' });
    }
    const updated = await setOrderStatus(order.id, d.next, actorOf(user), { note: reason, prepMinutes: d.prepMinutes ?? null });
    if (!updated) return fail('conflict');
    return ok({ status: updated.status });
  });
}

const switchesSchema = z.object({
  branchId: z.string().min(1).max(64),
  orderingPaused: z.boolean().optional(),
  busyMode: z.boolean().optional(),
});

/** Pause online ordering at a house, or add the busy-kitchen minutes to every promise. */
export async function setOrderingSwitches(input: z.input<typeof switchesSchema>): Promise<ActionResult<null>> {
  return staffAction(
    'orders:manage',
    switchesSchema,
    input,
    async (d, user) => {
      const scope = await getAdminScope(user);
      if (!scope.branchIds.includes(d.branchId)) return fail('notFound');
      const set: Partial<typeof s.branches.$inferInsert> = { updatedAt: new Date() };
      if (d.orderingPaused !== undefined) set.orderingPaused = d.orderingPaused;
      if (d.busyMode !== undefined) set.busyMode = d.busyMode;
      await db.update(s.branches).set(set).where(eq(s.branches.id, d.branchId));
      await audit({ actor: actorOf(user), action: 'branch.ordering', entity: 'branch', entityId: d.branchId, diff: { orderingPaused: d.orderingPaused, busyMode: d.busyMode } });
      return ok(null);
    },
    { tags: [TAGS.branches] },
  );
}
