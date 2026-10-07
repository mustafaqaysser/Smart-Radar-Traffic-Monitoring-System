import 'server-only';
import { and, desc, gte, inArray, lt } from 'drizzle-orm';
import type { Permission } from '@/lib/auth/permissions';
import { db } from '@/lib/db/client';
import * as s from '@/lib/db/schema';
import { tr } from '@/lib/i18n/localized';
import { addDays, localToUtc, toDateString } from '@/lib/time/zoned';
import type { AdminScope } from './context';
import { csvMoney } from './csv';

const DATE = /^\d{4}-\d{2}-\d{2}$/;

export interface ExportRange {
  from: string;
  to: string;
}

/** The requested date range (inclusive, in the scope's zone), defaulting to the last 30 days, at most a year. */
export function exportRange(scope: AdminScope, params: URLSearchParams, now = new Date()): ExportRange {
  const today = toDateString(now, scope.timeZone);
  let to = params.get('to') ?? '';
  let from = params.get('from') ?? '';
  if (!DATE.test(to)) to = today;
  if (!DATE.test(from) || from > to) from = addDays(to, -29);
  if (from < addDays(to, -366)) from = addDays(to, -366);
  return { from, to };
}

export function rangeInstants(scope: AdminScope, range: ExportRange): [Date, Date] {
  return [localToUtc(range.from, 0, scope.timeZone, true) as Date, localToUtc(addDays(range.to, 1), 0, scope.timeZone, true) as Date];
}

export interface ExportFile {
  filename: string;
  header: string[];
  rows: (string | number | null)[][];
}

export interface ExportDefinition {
  permission: Permission;
  build: (scope: AdminScope, params: URLSearchParams, locale: string) => Promise<ExportFile>;
}

/** Every CSV the back-office offers; reports and lists add theirs here. */
export const EXPORTS: Record<string, ExportDefinition> = {
  orders: {
    permission: 'reports:view',
    build: async (scope, params, locale) => {
      const range = exportRange(scope, params);
      const [start, end] = rangeInstants(scope, range);
      const rows = await db
        .select()
        .from(s.orders)
        .where(and(inArray(s.orders.branchId, scope.branchIds), gte(s.orders.createdAt, start), lt(s.orders.createdAt, end)))
        .orderBy(desc(s.orders.createdAt));
      const branch = new Map(scope.branches.map((b) => [b.id, tr(b.shortName, locale)]));
      return {
        filename: `orders-${range.from}-to-${range.to}.csv`,
        header: ['number', 'placed_at', 'house', 'channel', 'status', 'name', 'email', 'phone', 'subtotal', 'discount', 'promo_code', 'delivery_fee', 'service_charge', 'vat', 'tip', 'gift_card', 'loyalty_discount', 'total', 'payment_method', 'payment_status'],
        rows: rows.map((o) => [
          o.number,
          o.createdAt.toISOString(),
          branch.get(o.branchId) ?? o.branchId,
          o.channel,
          o.status,
          o.name,
          o.email,
          o.phone,
          csvMoney(o.subtotal),
          csvMoney(o.discount),
          o.promoCode,
          csvMoney(o.deliveryFee),
          csvMoney(o.serviceCharge),
          csvMoney(o.tax),
          csvMoney(o.tip),
          csvMoney(o.giftCardAmount),
          csvMoney(o.loyaltyDiscount),
          csvMoney(o.total),
          o.paymentMethod,
          o.paymentStatus,
        ]),
      };
    },
  },
};
