import 'server-only';
import { lt } from 'drizzle-orm';
import { db } from '@/lib/db/client';
import { reservationHolds } from '@/lib/db/schema';
import { purgeExpiredRateLimits } from '@/lib/services/rate-limit';
import { expireUnpaidBookings } from './events';
import { deliverScheduledGiftCards } from './gift-cards';
import { expireUnpaidOrders } from './orders';
import { expireUnpaidDeposits, processAllWaitlists, sendDueReminders } from './reservations';

/**
 * Scheduled jobs. Each runs from the cron endpoint (Vercel Cron, a crontab calling `npm run cron -- <job>`)
 * and can be started by hand from the admin. Every job is idempotent, so overlapping runs are harmless.
 */
export const JOBS = {
  /** Hourly: reminder emails, lapsed deposits and unpaid orders, waitlist offers. */
  reminders: async () => ({
    reminders: await sendDueReminders(),
    expiredDeposits: await expireUnpaidDeposits(),
    expiredOrders: await expireUnpaidOrders(),
    expiredTickets: await expireUnpaidBookings(),
    waitlistOffers: await processAllWaitlists(),
  }),
  /** Every 15 minutes: gift cards scheduled for delivery. */
  'gift-cards': async () => ({ delivered: await deliverScheduledGiftCards() }),
  /** Daily: clears expired holds and rate-limit windows. */
  housekeeping: async () => {
    const holds = await db.delete(reservationHolds).where(lt(reservationHolds.expiresAt, new Date())).returning({ id: reservationHolds.id });
    await purgeExpiredRateLimits();
    return { expiredHolds: holds.length };
  },
} satisfies Record<string, () => Promise<Record<string, number>>>;

export type JobName = keyof typeof JOBS;

export function isJobName(value: string): value is JobName {
  return Object.hasOwn(JOBS, value);
}

export async function runJob(name: JobName): Promise<Record<string, number>> {
  return JOBS[name]();
}
