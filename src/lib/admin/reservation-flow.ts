import type { ReservationStatus } from '@/lib/db/schema';

/**
 * What the team may do to a booking from each status. Mistakes can be undone: a no-show who arrives late can
 * be seated, a finished table reopened, a cancelled booking restored if its tables are still free.
 */
export const STAFF_TRANSITIONS: Record<ReservationStatus, ReservationStatus[]> = {
  pending: ['confirmed', 'cancelled'],
  confirmed: ['seated', 'no_show', 'cancelled'],
  seated: ['completed', 'confirmed'],
  completed: ['seated'],
  no_show: ['seated', 'confirmed'],
  cancelled: ['confirmed'],
};

export const RESERVATION_TONE: Record<ReservationStatus, 'neutral' | 'accent' | 'success' | 'warning' | 'danger' | 'sun' | 'outline'> = {
  pending: 'warning',
  confirmed: 'outline',
  seated: 'success',
  completed: 'neutral',
  no_show: 'danger',
  cancelled: 'danger',
};

/** Timeline scale: pixels per minute. */
export const TIMELINE_PX_PER_MIN = 2;
