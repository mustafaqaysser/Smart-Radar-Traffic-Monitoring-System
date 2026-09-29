/**
 * Kitchen throttling for online orders: at most N orders may be promised in any window of W minutes.
 * ASAP orders get the first window after the prep time that has room and is within ordering hours;
 * scheduled orders choose among windows that have room.
 */

export interface ThrottleRules {
  maxOrdersPerWindow: number;
  windowMinutes: number;
  basePrepMinutes: number;
  busyExtraMinutes: number;
}

export interface OrderingInterval {
  start: Date;
  end: Date;
}

/** Start of the window containing `t` (windows are aligned to the epoch in minutes). */
export function windowStart(t: Date, windowMinutes: number): Date {
  const w = windowMinutes * 60000;
  return new Date(Math.floor(t.getTime() / w) * w);
}

export function countInWindow(promises: Date[], start: Date, windowMinutes: number): number {
  const end = start.getTime() + windowMinutes * 60000;
  return promises.filter((p) => p.getTime() >= start.getTime() && p.getTime() < end).length;
}

export function prepMinutes(rules: ThrottleRules, busy: boolean, itemsMaxPrep = 0): number {
  return Math.max(rules.basePrepMinutes, itemsMaxPrep) + (busy ? rules.busyExtraMinutes : 0);
}

/**
 * Earliest promise time for an ASAP order, or null when nothing fits in the given ordering intervals.
 * `promises` are the promised times of active orders.
 */
export function earliestAsap(now: Date, prep: number, rules: ThrottleRules, promises: Date[], intervals: OrderingInterval[]): Date | null {
  const earliest = new Date(now.getTime() + prep * 60000);
  const sorted = [...intervals].sort((a, b) => a.start.getTime() - b.start.getTime());
  for (const iv of sorted) {
    if (iv.end <= earliest) continue;
    let candidate = earliest > iv.start ? earliest : new Date(iv.start.getTime() + prep * 60000);
    // Walk window by window.
    for (let guard = 0; guard < 500 && candidate < iv.end; guard++) {
      const ws = windowStart(candidate, rules.windowMinutes);
      if (countInWindow(promises, ws, rules.windowMinutes) < rules.maxOrdersPerWindow) return candidate;
      candidate = new Date(ws.getTime() + rules.windowMinutes * 60000);
    }
  }
  return null;
}

export interface ScheduleSlot {
  at: Date;
  available: boolean;
}

/** Scheduled-order slots at `slotMinutes` steps inside the intervals, with capacity applied. */
export function scheduleSlots(now: Date, prep: number, rules: ThrottleRules, promises: Date[], intervals: OrderingInterval[], slotMinutes: number): ScheduleSlot[] {
  const min = now.getTime() + prep * 60000;
  const out: ScheduleSlot[] = [];
  const step = slotMinutes * 60000;
  for (const iv of intervals) {
    // First slot is the first step boundary after the interval opens plus prep time.
    const first = Math.ceil((iv.start.getTime() + prep * 60000) / step) * step;
    for (let t = first; t <= iv.end.getTime(); t += step) {
      if (t < min) continue;
      const at = new Date(t);
      const full = countInWindow(promises, windowStart(at, rules.windowMinutes), rules.windowMinutes) >= rules.maxOrdersPerWindow;
      out.push({ at, available: !full });
    }
  }
  return out;
}
