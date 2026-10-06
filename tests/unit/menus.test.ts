import { describe, expect, it } from 'vitest';
import { activeSeasons, isServing, menuAvailable, minutesUntilServing, servingNow, type ScheduledMenu } from '@/lib/domain/menus';

const TZ = 'Asia/Riyadh'; // UTC+3, no DST
const at = (local: string) => new Date(`${local}+03:00`);
const EVERY = [0, 1, 2, 3, 4, 5, 6];

const breakfast: ScheduledMenu = { id: 'b', slug: 'morning', schedule: [{ weekdays: EVERY, start: '07:00', end: '11:30' }], branchIds: null, seasonalModeId: null };
const suhoor: ScheduledMenu = { id: 's', slug: 'suhoor', schedule: [{ weekdays: EVERY, start: '22:00', end: '03:00' }], branchIds: null, seasonalModeId: 'ramadan' };
const drinks: ScheduledMenu = { id: 'd', slug: 'cups', schedule: [], branchIds: null, seasonalModeId: null };
const tasting: ScheduledMenu = { id: 't', slug: 'sundial', schedule: [{ weekdays: [3, 4, 5, 6], start: '19:30', end: '22:00' }], branchIds: ['br_al-balad'], seasonalModeId: null };

describe('menu schedules', () => {
  it('serves within a window and not outside it', () => {
    expect(isServing(breakfast, at('2026-10-06T07:00:00'), TZ)).toBe(true);
    expect(isServing(breakfast, at('2026-10-06T11:29:00'), TZ)).toBe(true);
    expect(isServing(breakfast, at('2026-10-06T11:30:00'), TZ)).toBe(false);
    expect(isServing(breakfast, at('2026-10-06T06:59:00'), TZ)).toBe(false);
  });

  it('treats an empty schedule as all day', () => {
    expect(isServing(drinks, at('2026-10-06T03:15:00'), TZ)).toBe(true);
  });

  it('carries a window past midnight into the next calendar day', () => {
    expect(isServing(suhoor, at('2026-10-06T23:00:00'), TZ)).toBe(true);
    expect(isServing(suhoor, at('2026-10-07T02:59:00'), TZ)).toBe(true);
    expect(isServing(suhoor, at('2026-10-07T03:00:00'), TZ)).toBe(false);
  });

  it('respects weekdays, including the day an after-midnight window started', () => {
    // 2026-10-07 is a Wednesday (3); Tuesday 21:00 is outside, Wednesday 20:00 inside.
    expect(isServing(tasting, at('2026-10-06T20:00:00'), TZ)).toBe(false);
    expect(isServing(tasting, at('2026-10-07T20:00:00'), TZ)).toBe(true);
  });

  it('uses the branch time zone, not the server zone', () => {
    // 05:00 UTC is 08:00 in Riyadh.
    expect(isServing(breakfast, new Date('2026-10-06T05:00:00Z'), TZ)).toBe(true);
    expect(isServing(breakfast, new Date('2026-10-06T05:00:00Z'), 'Europe/London')).toBe(false);
  });

  it('counts minutes until the next service', () => {
    expect(minutesUntilServing(breakfast, at('2026-10-06T06:30:00'), TZ)).toBe(30);
    expect(minutesUntilServing(breakfast, at('2026-10-06T12:00:00'), TZ)).toBe(19 * 60);
    expect(minutesUntilServing(breakfast, at('2026-10-06T08:00:00'), TZ)).toBe(0);
  });

  it('filters by branch and active season', () => {
    const none = new Set<string>();
    expect(menuAvailable(tasting, 'br_wadi-hanifah', none)).toBe(false);
    expect(menuAvailable(tasting, 'br_al-balad', none)).toBe(true);
    expect(menuAvailable(suhoor, null, none)).toBe(false);
    expect(menuAvailable(suhoor, null, new Set(['ramadan']))).toBe(true);
    const list = servingNow([breakfast, suhoor, drinks], null, at('2026-10-06T08:00:00'), TZ, none).map((m) => m.slug);
    expect(list).toEqual(['morning', 'cups']);
  });

  it('finds active seasons by local date, inclusive', () => {
    const modes = [
      { id: 'a', startDate: '2026-10-01', endDate: '2026-10-06', isEnabled: true },
      { id: 'b', startDate: '2026-10-07', endDate: '2026-10-09', isEnabled: true },
      { id: 'c', startDate: '2026-10-01', endDate: '2026-10-30', isEnabled: false },
    ];
    expect(activeSeasons(modes, '2026-10-06').map((m) => m.id)).toEqual(['a']);
  });
});
