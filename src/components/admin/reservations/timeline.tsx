'use client';

import { DndContext, PointerSensor, useDraggable, useDroppable, useSensor, useSensors, type DragEndEvent } from '@dnd-kit/core';
import { CakeSlice, MessageSquareText, Utensils } from 'lucide-react';
import { useLocale, useTranslations } from 'next-intl';
import { Fragment, useEffect, useRef, useState } from 'react';
import { assignReservationTables } from '@/lib/actions/admin/reservations';
import { TIMELINE_PX_PER_MIN } from '@/lib/admin/reservation-flow';
import type { DayTable, DayView } from '@/lib/server/reservations-staff';
import { serviceMinutes } from '@/lib/domain/hours';
import { formatNumber, formatWallTime } from '@/lib/i18n/format';
import { formatTime } from '@/lib/time/zoned';
import { cn } from '@/lib/utils/cn';
import { useMinuteClock } from '../shell/relative-time';
import { useAdminAction } from '../use-action';
import { BookingSheet } from './booking-sheet';

type Booking = DayView['bookings'][number];

const ROW = 40;
const LABEL_WIDTH = 112;

const STATUS_STYLE: Record<string, string> = {
  pending: 'border border-dashed border-warning bg-warning/10',
  confirmed: 'border border-ink/50 bg-raised border-s-[3px] border-s-accent',
  seated: 'border border-success bg-success/15 border-s-[3px]',
  completed: 'border border-line bg-surface text-muted',
  no_show: 'border border-danger/60 bg-danger/10 text-danger line-through decoration-1',
};

function Block({ booking, left, width, onOpen, draggable }: { booking: Booking; left: number; width: number; onOpen: () => void; draggable: boolean }) {
  const t = useTranslations('admin.reservations.timeline');
  const ts = useTranslations('admin.reservations.status');
  const locale = useLocale();
  const { attributes, listeners, setNodeRef, transform, isDragging } = useDraggable({ id: booking.id, disabled: !draggable });
  const style = { insetInlineStart: left, width, transform: transform ? `translate3d(${transform.x}px, ${transform.y}px, 0)` : undefined };
  return (
    <button
      ref={setNodeRef}
      type="button"
      onClick={onOpen}
      {...(draggable ? { ...listeners, ...attributes, role: undefined, tabIndex: undefined } : {})}
      aria-label={t('blockLabel', { name: booking.name, party: formatNumber(booking.partySize, locale), time: formatWallTime(booking.time, locale), status: ts(booking.status) })}
      style={style}
      className={cn(
        'absolute top-1 flex h-8 items-center gap-1.5 overflow-hidden rounded-hair px-2 text-start text-xs shadow-none transition-shadow',
        STATUS_STYLE[booking.status] ?? STATUS_STYLE.confirmed,
        isDragging ? 'z-30 cursor-grabbing shadow-[var(--cast-x)_var(--cast-y)_0_var(--c-shade)]' : 'z-10 hover-capable:hover:shadow-[0_0_0_1px_var(--c-ink)]',
        draggable && !isDragging && 'cursor-grab',
      )}
    >
      <span className="shrink-0 font-semibold tabular">{formatNumber(booking.partySize, locale)}</span>
      <span className="min-w-0 truncate">
        <bdi>{booking.name}</bdi>
      </span>
      {booking.occasion ? <CakeSlice className="size-3 shrink-0" aria-hidden="true" /> : null}
      {booking.notes || booking.dietaryNotes || booking.staffNotes ? <MessageSquareText className="size-3 shrink-0" aria-hidden="true" /> : null}
    </button>
  );
}

function Row({ table, children, highlight }: { table: DayTable; children: React.ReactNode; highlight: boolean }) {
  const { setNodeRef, isOver } = useDroppable({ id: table.id });
  return (
    <div ref={setNodeRef} className={cn('relative border-b border-line', isOver && 'bg-accent/10', highlight && 'bg-sun/5')} style={{ height: ROW }}>
      {children}
    </div>
  );
}

/**
 * The service day as a floor timeline: one row per table (grouped by area), time across. Bookings are blocks
 * spanning their turn time; open one for its details and actions, or drag a one-table booking onto another
 * table to move it (the same move is available from the booking panel for keyboard users).
 */
export function ReservationTimeline({ day, date, timeZone, today, canManage }: { day: DayView; date: string; timeZone: string; today: boolean; canManage: boolean }) {
  const t = useTranslations('admin.reservations.timeline');
  const ta = useTranslations('admin.reservations.areas');
  const locale = useLocale();
  const now = useMinuteClock();
  const scroller = useRef<HTMLDivElement>(null);
  const [openId, setOpenId] = useState<string | null>(null);
  const [, run] = useAdminAction();
  const sensors = useSensors(useSensor(PointerSensor, { activationConstraint: { distance: 6 } }));
  const { start, end } = day.span;
  const width = (end - start) * TIMELINE_PX_PER_MIN;
  const x = (minutes: number) => (minutes - start) * TIMELINE_PX_PER_MIN;
  const hours = Array.from({ length: Math.floor((end - start) / 60) + 1 }, (_, i) => start + i * 60);
  const nowMin = today && now ? serviceMinutes(new Date(now), date, timeZone) : null;
  const live = day.bookings.filter((b) => b.status !== 'cancelled');
  const areas = [...new Set(day.tables.map((tb) => tb.area))];
  const open = live.find((b) => b.id === openId) ?? day.bookings.find((b) => b.id === openId) ?? null;

  // Start scrolled to just before now (today) or to the first booking.
  useEffect(() => {
    const el = scroller.current;
    if (!el) return;
    const target = nowMin ?? live[0]?.startMin ?? start;
    const offset = Math.max(0, x(target) - 160);
    el.scrollLeft = getComputedStyle(el).direction === 'rtl' ? -offset : offset;
    // Only on first render for this day.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [date]);

  const onDragEnd = (e: DragEndEvent) => {
    const booking = live.find((b) => b.id === e.active.id);
    const target = e.over?.id ? String(e.over.id) : null;
    if (!booking || !target || booking.tableIds.length !== 1 || booking.tableIds[0] === target) return;
    const table = day.tables.find((tb) => tb.id === target);
    void run(() => assignReservationTables({ id: booking.id, tableIds: [target] }), { success: t('moved', { name: booking.name, table: table?.label ?? '' }) });
  };

  return (
    <>
      <DndContext sensors={sensors} onDragEnd={onDragEnd}>
        <div ref={scroller} className="relative max-h-[70vh] overflow-auto rounded-soft border border-line bg-raised scrollbar-thin" role="region" aria-label={t('label')} tabIndex={0}>
          <div className="relative" style={{ width: width + LABEL_WIDTH }}>
            {/* Hour header */}
            <div className="sticky top-0 z-20 flex border-b border-line bg-raised" style={{ height: 32 }}>
              <div className="sticky start-0 z-30 shrink-0 border-e border-line bg-raised" style={{ width: LABEL_WIDTH }} />
              <div className="relative" style={{ width }}>
                {hours.map((h) => (
                  <span key={h} className="absolute top-2 -translate-x-1/2 text-[0.6875rem] text-muted tabular rtl:translate-x-1/2" style={{ insetInlineStart: x(h) }}>
                    {formatWallTime(formatTime(h), locale)}
                  </span>
                ))}
              </div>
            </div>
            {areas.map((area) => (
              <Fragment key={area}>
                <div className="sticky top-8 z-[15] flex border-b border-line bg-surface/90 backdrop-blur" style={{ height: 24 }}>
                  <div className="sticky start-0 px-3 text-[0.6875rem] font-medium leading-6 text-muted" style={{ width: LABEL_WIDTH }}>
                    {ta(area)}
                  </div>
                </div>
                {day.tables
                  .filter((tb) => tb.area === area)
                  .map((table) => {
                    const blocks = live.filter((b) => b.tableIds.includes(table.id));
                    return (
                      <div key={table.id} className="flex">
                        <div className="sticky start-0 z-[15] flex shrink-0 items-center justify-between gap-1 border-e border-b border-line bg-raised px-3 text-[0.8125rem]" style={{ width: LABEL_WIDTH, height: ROW }}>
                          <span className="font-medium">{table.label}</span>
                          <span className="text-[0.6875rem] text-muted tabular">{t('seats', { min: formatNumber(table.minSeats, locale), max: formatNumber(table.maxSeats, locale) })}</span>
                        </div>
                        <Row table={table} highlight={!table.reservable}>
                          <div style={{ width }} className="relative h-full">
                            {hours.map((h) => (
                              <span key={h} aria-hidden="true" className="absolute inset-y-0 w-px bg-line" style={{ insetInlineStart: x(h) }} />
                            ))}
                            {day.holds
                              .filter((hd) => hd.tableIds.includes(table.id))
                              .map((hd) => (
                                <span
                                  key={hd.id}
                                  title={t('hold')}
                                  className="absolute top-1 h-8 rounded-hair border border-dashed border-field bg-[repeating-linear-gradient(135deg,transparent_0_6px,var(--c-line)_6px_8px)]"
                                  style={{ insetInlineStart: x(hd.startMin), width: (hd.endMin - hd.startMin) * TIMELINE_PX_PER_MIN }}
                                >
                                  <span className="sr-only">{t('hold')}</span>
                                </span>
                              ))}
                            {blocks.map((b) => (
                              <Block key={b.id} booking={b} left={x(b.startMin)} width={Math.max(28, (b.endMin - b.startMin) * TIMELINE_PX_PER_MIN - 2)} onOpen={() => setOpenId(b.id)} draggable={canManage && b.tableIds.length === 1 && ['pending', 'confirmed', 'seated'].includes(b.status)} />
                            ))}
                          </div>
                        </Row>
                      </div>
                    );
                  })}
              </Fragment>
            ))}
            {nowMin !== null && nowMin >= start && nowMin <= end ? (
              <div aria-hidden="true" className="pointer-events-none absolute top-0 bottom-0 z-20 w-0.5 bg-accent" style={{ insetInlineStart: LABEL_WIDTH + x(nowMin) }}>
                <span className="absolute -top-0.5 start-1 rounded-hair bg-accent px-1 text-[0.625rem] font-medium text-on-accent">{t('now')}</span>
              </div>
            ) : null}
          </div>
        </div>
      </DndContext>
      {live.length === 0 ? (
        <p className="mt-3 flex items-center gap-2 text-[0.8125rem] text-muted">
          <Utensils className="size-4" aria-hidden="true" />
          {t('empty')}
        </p>
      ) : null}
      {open ? <BookingSheet booking={open} tables={day.tables} bookings={day.bookings} timeZone={timeZone} canManage={canManage} onClose={() => setOpenId(null)} /> : null}
    </>
  );
}
