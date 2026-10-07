'use client';

import Link from 'next/link';
import { useRouter } from 'next/navigation';
import { ArrowLeft, BellOff, BellRing, Maximize, Minimize, ShoppingBag, Truck, Utensils } from 'lucide-react';
import { useLocale, useTranslations } from 'next-intl';
import { useEffect, useRef, useState } from 'react';
import { advanceOrder } from '@/lib/actions/admin/orders';
import { setAdminSound } from '@/lib/actions/admin/shell';
import type { OrderCard } from '@/lib/admin/orders';
import { primaryNext } from '@/lib/admin/order-flow';
import type { OrderStatus } from '@/lib/db/schema';
import { formatClock, formatNumber } from '@/lib/i18n/format';
import { cn } from '@/lib/utils/cn';
import { formatElapsed, useSecondClock } from '../clock';
import { audioReady, chime, unlockAudio } from '../sound';
import { Button } from '../ui/button';
import { useAdminAction } from '../use-action';

const CHANNEL_ICONS = { delivery: Truck, pickup: ShoppingBag, dine_in: Utensils } as const;

/** Timer colour: calm while within the kitchen's minutes, amber in the last quarter, red once over or late. */
function urgency(card: OrderCard, now: number): 'ok' | 'soon' | 'late' {
  const started = new Date(card.acceptedAt ?? card.createdAt).getTime();
  const budget = (card.prepMinutes ?? 20) * 60_000;
  const promised = card.promisedAt ? new Date(card.promisedAt).getTime() : null;
  if (now - started > budget || (promised !== null && card.status !== 'ready' && now > promised)) return 'late';
  if (now - started > budget * 0.75) return 'soon';
  return 'ok';
}

function Ticket({ card, index, now, canBump, onBump, busy, timeZone }: { card: OrderCard; index: number; now: number; canBump: boolean; onBump: (card: OrderCard, next: OrderStatus) => void; busy: boolean; timeZone: string }) {
  const t = useTranslations('admin.kds');
  const tc = useTranslations('admin.orders.channels');
  const ta = useTranslations('admin.orders.actions');
  const locale = useLocale();
  const Icon = CHANNEL_ICONS[card.channel];
  const next = primaryNext(card.status, card.channel, card.allowed);
  const level = now ? urgency(card, now) : 'ok';
  const started = new Date(card.acceptedAt ?? card.createdAt).getTime();
  const bumpLabel = next === 'preparing' ? t('bump.start') : next === 'ready' ? (card.channel === 'dine_in' ? ta('readyServe') : t('bump.ready')) : next === 'out_for_delivery' ? ta('out_for_delivery') : next === 'completed' ? ta(`completed.${card.channel}`) : null;
  return (
    <article aria-labelledby={`kds-${card.id}`} className={cn('flex flex-col overflow-hidden rounded-soft border-2 bg-raised', card.status === 'ready' ? 'border-success/70 opacity-85' : level === 'late' ? 'border-danger' : level === 'soon' ? 'border-warning' : 'border-line')}>
      <header className={cn('flex items-start justify-between gap-2 px-3 py-2', card.status === 'ready' ? 'bg-success/15' : level === 'late' ? 'bg-danger/20' : level === 'soon' ? 'bg-warning/15' : 'bg-surface')}>
        <div className="min-w-0">
          <h2 id={`kds-${card.id}`} className="flex items-center gap-2 text-lg font-semibold">
            {index < 9 && card.status !== 'ready' ? (
              <kbd aria-hidden="true" className="inline-flex size-6 items-center justify-center rounded-hair border border-field font-mono text-xs">
                {index + 1}
              </kbd>
            ) : null}
            <span className="font-mono" dir="ltr">
              {card.number}
            </span>
          </h2>
          <p className="flex items-center gap-1.5 text-[0.8125rem] text-muted">
            <Icon className="size-3.5" aria-hidden="true" />
            {card.channel === 'dine_in' && card.table ? tc('table', { table: card.table }) : tc(card.channel)}
            {card.promisedAt ? <span>· {t('due', { time: formatClock(new Date(card.promisedAt), locale, timeZone) })}</span> : null}
          </p>
        </div>
        <p className={cn('font-mono text-xl font-semibold tabular', level === 'late' && card.status !== 'ready' ? 'text-danger' : level === 'soon' && card.status !== 'ready' ? 'text-warning' : '')} aria-label={t('elapsed')}>
          {now ? formatElapsed(now - started, locale) : ''}
        </p>
      </header>
      <ul className="flex flex-1 flex-col gap-2 px-3 py-3">
        {card.items.map((i) => (
          <li key={i.id}>
            <p className="flex gap-2 text-base font-semibold">
              <span className="w-7 shrink-0 tabular">{formatNumber(i.quantity, locale)}×</span>
              <span>{i.name}</span>
            </p>
            {i.modifiers.map((m) => (
              <p key={m} className="ps-9 text-[0.875rem] text-muted">
                {m}
              </p>
            ))}
            {i.notes ? (
              <p className="ps-9 text-[0.875rem] font-medium text-warning">
                “<bdi>{i.notes}</bdi>”
              </p>
            ) : null}
          </li>
        ))}
      </ul>
      {card.notes ? (
        <p className="mx-3 mb-3 rounded-hair bg-sun/15 px-2 py-1.5 text-[0.875rem]">
          <bdi>{card.notes}</bdi>
        </p>
      ) : null}
      {canBump && next && bumpLabel ? (
        <button
          type="button"
          disabled={busy}
          onClick={() => onBump(card, next)}
          className={cn('min-h-14 border-t border-line px-3 text-base font-semibold transition-colors disabled:opacity-60', card.status === 'ready' ? 'bg-surface text-ink hover-capable:hover:bg-line' : 'bg-accent text-on-accent hover-capable:hover:bg-[color-mix(in_oklab,var(--c-accent)_86%,var(--c-ink))]')}
        >
          {bumpLabel}
        </button>
      ) : null}
    </article>
  );
}

/**
 * The kitchen display: dark, large, glanceable. Cooking tickets in the order they were accepted, then what is
 * ready to go. Bump with a tap or with the number key shown on the ticket.
 */
export function KitchenDisplay({
  branch,
  branches,
  cards,
  incoming,
  canBump,
  soundPreferred,
  exitHref,
}: {
  branch: { id: string; name: string; timeZone: string };
  branches: { id: string; name: string }[];
  cards: OrderCard[];
  incoming: number;
  canBump: boolean;
  soundPreferred: boolean;
  exitHref: string;
}) {
  const t = useTranslations('admin.kds');
  const ta = useTranslations('admin.orders.actions');
  const ts = useTranslations('admin.orders.status');
  const locale = useLocale();
  const router = useRouter();
  const now = useSecondClock();
  const [pending, run] = useAdminAction();
  const [sound, setSound] = useState(soundPreferred);
  const [full, setFull] = useState(false);
  const cooking = cards.filter((c) => c.status !== 'ready');
  const ready = cards.filter((c) => c.status === 'ready');
  const known = useRef(new Set(cards.map((c) => c.id)));

  const bump = (card: OrderCard, next: OrderStatus) => run(() => advanceOrder({ orderId: card.id, next }), { success: ta('moved', { number: card.number, step: ts(next) }) });

  // New tickets on the screen chime (when sound is on in this browser).
  useEffect(() => {
    const arrived = cards.filter((c) => !known.current.has(c.id));
    for (const c of cards) known.current.add(c.id);
    if (arrived.length && sound) chime();
  }, [cards, sound]);

  useEffect(() => {
    if (!sound || audioReady()) return;
    const unlock = () => void unlockAudio();
    window.addEventListener('pointerdown', unlock, { once: true });
    window.addEventListener('keydown', unlock, { once: true });
    return () => {
      window.removeEventListener('pointerdown', unlock);
      window.removeEventListener('keydown', unlock);
    };
  }, [sound]);

  // Number keys bump the matching cooking ticket.
  useEffect(() => {
    if (!canBump) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.metaKey || e.ctrlKey || e.altKey || (e.target as HTMLElement).closest('input,textarea,select,[role="dialog"]')) return;
      const n = Number(e.key);
      if (!Number.isInteger(n) || n < 1 || n > 9) return;
      const card = cooking[n - 1];
      const next = card ? primaryNext(card.status, card.channel, card.allowed) : null;
      if (card && next) {
        e.preventDefault();
        bump(card, next);
      }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  });

  useEffect(() => {
    const onChange = () => setFull(Boolean(document.fullscreenElement));
    document.addEventListener('fullscreenchange', onChange);
    return () => document.removeEventListener('fullscreenchange', onChange);
  }, []);

  return (
    <div className="admin-dark min-h-dvh bg-bg text-ink">
      <header className="sticky top-0 z-10 flex flex-wrap items-center gap-3 border-b border-line bg-bg/95 px-4 py-2.5 backdrop-blur">
        <Link href={exitHref} className="inline-flex size-9 items-center justify-center rounded-hair text-muted no-underline hover-capable:hover:bg-surface hover-capable:hover:text-ink" aria-label={t('exit')}>
          <ArrowLeft className="size-5 rtl:-scale-x-100" />
        </Link>
        <h1 className="text-lg font-semibold">{t('title')}</h1>
        {branches.length > 1 ? (
          <select value={branch.id} onChange={(e) => router.push(`/admin/kds?branch=${e.target.value}`)} aria-label={t('house')} className="h-9 rounded-hair border border-field bg-raised px-2 text-[0.875rem]">
            {branches.map((b) => (
              <option key={b.id} value={b.id}>
                {b.name}
              </option>
            ))}
          </select>
        ) : (
          <span className="text-[0.875rem] text-muted">{branch.name}</span>
        )}
        <p className="text-[0.875rem] text-muted" aria-live="polite">
          {t('counts', { cooking: formatNumber(cooking.length, locale), ready: formatNumber(ready.length, locale) })}
          {incoming ? (
            <>
              {' · '}
              <Link href="/admin/orders" className="font-medium text-warning">
                {t('incoming', { n: formatNumber(incoming, locale) })}
              </Link>
            </>
          ) : null}
        </p>
        <div className="ms-auto flex items-center gap-2">
          <p className="font-mono text-2xl font-semibold tabular" aria-label={t('clock')}>
            {now ? formatClock(new Date(now), locale, branch.timeZone) : ''}
          </p>
          <Button
            variant="ghost"
            size="icon"
            aria-pressed={sound}
            aria-label={sound ? t('soundOn') : t('soundOff')}
            onClick={async () => {
              const next = !sound;
              if (next && (await unlockAudio())) chime();
              setSound(next);
              void run(() => setAdminSound(next));
            }}
          >
            {sound ? <BellRing /> : <BellOff />}
          </Button>
          <Button variant="ghost" size="icon" aria-label={full ? t('exitFullscreen') : t('fullscreen')} onClick={() => (document.fullscreenElement ? document.exitFullscreen() : document.documentElement.requestFullscreen())}>
            {full ? <Minimize /> : <Maximize />}
          </Button>
        </div>
      </header>

      <main className="flex flex-col gap-6 p-4">
        {cooking.length ? (
          <section aria-label={t('cooking')} className="grid grid-cols-[repeat(auto-fill,minmax(17rem,1fr))] items-start gap-4">
            {cooking.map((c, i) => (
              <Ticket key={c.id} card={c} index={i} now={now} canBump={canBump} onBump={bump} busy={pending} timeZone={branch.timeZone} />
            ))}
          </section>
        ) : (
          <div className="grid min-h-[40vh] place-items-center text-center">
            <div>
              <p className="text-xl font-semibold">{t('empty.title')}</p>
              <p className="mt-1 text-muted">{incoming ? t('empty.incoming', { n: formatNumber(incoming, locale) }) : t('empty.body')}</p>
            </div>
          </div>
        )}
        {ready.length ? (
          <section aria-labelledby="kds-ready">
            <h2 id="kds-ready" className="mb-3 text-[0.875rem] font-semibold text-success">
              {t('ready', { n: formatNumber(ready.length, locale) })}
            </h2>
            <div className="grid grid-cols-[repeat(auto-fill,minmax(17rem,1fr))] items-start gap-4">
              {ready.map((c, i) => (
                <Ticket key={c.id} card={c} index={i + 9} now={now} canBump={canBump} onBump={bump} busy={pending} timeZone={branch.timeZone} />
              ))}
            </div>
          </section>
        ) : null}
        {canBump && cooking.length ? <p className="text-center text-xs text-muted">{t('keys')}</p> : null}
      </main>
    </div>
  );
}
