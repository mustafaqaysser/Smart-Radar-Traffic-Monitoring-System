'use client';

import { useLocale, useTranslations } from 'next-intl';
import { Fragment, useEffect, useId, useRef, useState, type ReactNode } from 'react';
import { Icon } from '@/components/brand/icon';
import { Button } from '@/components/site/ui/button';
import { Textarea } from '@/components/site/ui/field';
import { Link } from '@/i18n/navigation';
import { formatDateString, formatWallTime } from '@/lib/i18n/format';
import { plural } from '@/lib/i18n/plural';
import { cn } from '@/lib/utils/cn';

interface Booking {
  code: string;
  house: string;
  date: string;
  time: string;
  partySize: number;
  depositDue: boolean;
  href: string;
}

interface Turn {
  role: 'user' | 'assistant';
  content: string;
  bookings?: Booking[];
  notice?: string;
}

/** Turns /ar/… and /en/… paths and this site's absolute links into in-site links; everything else stays text. */
function Linkified({ text, origin }: { text: string; origin: string }) {
  const escaped = origin.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const pattern = new RegExp(`(${escaped}/(?:ar|en)/[^\\s،,)]+|/(?:ar|en)/[\\w\\-/?=&.%]+)`, 'g');
  const parts = text.split(pattern);
  return (
    <>
      {parts.map((part, i) => {
        if (i % 2 === 0) return <Fragment key={i}>{part}</Fragment>;
        const path = part.startsWith(origin) ? part.slice(origin.length) : part;
        const local = path.replace(/^\/(ar|en)(?=\/)/, '');
        return (
          <Link key={i} href={local} className="underline underline-offset-4">
            <bdi>{path.split('?')[0]}</bdi>
          </Link>
        );
      })}
    </>
  );
}

/**
 * The concierge conversation. Each guest message is sent with the visible history; the reply streams in as
 * Server-Sent Events (text, which tool is running, bookings made), and can be stopped at any time.
 */
export function ConciergeChat({ origin }: { origin: string }) {
  const t = useTranslations('pages.concierge');
  const tu = useTranslations('common.units');
  const locale = useLocale();
  const id = useId();
  const [turns, setTurns] = useState<Turn[]>([]);
  const [draft, setDraft] = useState('');
  const [busy, setBusy] = useState(false);
  const [status, setStatus] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);
  const logRef = useRef<HTMLOListElement>(null);
  const suggestions = t.raw('suggestions') as string[];

  useEffect(() => () => abortRef.current?.abort(), []);
  useEffect(() => {
    logRef.current?.lastElementChild?.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
  }, [turns]);

  const updateLast = (fn: (turn: Turn) => Turn) => setTurns((all) => [...all.slice(0, -1), fn(all[all.length - 1] as Turn)]);

  const ask = async (text: string) => {
    const question = text.trim();
    if (!question || busy) return;
    const history: Turn[] = [...turns.filter((x) => x.content.trim()), { role: 'user', content: question }];
    setTurns([...history, { role: 'assistant', content: '' }]);
    setDraft('');
    setBusy(true);
    setStatus(t('thinking'));
    const controller = new AbortController();
    abortRef.current = controller;
    try {
      const res = await fetch('/api/concierge', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ locale, messages: history.slice(-24).map(({ role, content }) => ({ role, content: content.slice(0, 2000) })) }),
        signal: controller.signal,
      });
      if (!res.ok || !res.body) {
        updateLast((x) => ({ ...x, notice: res.status === 429 ? t('notices.rateLimited') : t('notices.error') }));
        return;
      }
      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        let cut: number;
        while ((cut = buffer.indexOf('\n\n')) >= 0) {
          const raw = buffer.slice(0, cut);
          buffer = buffer.slice(cut + 2);
          const event = /^event: (.+)$/m.exec(raw)?.[1];
          const data = /^data: (.+)$/m.exec(raw)?.[1];
          if (!event || !data) continue;
          const payload = JSON.parse(data) as Record<string, unknown>;
          if (event === 'text') {
            setStatus(null);
            updateLast((x) => ({ ...x, content: x.content + String(payload.delta ?? '') }));
          } else if (event === 'tool') {
            const name = String(payload.name ?? '');
            setStatus(t.has(`tools.${name}`) ? t(`tools.${name}`) : t('thinking'));
          } else if (event === 'booking') {
            updateLast((x) => ({ ...x, bookings: [...(x.bookings ?? []), payload as unknown as Booking] }));
          } else if (event === 'notice') {
            const kind = String(payload.kind ?? 'error');
            updateLast((x) => ({ ...x, notice: t.has(`notices.${kind}`) ? t(`notices.${kind}`) : t('notices.error') }));
          }
        }
      }
    } catch (err) {
      if (!(err instanceof DOMException && err.name === 'AbortError')) updateLast((x) => ({ ...x, notice: t('notices.error') }));
    } finally {
      setBusy(false);
      setStatus(null);
      abortRef.current = null;
      setTurns((all) => all.map((x, i) => (i === all.length - 1 && x.role === 'assistant' ? { ...x, content: x.content.trim() } : x)));
    }
  };

  const bubble = (turn: Turn, i: number): ReactNode => (
    <li key={i} className={cn('flex flex-col gap-2', turn.role === 'user' ? 'items-end' : 'items-start')}>
      <span className="t-label text-muted">{turn.role === 'user' ? t('you') : t('host')}</span>
      {turn.content ? (
        <div className={cn('t-body max-w-[42rem] whitespace-pre-line [overflow-wrap:anywhere]', turn.role === 'user' ? 'bg-ink px-4 py-3 text-bg' : 'border-s-2 border-sun ps-4')} dir="auto">
          {turn.role === 'assistant' ? <Linkified text={turn.content} origin={origin} /> : turn.content}
        </div>
      ) : null}
      {turn.bookings?.map((b) => (
        <div key={b.code} className="flex w-full max-w-md flex-col gap-2 bg-raised p-5 cast-shade">
          <p className="t-heading-sm flex items-center gap-2">
            <Icon name="calendar" size={18} />
            {t('booking.title', { code: b.code })}
          </p>
          <p className="t-small text-muted">{t('booking.detail', { house: b.house, date: formatDateString(b.date, locale, { weekday: 'long', day: 'numeric', month: 'long' }), time: formatWallTime(b.time, locale), guests: tu('guests', plural(b.partySize, locale)) })}</p>
          <a href={b.href} className="t-small self-start underline underline-offset-4">
            {b.depositDue ? t('booking.deposit') : t('booking.view')}
          </a>
        </div>
      ))}
      {turn.notice ? <p className="t-small text-danger">{turn.notice}</p> : null}
    </li>
  );

  return (
    <div className="flex flex-col gap-8">
      {turns.length ? (
        <ol ref={logRef} aria-live="polite" aria-busy={busy} className="flex flex-col gap-6 border-t border-ink pt-6">
          {turns.map(bubble)}
        </ol>
      ) : (
        <ul className="flex flex-wrap gap-2" aria-label={t('placeholder')}>
          {suggestions.map((s) => (
            <li key={s}>
              <button type="button" onClick={() => void ask(s)} className="inline-flex min-h-11 items-center rounded-pill border border-line px-4 text-start text-[0.9375rem] hover-capable:hover:border-ink">
                {s}
              </button>
            </li>
          ))}
        </ul>
      )}
      {status ? (
        <p role="status" className="t-small flex items-center gap-2 text-muted">
          <span aria-hidden="true" className="size-2 animate-pulse rounded-full bg-sun reduced:animate-none" />
          {status}
        </p>
      ) : null}
      <form
        className="flex flex-col gap-3 border-t border-line pt-6"
        onSubmit={(e) => {
          e.preventDefault();
          void ask(draft);
        }}
      >
        <label htmlFor={`${id}-q`} className="sr-only">
          {t('label')}
        </label>
        <Textarea
          id={`${id}-q`}
          value={draft}
          rows={3}
          maxLength={2000}
          placeholder={t('placeholder')}
          dir="auto"
          onChange={(e) => setDraft(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter' && !e.shiftKey && !e.nativeEvent.isComposing) {
              e.preventDefault();
              void ask(draft);
            }
          }}
          className="min-h-24"
        />
        <div className="flex flex-wrap items-center justify-between gap-3">
          <p className="t-small measure text-muted">{t('disclaimer')}</p>
          <div className="flex gap-3">
            {turns.length && !busy ? (
              <Button variant="quiet" size="sm" onClick={() => setTurns([])}>
                {t('new')}
              </Button>
            ) : null}
            {busy ? (
              <Button variant="secondary" icon={null} onClick={() => abortRef.current?.abort()}>
                {t('stop')}
              </Button>
            ) : (
              <Button type="submit" icon="arrow" disabled={!draft.trim()}>
                {t('send')}
              </Button>
            )}
          </div>
        </div>
      </form>
    </div>
  );
}
