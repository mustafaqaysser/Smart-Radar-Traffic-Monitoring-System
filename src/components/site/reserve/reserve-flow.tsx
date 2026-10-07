'use client';

import Image from 'next/image';
import { useRouter } from 'next/navigation';
import { useLocale, useTranslations } from 'next-intl';
import { useEffect, useId, useRef, useState, useTransition, type ReactNode } from 'react';
import { Icon } from '@/components/brand/icon';
import { useMotion } from '@/components/motion/motion-provider';
import { PaymentForm } from '@/components/site/payment-form';
import { Button } from '@/components/site/ui/button';
import { Chip } from '@/components/site/ui/field';
import { Link } from '@/i18n/navigation';
import { confirmReservation, holdTable, releaseTable, type BookingOutcome, type HoldView } from '@/lib/actions/reservations';
import { formatClock, formatDateString, formatMoney, formatNumber } from '@/lib/i18n/format';
import { plural } from '@/lib/i18n/plural';
import { OCCASIONS, type AreaChoice, type FlowBranch, type FlowSetup, type GuestPrefill, type PublicSlot, type SlotsResponse } from '@/lib/reserve/types';
import { cn } from '@/lib/utils/cn';
import { BookingTicket } from './booking-ticket';
import { DetailsForm } from './details-form';
import { MonthCalendar } from './month-calendar';
import { SlotPicker } from './slot-picker';
import { formatCountdown, useCountdown } from './use-countdown';
import { WaitlistForm } from './waitlist-form';

type Step = 'branch' | 'date' | 'party' | 'time' | 'seating' | 'details' | 'payment';

export interface ReserveFlowProps {
  setup: FlowSetup;
  guest: GuestPrefill | null;
  signedIn: boolean;
  newsletter: boolean;
  initial: { branch?: string | null; date?: string | null; party?: number | null };
  /** 'page' on /reserve (with the ticket beside it); 'sheet' inside the mobile booking sheet. */
  layout: 'page' | 'sheet';
  /** Called just before the flow navigates away (the sheet closes itself). */
  onLeave?: () => void;
}

const HOLD_ERRORS = new Set(['holdExpired', 'unavailable']);

/**
 * The booking flow: house → day → guests → time → seating & occasion → details → (deposit). Each answered
 * step folds into a line of the ledger and can be reopened; choosing a time holds a table for a few minutes,
 * and the ticket beside the flow takes on the light of the chosen hour.
 */
export function ReserveFlow({ setup, guest, signedIn, newsletter, initial, layout, onLeave }: ReserveFlowProps) {
  const t = useTranslations('reserve');
  const tc = useTranslations('common');
  const tu = useTranslations('common.units');
  const locale = useLocale();
  const router = useRouter();
  const { scrollTo } = useMotion();
  const uid = useId();
  const single = setup.branches.length === 1 ? setup.branches[0] : undefined;

  const initialBranch = setup.branches.find((b) => b.slug === initial.branch) ?? single ?? null;
  const initialDate = initialBranch && initial.date && initialBranch.days[initial.date] === 'open' ? initial.date : null;
  const initialParty = initial.party && initial.party >= 1 && initial.party <= setup.maxPartyOnline ? initial.party : null;

  const [branchSlug, setBranchSlug] = useState<string | null>(initialBranch?.slug ?? null);
  const [date, setDate] = useState<string | null>(initialDate);
  const [party, setParty] = useState<number | null>(initialParty);
  const [slot, setSlot] = useState<PublicSlot | null>(null);
  const [area, setArea] = useState<AreaChoice>('any');
  const [occasion, setOccasion] = useState<string>('none');
  const [hold, setHold] = useState<HoldView | null>(null);
  const [holding, setHolding] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [payment, setPayment] = useState<Extract<BookingOutcome, { kind: 'payment' }> | null>(null);
  const [refresh, setRefresh] = useState(0);
  const [slotsByKey, setSlotsByKey] = useState<Record<string, SlotsResponse | 'error'>>({});
  const [waitlistOpen, setWaitlistOpen] = useState(false);
  const [holdPending, startHold] = useTransition();
  const [open, setOpen] = useState<Step>(() => (!initialBranch ? 'branch' : !initialDate ? 'date' : !initialParty ? 'party' : 'time'));
  const headings = useRef<Partial<Record<Step, HTMLHeadingElement | null>>>({});
  const moved = useRef(false);

  const branch: FlowBranch | null = setup.branches.find((b) => b.slug === branchSlug) ?? null;
  const today = branch?.first ?? setup.branches[0]?.first ?? '';
  const deposit = party && setup.deposit.perGuest > 0 && party >= setup.deposit.minParty ? party * setup.deposit.perGuest : 0;

  // ——— Slots: fetched as soon as house, day and party are known (keyed, so effects never set state synchronously).
  const slotsKey = branch && date && party ? `${branch.slug}|${date}|${party}|${refresh}` : null;
  useEffect(() => {
    if (!slotsKey || !branch || !date || !party || slotsByKey[slotsKey]) return undefined;
    const ctrl = new AbortController();
    const query = new URLSearchParams({ branch: branch.slug, date, party: String(party) });
    fetch(`/api/reservations/slots?${query.toString()}`, { signal: ctrl.signal, cache: 'no-store' })
      .then((res) => (res.ok ? (res.json() as Promise<SlotsResponse>) : Promise.reject(new Error(String(res.status)))))
      .then((data) => setSlotsByKey((m) => ({ ...m, [slotsKey]: data })))
      .catch(() => {
        if (!ctrl.signal.aborted) setSlotsByKey((m) => ({ ...m, [slotsKey]: 'error' }));
      });
    return () => ctrl.abort();
  }, [slotsKey, branch, date, party, slotsByKey]);
  const slotsState = slotsKey ? slotsByKey[slotsKey] : undefined;
  const slots = slotsState && slotsState !== 'error' ? slotsState : null;

  // ——— The hold's countdown; when it runs out the guest chooses a time again.
  const secondsLeft = useCountdown(hold?.expiresAt ?? null, () => {
    setHold(null);
    setSlot(null);
    setNotice(t('hold.expired'));
    setRefresh((n) => n + 1);
    go('time');
  });

  // ——— Keep the address shareable on the full page.
  useEffect(() => {
    if (layout !== 'page') return;
    const url = new URL(window.location.href);
    const set = (k: string, v: string | null) => (v ? url.searchParams.set(k, v) : url.searchParams.delete(k));
    set('branch', branchSlug);
    set('date', date);
    set('party', party ? String(party) : null);
    if (url.href !== window.location.href) window.history.replaceState(null, '', url);
  }, [layout, branchSlug, date, party]);

  // ——— Move focus to the step that just opened (keyboard and screen-reader users follow the flow).
  useEffect(() => {
    if (!moved.current) return;
    moved.current = false;
    const heading = headings.current[open];
    if (!heading) return;
    heading.focus({ preventScroll: true });
    if (layout === 'page') scrollTo(heading, { offset: -120 });
    else heading.scrollIntoView({ block: 'start', behavior: window.matchMedia('(prefers-reduced-motion: reduce)').matches ? 'auto' : 'smooth' });
  }, [open, layout, scrollTo]);

  function go(step: Step) {
    moved.current = true;
    setOpen(step);
  }

  function dropHold() {
    if (hold) void releaseTable();
    setHold(null);
    setSlot(null);
    setArea('any');
    setPayment(null);
  }

  const errorText = (key: string) => (key === 'taken' || HOLD_ERRORS.has(key) ? t(`errors.${key}`) : key === 'window' ? t('errors.window', { n: formatNumber(setup.bookingWindowDays, locale) }) : t('time.error'));

  function chooseBranch(slug: string) {
    if (slug !== branchSlug) {
      dropHold();
      const next = setup.branches.find((b) => b.slug === slug);
      if (date && next?.days[date] !== 'open') setDate(null);
      setWaitlistOpen(false);
      setBranchSlug(slug);
      go(date && next?.days[date] === 'open' ? (party ? 'time' : 'party') : 'date');
      return;
    }
    go(date ? (party ? 'time' : 'party') : 'date');
  }

  function chooseDate(d: string) {
    if (d !== date) {
      dropHold();
      setWaitlistOpen(false);
      setNotice(null);
      setDate(d);
    }
    go(party ? 'time' : 'party');
  }

  function chooseParty(n: number) {
    if (n !== party) {
      dropHold();
      setWaitlistOpen(false);
      setNotice(null);
      setParty(n);
    }
    go('time');
  }

  function pickTime(s: PublicSlot) {
    if (!branch || !date || !party) return;
    if (hold && slot?.time === s.time) {
      go('seating');
      return;
    }
    setNotice(null);
    setHolding(s.time);
    startHold(async () => {
      const res = await holdTable({ branch: branch.slug, date, time: s.time, party, area: 'any' });
      setHolding(null);
      if (res.ok) {
        setSlot(s);
        setArea('any');
        setHold(res.data);
        setPayment(null);
        go('seating');
      } else {
        setNotice(errorText(res.error));
        if (res.error === 'taken') setRefresh((n) => n + 1);
      }
    });
  }

  function pickArea(next: AreaChoice) {
    if (!branch || !date || !party || !slot || next === area) return;
    setNotice(null);
    startHold(async () => {
      const res = await holdTable({ branch: branch.slug, date, time: slot.time, party, area: next });
      if (res.ok) {
        setArea(next);
        setHold(res.data);
      } else {
        // The table already held stays the guest's; only this corner is taken.
        setNotice(res.error === 'taken' ? t('areas.unavailable') : errorText(res.error));
      }
    });
  }

  function lostTable(key: string): boolean {
    if (!HOLD_ERRORS.has(key)) return false;
    setHold(null);
    setSlot(null);
    setNotice(t(`errors.${key}`));
    setRefresh((n) => n + 1);
    go('time');
    return true;
  }

  function leave(href: string) {
    onLeave?.();
    router.push(href);
  }

  function onOutcome(outcome: BookingOutcome) {
    setHold(null);
    if (outcome.kind === 'confirmed') {
      leave(outcome.href);
      return;
    }
    setPayment(outcome);
    go('payment');
  }

  // ——— The ledger.
  const steps: Step[] = [...(setup.branches.length > 1 ? (['branch'] as const) : []), 'date', 'party', 'time', 'seating', 'details', ...(payment ? (['payment'] as const) : [])];
  const done: Record<Step, boolean> = {
    branch: Boolean(branch),
    date: Boolean(date),
    party: Boolean(party),
    time: Boolean(hold && slot),
    seating: Boolean(hold && slot) && (open === 'details' || open === 'payment'),
    details: Boolean(payment),
    payment: false,
  };
  const reachable = (step: Step) => {
    const index = steps.indexOf(step);
    return steps.slice(0, index).every((s) => done[s]);
  };
  const value: Record<Step, string | null> = {
    branch: branch?.name ?? null,
    date: date ? formatDateString(date, locale, { weekday: 'long', day: 'numeric', month: 'long' }) : null,
    party: party ? tu('guests', plural(party, locale)) : null,
    time: slot && branch ? formatClock(new Date(slot.startsAt), locale, branch.timeZone) : null,
    seating: slot ? [t(`areas.${area}`), occasion !== 'none' ? t(`occasions.${occasion}`) : null].filter(Boolean).join(' · ') : null,
    details: payment ? payment.code : null,
    payment: null,
  };

  const content: Record<Step, () => ReactNode> = {
    branch: () => (
      <div className="grid gap-6 sm:grid-cols-2">
        {setup.branches.map((b) => (
          <button key={b.slug} type="button" aria-pressed={b.slug === branchSlug} onClick={() => chooseBranch(b.slug)} className="group flex flex-col gap-4 text-start" aria-label={t('branch.choose', { house: b.name })}>
            <span className={cn('arch-4x5 relative block aspect-[4/5] w-full overflow-hidden bg-surface transition-[box-shadow] duration-[var(--dur-base)]', b.slug === branchSlug ? 'cast-shade' : 'hover-capable:group-hover:cast-shade')}>
              {b.image ? <Image src={b.image.src} alt="" fill sizes="(min-width: 1024px) 26vw, (min-width: 640px) 45vw, 90vw" quality={72} placeholder={b.image.blur ? 'blur' : 'empty'} blurDataURL={b.image.blur ?? undefined} className="object-cover" style={{ objectPosition: `${b.image.focalX * 100}% ${b.image.focalY * 100}%` }} /> : null}
            </span>
            <span className="flex items-start justify-between gap-3">
              <span className="flex flex-col gap-1">
                <span className="t-label text-muted">{b.city}</span>
                <span className="t-heading-md">{b.name}</span>
                <span className="t-small text-muted">{b.address}</span>
              </span>
              {b.slug === branchSlug ? <Icon name="check" size={22} className="mt-1 shrink-0" /> : null}
            </span>
          </button>
        ))}
      </div>
    ),
    date: () =>
      branch ? (
        <div className="max-w-md">
          <MonthCalendar first={branch.first} last={branch.last} today={today} value={date} states={branch.days} notes={branch.notes} onSelect={chooseDate} />
        </div>
      ) : null,
    party: () => (
      <div className="flex flex-col gap-5">
        <div role="group" aria-label={t('fields.party')} className="grid grid-cols-4 gap-2 sm:grid-cols-8">
          {Array.from({ length: setup.maxPartyOnline }, (_, i) => i + 1).map((n) => (
            <Chip key={n} pressed={party === n} onClick={() => chooseParty(n)} aria-label={tu('guests', plural(n, locale))} className="tabular">
              {formatNumber(n, locale)}
            </Chip>
          ))}
        </div>
        <p className="t-small text-muted">{t('party.kids')}</p>
        <div className="flex flex-col gap-2 border-t border-line pt-5">
          <p className="t-heading-sm">{t('party.more', { n: formatNumber(setup.maxPartyOnline, locale) })}</p>
          <p className="t-small measure">{t('party.moreBody')}</p>
          <Link href={setup.privateDining ? '/private-dining' : { pathname: '/contact', query: { subject: 'reservations' } }} className="t-small inline-flex items-center gap-2 self-start underline underline-offset-4">
            {setup.privateDining ? t('party.moreCta') : tc('nav.contact')}
            <Icon name="arrow" size={16} />
          </Link>
        </div>
      </div>
    ),
    time: () =>
      branch && date && party ? (
        <div className="flex flex-col gap-6">
          {notice ? (
            <p role="alert" className="t-small flex items-start gap-2 border-s-2 border-danger ps-4 text-ink">
              {notice}
            </p>
          ) : null}
          <SlotPicker data={slots} error={slotsState === 'error'} periods={branch.periods} timeZone={branch.timeZone} value={hold ? (slot?.time ?? null) : null} pending={holding} onPick={pickTime} onRetry={() => setRefresh((n) => n + 1)} />
          {slots && !slots.slots.some((s) => s.available) ? (
            <div className="flex flex-col gap-2">
              <p className="t-heading-sm">{t('time.none')}</p>
              <p className="t-small text-muted measure">{t('time.noneHint')}</p>
            </div>
          ) : null}
          {slots && slots.slots.some((s) => s.reason === 'full' || s.reason === 'pacing' || !s.available) ? (
            waitlistOpen ? (
              <section aria-labelledby={`${uid}-wl`} className="flex flex-col gap-5 border-t border-line pt-6">
                <h3 id={`${uid}-wl`} className="t-heading-md">
                  {t('waitlist.title')}
                </h3>
                <WaitlistForm branch={branch.slug} date={date} dateLabel={value.date ?? date} party={party} times={slots.slots.filter((s) => s.reason !== 'past').map((s) => s.time)} guest={guest} />
              </section>
            ) : (
              <Button variant="quiet" className="self-start" onClick={() => setWaitlistOpen(true)}>
                {t('waitlist.open')}
              </Button>
            )
          ) : null}
        </div>
      ) : null,
    seating: () =>
      branch && hold && slot ? (
        <div className="flex flex-col gap-8">
          {notice ? (
            <p role="alert" className="t-small border-s-2 border-danger ps-4">
              {notice}
            </p>
          ) : null}
          <div role="group" aria-label={t('summary.seating')} aria-busy={holdPending || undefined} className="grid gap-3 sm:grid-cols-2">
            {(['any', ...branch.areas] as AreaChoice[]).map((a) => {
              const possible = a === 'any' || hold.areas.includes(a);
              return (
                <button
                  key={a}
                  type="button"
                  aria-pressed={area === a}
                  aria-disabled={!possible || undefined}
                  disabled={holdPending}
                  onClick={() => {
                    if (possible) pickArea(a);
                  }}
                  className={cn(
                    'flex min-h-20 flex-col items-start justify-center gap-1 border px-5 py-4 text-start transition-[border-color,background-color,color] duration-[var(--dur-quick)]',
                    area === a ? 'border-ink bg-ink text-bg' : 'border-line',
                    possible && area !== a && 'hover-capable:hover:border-ink',
                    !possible && 'cursor-not-allowed text-muted',
                  )}
                >
                  <span className="t-heading-sm">{t(`areas.${a}`)}</span>
                  <span className={cn('t-small', area === a ? 'text-bg/80' : 'text-muted')}>{possible ? t(`areas.${a}Hint`) : t('areas.unavailable')}</span>
                </button>
              );
            })}
          </div>
          <fieldset className="flex flex-col gap-3">
            <legend className="t-label mb-3 text-muted">{t('occasions.title')}</legend>
            <div className="flex flex-wrap gap-2">
              {(['none', ...OCCASIONS] as const).map((o) => (
                <Chip key={o} pressed={occasion === o} onClick={() => setOccasion(o)}>
                  {t(`occasions.${o}`)}
                </Chip>
              ))}
            </div>
          </fieldset>
          <Button size="lg" icon="arrow" disabled={holdPending} onClick={() => go('details')} className="w-full sm:w-auto sm:self-start">
            {t('steps.continue')}
          </Button>
        </div>
      ) : null,
    details: () =>
      hold && slot ? (
        <DetailsForm
          guest={guest}
          signedIn={signedIn}
          deposit={deposit}
          rules={{ minParty: setup.deposit.minParty, perGuest: setup.deposit.perGuest, cutoffHours: setup.cutoffHours }}
          newsletter={newsletter}
          hidden={{ holdId: hold.holdId, occasion }}
          action={confirmReservation}
          onOutcome={onOutcome}
          onFlowError={lostTable}
        />
      ) : null,
    payment: () =>
      payment ? (
        <div className="flex flex-col gap-6">
          <div className="flex flex-col gap-2">
            <p className="t-heading-sm">{t('deposit.title', { amount: formatMoney(payment.payment.amount, locale) })}</p>
            <p className="t-small text-muted">{t('deposit.later', { minutes: formatNumber(setup.depositWindowMinutes, locale) })}</p>
          </div>
          <PaymentForm
            {...payment.payment}
            onSucceeded={() => {
              const target = new URL(payment.payment.returnUrl);
              leave(`${target.pathname}${target.search}`);
            }}
          />
        </div>
      ) : null,
  };

  const ledger = (
    <ol className="flex flex-col border-b border-line">
      {steps.map((step, i) => {
        const isOpen = open === step;
        const canOpen = !isOpen && done[step] && step !== 'payment' && !(step === 'details' && payment);
        const headingId = `${uid}-${step}`;
        return (
          <li key={step} className="border-t border-line">
            <section aria-labelledby={headingId}>
              <div className="flex min-h-16 items-center gap-4 py-4">
                <span aria-hidden="true" className={cn('t-instrument tabular w-7 shrink-0', isOpen ? 'text-accent' : 'text-muted')}>
                  {formatNumber(i + 1, locale, { minimumIntegerDigits: 2 })}
                </span>
                <h2
                  id={headingId}
                  ref={(el) => {
                    headings.current[step] = el;
                  }}
                  tabIndex={-1}
                  className={cn('min-w-0 focus:outline-none focus-visible:outline-2', isOpen ? 't-heading-lg' : 't-label', !isOpen && !reachable(step) && 'text-muted/70')}
                >
                  {isOpen ? t(step === 'payment' ? 'steps.payment' : `questions.${step}`) : t(`steps.${step === 'seating' ? 'seating' : step}`)}
                </h2>
                {!isOpen && done[step] && value[step] ? <span className="t-body ms-auto min-w-0 truncate text-end">{value[step]}</span> : null}
                {canOpen ? (
                  <button type="button" onClick={() => go(step)} className="t-label shrink-0 underline underline-offset-4 hover-capable:hover:text-accent">
                    {t('steps.change')}
                    <span className="sr-only"> — {t(`steps.${step}`)}</span>
                  </button>
                ) : null}
              </div>
              {isOpen ? <div className="pb-10 sm:ps-11">{content[step]()}</div> : null}
            </section>
          </li>
        );
      })}
    </ol>
  );

  const summary = [value.date, value.time, value.party].filter(Boolean).join(' · ');

  if (layout === 'sheet') {
    return (
      <div className="flex flex-col">
        {ledger}
        {summary ? (
          <div className="sticky -bottom-6 -mx-6 -mb-6 mt-6 flex items-center justify-between gap-4 border-t border-line bg-raised px-6 pt-3 pb-[calc(env(safe-area-inset-bottom)+0.75rem)]">
            <p className="t-small min-w-0 truncate">{summary}</p>
            {secondsLeft !== null ? (
              <p className="t-instrument tabular flex shrink-0 items-center gap-2" aria-label={t('hold.held', { time: formatCountdown(secondsLeft, locale) })}>
                <Icon name="clock" size={16} />
                {formatCountdown(secondsLeft, locale)}
              </p>
            ) : null}
          </div>
        ) : null}
      </div>
    );
  }

  return (
    <div className="site-grid gap-y-10 pb-28 lg:pb-0">
      <div className="col-span-full lg:col-span-7">{ledger}</div>
      <aside className="hidden lg:col-span-4 lg:col-start-9 lg:block">
        <div className="sticky top-28">
          <BookingTicket branch={branch} date={date} party={party} slot={hold || payment ? slot : null} area={hold || payment ? area : null} occasion={occasion} deposit={deposit} secondsLeft={secondsLeft} holdSeconds={setup.holdMinutes * 60} />
        </div>
      </aside>
      {summary ? (
        <div className="fixed inset-x-0 bottom-0 z-30 border-t border-line bg-bg px-4 pt-3 pb-[calc(env(safe-area-inset-bottom)+0.75rem)] lg:hidden">
          <div className="flex items-center justify-between gap-4">
            <p className="t-small min-w-0 truncate">{summary}</p>
            {secondsLeft !== null ? (
              <p className="t-instrument tabular flex shrink-0 items-center gap-2" aria-label={t('hold.held', { time: formatCountdown(secondsLeft, locale) })}>
                <Icon name="clock" size={16} />
                {formatCountdown(secondsLeft, locale)}
              </p>
            ) : null}
          </div>
        </div>
      ) : null}
    </div>
  );
}
