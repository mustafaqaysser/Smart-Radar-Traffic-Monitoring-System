import { getTranslations } from 'next-intl/server';
import { Reveal } from '@/components/motion/reveal';
import { SplitWords } from '@/components/motion/split-words';
import { ArrowLink, ButtonAnchor } from '@/components/site/ui/button';
import { MediaImage } from '@/components/site/ui/media-image';
import { Stars } from '@/components/site/ui/stars';
import { Rail } from '@/components/site/ui/rail';
import { SectionHeading } from '@/components/site/ui/section-heading';
import { DishCard } from '@/components/site/menu/dish-card';
import { Link } from '@/i18n/navigation';
import { formatClock, formatDate, formatMoney, formatNumber, formatWallTime } from '@/lib/i18n/format';
import { tr } from '@/lib/i18n/localized';
import { plural } from '@/lib/i18n/plural';
import type { EventDTO, PressDTO, ReviewDTO } from '@/lib/queries/content';
import type { BranchDTO, MediaDTO, MenuItemDTO, SeasonalModeDTO } from '@/lib/queries/types';
import { hoursFor } from '@/lib/queries/branches';
import { describeOpenStatus } from '@/lib/site/open-status';
import { googleDirectionsUrl, telUrl, whatsappUrl } from '@/lib/services/maps';
import { scheduleForDate } from '@/lib/domain/hours';
import { formatTime, toDateString } from '@/lib/time/zoned';

// ——— Concept ———

export async function ConceptSection({ locale, title, body, photo }: { locale: string; title: string; body: string; photo: MediaDTO | null }) {
  const t = await getTranslations('home.concept');
  return (
    <section className="site-grid gap-y-12 pt-[var(--spacing-section)]" aria-labelledby="concept-title">
      <div className="col-span-full flex flex-col gap-8 md:col-span-5 lg:col-span-6 lg:col-start-1 lg:self-center">
        <Reveal as="p" variant="fade" className="t-label text-muted">
          {t('eyebrow')}
        </Reveal>
        <h2 id="concept-title" className="t-display-md">
          <SplitWords text={title} />
        </h2>
        <Reveal as="p" delay={150} className="t-body-lg measure">
          {body}
        </Reveal>
        <Reveal delay={250}>
          <ArrowLink href="/story">{t('link')}</ArrowLink>
        </Reveal>
      </div>
      {photo ? (
        <Reveal variant="shade" className="col-span-3 col-start-2 md:col-span-3 md:col-start-6 lg:col-span-4 lg:col-start-9">
          <MediaImage media={photo} locale={locale} sizes="(min-width: 1024px) 30vw, (min-width: 768px) 36vw, 70vw" ratio="3/4" shape="arch-3x4" className="cast-shade" />
        </Reveal>
      ) : null}
    </section>
  );
}

// ——— Signature dishes (the menu of the hour) ———

export async function SignatureSection({ locale, items, branch, serving }: { locale: string; items: MenuItemDTO[]; branch: BranchDTO | null; serving: boolean }) {
  const t = await getTranslations('home.signature');
  const tc = await getTranslations('common.status');
  if (!items.length) return null;
  const house = branch ? tr(branch.shortName, locale) : '';
  return (
    <section className="pt-[var(--spacing-section)]" aria-labelledby="signature-title">
      <div className="site-grid mb-12">
        <SectionHeading
          id="signature-title"
          className="col-span-full lg:col-span-8"
          eyebrow={t('eyebrow')}
          title={serving ? t('title', { house }) : t('titleClosed', { house })}
          intro={<p>{serving ? t('body') : t('nothing')}</p>}
        />
        <div className="col-span-full mt-8 lg:col-span-4 lg:mt-0 lg:self-end lg:justify-self-end">
          <ArrowLink href="/menu">{t('cta')}</ArrowLink>
        </div>
      </div>
      <Rail label={t('eyebrow')}>
        {items.map((item) => (
          <li key={item.id} className="w-[72vw] shrink-0 snap-start md:w-[38vw] lg:w-[23vw]">
            <DishCard item={item} locale={locale} branchId={branch?.id ?? null} labels={{ soldOut: tc('soldOut'), signature: tc('signature') }} />
          </li>
        ))}
      </Rail>
    </section>
  );
}

// ——— The houses (the room) ———

export async function RoomSection({ locale, branches, photos }: { locale: string; branches: BranchDTO[]; photos: MediaDTO[] }) {
  const t = await getTranslations('home.room');
  const [a, b, c] = photos;
  return (
    <section className="site-grid gap-y-12 pt-[var(--spacing-section)]" aria-labelledby="room-title">
      <SectionHeading id="room-title" className="col-span-full lg:col-span-7" eyebrow={t('eyebrow')} title={t('title')} intro={<p>{t('body')}</p>} action={<ArrowLink href="/locations">{t('cta')}</ArrowLink>} />
      {a ? (
        <Reveal variant="wipe" className="col-span-full lg:col-span-7 lg:row-span-2">
          <MediaImage media={a} locale={locale} sizes="(min-width: 1024px) 56vw, 100vw" ratio="4/3" />
          {branches[0] ? <p className="t-instrument mt-3 text-muted">{tr(branches[0].name, locale)}</p> : null}
        </Reveal>
      ) : null}
      {b ? (
        <Reveal variant="shade" delay={120} className="col-span-2 md:col-span-4 lg:col-span-4 lg:col-start-9">
          <MediaImage media={b} locale={locale} sizes="(min-width: 1024px) 30vw, 50vw" ratio="3/4" shape="arch-3x4" />
        </Reveal>
      ) : null}
      {c ? (
        <Reveal variant="shade" delay={200} className="col-span-2 md:col-span-4 lg:col-span-3 lg:col-start-10">
          <MediaImage media={c} locale={locale} sizes="(min-width: 1024px) 22vw, 50vw" ratio="4/5" />
          {branches[1] ? <p className="t-instrument mt-3 text-muted">{tr(branches[1].name, locale)}</p> : null}
        </Reveal>
      ) : null}
    </section>
  );
}

// ——— The chef ———

export async function ChefSection({ locale, quote, name, role, photo }: { locale: string; quote: string; name: string | null; role: string | null; photo: MediaDTO | null }) {
  const t = await getTranslations('home.chef');
  return (
    <section className="site-grid gap-y-10 pt-[var(--spacing-section)]" aria-labelledby="chef-title">
      {photo ? (
        <Reveal variant="shade" className="col-span-3 md:col-span-3 lg:col-span-4">
          <MediaImage media={photo} locale={locale} sizes="(min-width: 1024px) 30vw, 70vw" ratio="4/5" shape="arch-4x5" />
        </Reveal>
      ) : null}
      <figure className="col-span-full flex flex-col justify-center gap-8 md:col-span-5 lg:col-span-7 lg:col-start-6">
        <p id="chef-title" className="t-label text-muted">
          {t('eyebrow')}
        </p>
        <blockquote className="t-heading-lg">
          <SplitWords text={`«${quote}»`} />
        </blockquote>
        {name ? (
          <figcaption className="t-small">
            <span className="font-semibold">{name}</span>
            {role ? <span className="text-muted"> — {role}</span> : null}
          </figcaption>
        ) : null}
        <ArrowLink href="/team">{t('cta')}</ArrowLink>
      </figure>
    </section>
  );
}

// ——— Experiences ———

export async function ExperiencesSection({ locale, events, branches }: { locale: string; events: EventDTO[]; branches: BranchDTO[] }) {
  const t = await getTranslations('home.experiences');
  if (!events.length) return null;
  return (
    <section className="site-grid gap-y-12 pt-[var(--spacing-section)]" aria-labelledby="experiences-title">
      <SectionHeading id="experiences-title" className="col-span-full lg:col-span-8" eyebrow={t('eyebrow')} title={t('title')} />
      <div className="col-span-full lg:col-span-4 lg:self-end lg:justify-self-end">
        <ArrowLink href="/experiences">{t('cta')}</ArrowLink>
      </div>
      <ul className="col-span-full grid gap-[var(--spacing-gutter)] md:grid-cols-3">
        {events.slice(0, 3).map((e, i) => {
          const branch = branches.find((b) => b.id === e.branchId);
          const tz = branch?.timeZone ?? 'Asia/Riyadh';
          const start = new Date(e.startsAt);
          const left = Math.max(0, e.capacity - e.seatsTaken);
          const from = Math.min(...e.tickets.map((tk) => tk.price));
          return (
            <Reveal as="li" key={e.id} delay={i * 90} className="relative flex flex-col gap-4 border-t border-ink pt-5" >
              <div data-card className="group relative flex flex-col gap-4">
                {e.image ? <MediaImage media={e.image} locale={locale} sizes="(min-width: 768px) 30vw, 100vw" ratio="4/3" /> : null}
                <p className="t-instrument text-muted">
                  <time dateTime={e.startsAt}>
                    {formatDate(start, locale, tz, { weekday: 'long', day: 'numeric', month: 'long' })} · {formatClock(start, locale, tz)}
                  </time>
                  {branch ? ` · ${tr(branch.shortName, locale)}` : ''}
                </p>
                <h3 className="t-heading-md">
                  <Link href={`/experiences/${e.slug}`} className="card-link">
                    {tr(e.title, locale)}
                  </Link>
                </h3>
                <p className="t-small text-muted">{tr(e.summary, locale)}</p>
                <p className="t-small flex flex-wrap gap-x-4">
                  <span>{t('seatsLeft', plural(left, locale))}</span>
                  {Number.isFinite(from) ? <span className="text-muted">{t('from', { price: formatMoney(from, locale) })}</span> : null}
                </p>
              </div>
            </Reveal>
          );
        })}
      </ul>
    </section>
  );
}

// ——— Voices ———

export async function VoicesSection({ locale, reviews, press, stats }: { locale: string; reviews: ReviewDTO[]; press: PressDTO[]; stats: { count: number; average: number } }) {
  const t = await getTranslations('home.voices');
  const ordered = [...reviews.filter((r) => r.locale === locale), ...reviews.filter((r) => r.locale !== locale)].slice(0, 3);
  if (!ordered.length && !press.length) return null;
  return (
    <section className="site-grid gap-y-12 pt-[var(--spacing-section)]" aria-labelledby="voices-title">
      <SectionHeading
        id="voices-title"
        className="col-span-full lg:col-span-8"
        eyebrow={t('eyebrow')}
        title={t('title')}
        intro={stats.count ? <p className="t-instrument">{t('rating', { average: formatNumber(stats.average, locale, { maximumFractionDigits: 1 }), ...plural(stats.count, locale) })}</p> : undefined}
      />
      <ul className="col-span-full grid gap-[var(--spacing-gutter)] md:grid-cols-3">
        {ordered.map((r, i) => (
          <Reveal as="li" key={r.id} delay={i * 90} className="flex flex-col gap-5 border-t border-line pt-6">
            <figure lang={r.locale} dir={r.locale === 'ar' ? 'rtl' : 'ltr'} className="flex h-full flex-col gap-5">
              <Stars rating={r.rating} label={t('stars', { rating: r.rating })} />
              <blockquote className="t-body flex-1">{r.title ? <p className="mb-2 font-semibold">{r.title}</p> : null}<p>{r.body}</p></blockquote>
              <figcaption className="t-small text-muted">{r.name}</figcaption>
            </figure>
          </Reveal>
        ))}
      </ul>
      {press.length ? (
        <ul className="col-span-full grid gap-[var(--spacing-gutter)] border-t border-line pt-10 md:grid-cols-2">
          {press.slice(0, 2).map((p) => (
            <li key={p.id}>
              <figure className="flex flex-col gap-3">
                <blockquote className="t-heading-md">«{tr(p.quote, locale)}»</blockquote>
                <figcaption className="t-label text-muted">
                  {tr(p.publication, locale)} · {formatNumber(p.year, locale, { useGrouping: false })}
                </figcaption>
              </figure>
            </li>
          ))}
        </ul>
      ) : null}
      <div className="col-span-full">
        <ArrowLink href="/reviews">{t('cta')}</ArrowLink>
      </div>
    </section>
  );
}

// ——— Find us ———

export async function FindUsSection({ locale, branches, seasons }: { locale: string; branches: BranchDTO[]; seasons: SeasonalModeDTO[] }) {
  const t = await getTranslations('home.find');
  const tb = await getTranslations('common.branch');
  const tc = await getTranslations('common');
  const now = new Date();
  return (
    <section className="site-grid gap-y-12 pt-[var(--spacing-section)]" aria-labelledby="find-title">
      <SectionHeading id="find-title" className="col-span-full" eyebrow={t('eyebrow')} title={t('title')} />
      <ul className="col-span-full grid gap-[var(--spacing-gutter)] md:grid-cols-2">
        {branches.map((b) => {
          const hours = hoursFor(b, seasons);
          const status = describeOpenStatus(hours, now, locale, (k, v) => tb(k, v));
          const today = scheduleForDate(hours, toDateString(now, b.timeZone));
          const ranges = today.ranges.map((r) => `${formatWallTime(formatTime(r.start), locale)} – ${formatWallTime(formatTime(r.end % 1440), locale)}`);
          return (
            <li key={b.id} className="flex flex-col gap-5 bg-surface p-6 lg:p-10">
              <h3 className="t-heading-lg">{tr(b.name, locale)}</h3>
              <p className="t-body text-muted">{tr(b.address, locale)}</p>
              <p className="t-small flex items-center gap-2">
                <span aria-hidden="true" className={status.open ? 'size-2.5 rounded-full bg-success' : 'size-2.5 rounded-full bg-muted'} />
                <span className="font-semibold">{status.open ? tb('openNow') : tb('closedNow')}</span>
                {status.detail ? <span className="text-muted">· {status.detail}</span> : null}
              </p>
              <p className="t-small">
                <span className="text-muted">{t('today')}: </span>
                {today.closed ? tc('time.closed') : ranges.map((r) => <bdi key={r}>{r} </bdi>)}
              </p>
              <div className="mt-auto flex flex-wrap gap-3 pt-2">
                <ButtonAnchor href={googleDirectionsUrl({ lat: b.lat, lng: b.lng })} target="_blank" size="sm" variant="secondary" icon="external" newWindowLabel={tc('a11y.newWindow')}>
                  {tc('actions.directions')}
                </ButtonAnchor>
                <ButtonAnchor href={telUrl(b.phone)} size="sm" variant="quiet" icon={null} leadingIcon="phone">
                  <bdi dir="ltr">{b.phone}</bdi>
                </ButtonAnchor>
                {b.whatsapp ? (
                  <ButtonAnchor href={whatsappUrl(b.whatsapp)} target="_blank" size="sm" variant="quiet" icon={null} leadingIcon="chat" newWindowLabel={tc('a11y.newWindow')}>
                    {tc('actions.whatsapp')}
                  </ButtonAnchor>
                ) : null}
              </div>
              <Link href={`/locations/${b.slug}`} className="t-small underline decoration-line underline-offset-4">
                {t('details')}
              </Link>
            </li>
          );
        })}
      </ul>
    </section>
  );
}
