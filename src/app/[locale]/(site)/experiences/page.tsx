import type { Metadata } from 'next';
import { notFound } from 'next/navigation';
import restaurantConfig from '@config';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import { EventCalendar, monthsBetween } from '@/components/site/experiences/event-calendar';
import { EventLedger, type EventRow } from '@/components/site/experiences/event-ledger';
import { JsonLd } from '@/components/site/json-ld';
import { PageHeader } from '@/components/site/ui/page-header';
import { formatClock, formatDate, formatNumber } from '@/lib/i18n/format';
import { tr } from '@/lib/i18n/localized';
import { imageView } from '@/lib/menu/view';
import { getBranches } from '@/lib/queries/branches';
import { getUpcomingEvents } from '@/lib/queries/content';
import { featureEnabled } from '@/lib/server/settings';
import { pageMetadata } from '@/lib/site/metadata';
import { absoluteUrl } from '@/lib/site/url';
import { toDateString } from '@/lib/time/zoned';

type Params = Promise<{ locale: string }>;

export async function generateMetadata({ params }: { params: Params }): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: 'gather.experiences.meta' });
  return pageMetadata({ locale, path: '/experiences', title: t('title'), description: t('description'), image: 'experiences' });
}

export default async function ExperiencesPage({ params }: { params: Params }) {
  const { locale } = await params;
  setRequestLocale(locale);
  if (!(await featureEnabled('events'))) notFound();
  const [t, events, branches] = await Promise.all([getTranslations('gather.experiences'), getUpcomingEvents(), getBranches()]);
  const now = new Date();
  const branchOf = new Map(branches.map((b) => [b.id, b]));
  const rows: EventRow[] = events
    .filter((e) => new Date(e.endsAt) > now)
    .flatMap((e) => {
      const b = branchOf.get(e.branchId);
      if (!b) return [];
      const at = new Date(e.startsAt);
      const date = toDateString(at, b.timeZone);
      const prices = e.tickets.map((x) => x.price);
      return [
        {
          slug: e.slug,
          kind: e.kind,
          title: tr(e.title, locale),
          summary: tr(e.summary, locale),
          monthKey: date.slice(0, 7),
          monthLabel: formatDate(at, locale, b.timeZone, { month: 'long', year: 'numeric' }),
          day: formatNumber(Number(date.slice(8)), locale),
          weekday: formatDate(at, locale, b.timeZone, { weekday: 'short' }),
          timeLabel: `${formatClock(at, locale, b.timeZone)} – ${formatClock(new Date(e.endsAt), locale, b.timeZone)}`,
          house: tr(b.shortName, locale),
          seatsLeft: Math.max(0, e.capacity - e.seatsTaken),
          priceFrom: prices.length ? Math.min(...prices) : null,
          image: imageView(e.image, locale),
        },
      ];
    });
  const tz = branches[0]?.timeZone ?? 'Asia/Riyadh';
  const today = toDateString(now, tz);
  const days: Record<string, string> = {};
  for (const e of events) {
    const b = branchOf.get(e.branchId);
    const d = toDateString(new Date(e.startsAt), b?.timeZone ?? tz);
    days[d] ??= e.slug;
  }
  const last = Object.keys(days).sort().at(-1) ?? today;
  const months = monthsBetween(today, last).slice(0, 2);

  return (
    <>
      <JsonLd
        data={{
          '@context': 'https://schema.org',
          '@graph': events.map((e) => {
            const b = branchOf.get(e.branchId);
            const seats = Math.max(0, e.capacity - e.seatsTaken);
            return {
              '@type': 'Event',
              name: tr(e.title, locale),
              description: tr(e.summary, locale),
              startDate: e.startsAt,
              endDate: e.endsAt,
              eventStatus: 'https://schema.org/EventScheduled',
              eventAttendanceMode: 'https://schema.org/OfflineEventAttendanceMode',
              url: absoluteUrl(`/${locale}/experiences/${e.slug}`),
              image: e.image ? absoluteUrl(e.image.src) : undefined,
              location: b ? { '@type': 'Place', name: tr(b.name, locale), address: tr(b.address, locale) } : undefined,
              offers: e.tickets.map((x) => ({ '@type': 'Offer', name: tr(x.name, locale), price: (x.price / 100).toFixed(2), priceCurrency: restaurantConfig.currency, availability: seats > 0 ? 'https://schema.org/InStock' : 'https://schema.org/SoldOut', url: absoluteUrl(`/${locale}/experiences/${e.slug}`) })),
            };
          }),
        }}
      />
      <PageHeader eyebrow={t('eyebrow')} title={t('title')} intro={<p>{t('intro')}</p>} />
      <div className="site-grid gap-y-14 pb-[var(--spacing-section)]">
        <div className="col-span-full lg:col-span-8">
          <EventLedger events={rows} />
        </div>
        {months.length ? (
          <aside className="col-span-full lg:col-span-3 lg:col-start-10">
            <div className="lg:sticky lg:top-24">
              <EventCalendar months={months} days={days} today={today} locale={locale} label={t('calendar')} />
            </div>
          </aside>
        ) : null}
      </div>
    </>
  );
}
