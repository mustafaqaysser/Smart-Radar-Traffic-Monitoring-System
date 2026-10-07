import type { Metadata } from 'next';
import { notFound } from 'next/navigation';
import restaurantConfig from '@config';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import { SplitWords } from '@/components/motion/split-words';
import { Reveal } from '@/components/motion/reveal';
import { TicketForm } from '@/components/site/experiences/ticket-form';
import { JsonLd } from '@/components/site/json-ld';
import { BookingRows } from '@/components/site/reserve/booking-pieces';
import { BackLink, ButtonLink } from '@/components/site/ui/button';
import { MediaImage } from '@/components/site/ui/media-image';
import { getCurrentUser } from '@/lib/auth/session';
import { formatClock, formatDate } from '@/lib/i18n/format';
import { tr } from '@/lib/i18n/localized';
import { plural } from '@/lib/i18n/plural';
import { getBranch } from '@/lib/queries/branches';
import { getEvent } from '@/lib/queries/content';
import { sanitizeRichText } from '@/lib/server/sanitize';
import { featureEnabled } from '@/lib/server/settings';
import { pageMetadata } from '@/lib/site/metadata';
import { absoluteUrl } from '@/lib/site/url';

type Params = Promise<{ locale: string; slug: string }>;

export async function generateMetadata({ params }: { params: Params }): Promise<Metadata> {
  const { locale, slug } = await params;
  const event = await getEvent(slug);
  if (!event) return {};
  return pageMetadata({ locale, path: `/experiences/${slug}`, title: tr(event.title, locale), description: tr(event.summary, locale), image: event.image?.src ?? 'experiences' });
}

export default async function EventPage({ params }: { params: Params }) {
  const { locale, slug } = await params;
  setRequestLocale(locale);
  if (!(await featureEnabled('events'))) notFound();
  const event = await getEvent(slug);
  if (!event) notFound();
  const branch = await getBranch(event.branchId);
  if (!branch) notFound();
  const [t, user] = await Promise.all([getTranslations('gather.experiences'), getCurrentUser()]);
  const start = new Date(event.startsAt);
  const end = new Date(event.endsAt);
  const seatsLeft = Math.max(0, event.capacity - event.seatsTaken);
  const started = start <= new Date();
  const title = tr(event.title, locale);

  return (
    <article className="site-grid gap-y-12 pt-8 pb-[var(--spacing-section)] lg:pt-12">
      <JsonLd
        data={{
          '@context': 'https://schema.org',
          '@type': 'Event',
          name: title,
          description: tr(event.summary, locale),
          startDate: event.startsAt,
          endDate: event.endsAt,
          eventStatus: 'https://schema.org/EventScheduled',
          eventAttendanceMode: 'https://schema.org/OfflineEventAttendanceMode',
          image: event.image ? absoluteUrl(event.image.src) : undefined,
          location: { '@type': 'Place', name: tr(branch.name, locale), address: tr(branch.address, locale) },
          offers: event.tickets.map((x) => ({ '@type': 'Offer', name: tr(x.name, locale), price: (x.price / 100).toFixed(2), priceCurrency: restaurantConfig.currency, availability: seatsLeft > 0 ? 'https://schema.org/InStock' : 'https://schema.org/SoldOut' })),
        }}
      />
      <nav className="col-span-full">
        <BackLink href="/experiences">{t('back')}</BackLink>
      </nav>
      <header className="col-span-full flex flex-col gap-6 lg:col-span-7">
        <p className="t-label text-muted">
          {t(`kinds.${event.kind}`)} · {tr(branch.shortName, locale)}
        </p>
        <h1 className="t-display-lg">
          <SplitWords text={title} />
        </h1>
        <p className="t-body-lg measure">{tr(event.summary, locale)}</p>
        <BookingRows
          rows={[
            { label: t('when'), value: `${formatDate(start, locale, branch.timeZone, { weekday: 'long', day: 'numeric', month: 'long' })} · ${formatClock(start, locale, branch.timeZone)} – ${formatClock(end, locale, branch.timeZone)}` },
            { label: t('where'), value: tr(branch.name, locale) },
            { label: t('seats'), value: seatsLeft ? t('seatsLeft', plural(seatsLeft, locale)) : t('soldOut') },
          ]}
        />
        {event.body ? <div className="prose-zill measure" dangerouslySetInnerHTML={{ __html: sanitizeRichText(tr(event.body, locale)) }} /> : null}
      </header>
      <div className="col-span-full flex flex-col gap-8 lg:col-span-4 lg:col-start-9">
        {event.image ? (
          <Reveal variant="shade">
            <MediaImage media={event.image} locale={locale} sizes="(min-width: 1024px) 30vw, 100vw" ratio="4/5" shape="arch-4x5" className="cast-shade" preload />
          </Reveal>
        ) : null}
        <section aria-labelledby="tickets-title" className="flex flex-col gap-6 bg-raised p-6">
          <h2 id="tickets-title" className="t-heading-md">
            {t('book')}
          </h2>
          {started ? (
            <p className="t-body">{t('started')}</p>
          ) : seatsLeft === 0 ? (
            <div className="flex flex-col gap-4">
              <p className="t-body">{t('soldOutBody')}</p>
              <ButtonLink href="/newsletter" variant="secondary" className="self-start">
                {t('notify')}
              </ButtonLink>
            </div>
          ) : (
            <TicketForm
              event={event.slug}
              seatsLeft={seatsLeft}
              tickets={event.tickets.map((x) => ({ id: x.id, name: tr(x.name, locale), description: x.description ? tr(x.description, locale) : null, price: x.price, left: x.capacity === null ? seatsLeft : Math.max(0, Math.min(seatsLeft, x.capacity - x.sold)) }))}
              user={user ? { name: user.name, email: user.email, phone: user.phone ?? '' } : null}
            />
          )}
        </section>
      </div>
    </article>
  );
}
