import type { Metadata } from 'next';
import { notFound } from 'next/navigation';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import restaurantConfig from '@config';
import { Reveal } from '@/components/motion/reveal';
import { SplitWords } from '@/components/motion/split-words';
import { InquiryForm, type InquiryHouse } from '@/components/site/private-dining/inquiry-form';
import { ButtonLink } from '@/components/site/ui/button';
import { MediaImage } from '@/components/site/ui/media-image';
import { Link } from '@/i18n/navigation';
import { formatMoney, formatNumber } from '@/lib/i18n/format';
import { tr } from '@/lib/i18n/localized';
import { plural } from '@/lib/i18n/plural';
import { getBranches } from '@/lib/queries/branches';
import { getMediaIndex } from '@/lib/queries/catalog';
import { getPrivateDining } from '@/lib/queries/content';
import { featureEnabled, getSettings } from '@/lib/server/settings';
import { telUrl } from '@/lib/services/maps';
import { pageMetadata } from '@/lib/site/metadata';
import { toDateString } from '@/lib/time/zoned';
import { cn } from '@/lib/utils/cn';

type Params = Promise<{ locale: string }>;
type Search = Promise<{ room?: string; package?: string; kind?: string }>;

export async function generateMetadata({ params }: { params: Params }): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: 'gather.privateDining.meta' });
  return pageMetadata({ locale, path: '/private-dining', title: t('title'), description: t('description'), image: 'private-dining' });
}

const ask = (query: Record<string, string>) => ({ pathname: '/private-dining' as const, query, hash: 'inquiry' });

export default async function PrivateDiningPage({ params, searchParams }: { params: Params; searchParams: Search }) {
  const { locale } = await params;
  setRequestLocale(locale);
  if (!(await featureEnabled('privateDining'))) notFound();
  const [search, t, { rooms, packages }, branches, settings, media] = await Promise.all([searchParams, getTranslations('gather.privateDining'), getPrivateDining(), getBranches(), getSettings(), getMediaIndex()]);
  const branchOf = new Map(branches.map((b) => [b.id, b]));
  const dining = packages.filter((p) => p.kind === 'private_dining');
  const catering = packages.filter((p) => p.kind === 'catering');
  const houses: InquiryHouse[] = branches.map((b) => ({
    slug: b.slug,
    name: tr(b.shortName, locale),
    city: tr(b.city, locale),
    rooms: rooms.filter((r) => r.branchId === b.id).map((r) => ({ slug: r.slug, name: tr(r.name, locale), max: Math.max(r.seated, r.standing ?? 0) })),
  }));
  const one = (v: string | undefined) => (typeof v === 'string' ? v.slice(0, 64) : '');
  const requestedPackage = packages.find((p) => p.id === one(search.package));
  const initial = {
    kind: requestedPackage?.kind ?? (search.kind === 'catering' ? 'catering' : 'private_dining'),
    room: one(search.room),
    package: requestedPackage?.id ?? '',
  } as const;
  const steps = t.raw('how.steps') as { title: string; body: string }[];
  const lead = media['place-lantern'] ?? null;

  return (
    <>
      <header className="site-grid gap-y-10 pt-10 pb-[var(--spacing-stack)] lg:pt-16">
        <div className="col-span-full flex flex-col gap-6 md:col-span-4 lg:col-span-6 lg:self-end">
          <Reveal as="p" variant="fade" className="t-label text-muted">
            {t('eyebrow')}
          </Reveal>
          <h1 className="t-display-lg">
            <SplitWords text={t('title')} />
          </h1>
          <Reveal as="p" delay={160} className="t-body-lg measure">
            {t('intro')}
          </Reveal>
          <div className="flex flex-wrap gap-3">
            <ButtonLink href={ask({})} size="lg">
              {t('inquiry.title')}
            </ButtonLink>
            {catering.length ? (
              <ButtonLink href={{ pathname: '/private-dining', hash: 'catering' }} variant="secondary" size="lg" icon={null}>
                {t('catering')}
              </ButtonLink>
            ) : null}
          </div>
        </div>
        {lead ? (
          <div className="col-span-full md:col-span-4 lg:col-span-5 lg:col-start-8">
            <MediaImage media={lead} locale={locale} sizes="(min-width: 1024px) 38vw, (min-width: 768px) 50vw, 100vw" shape="arch-4x5" ratio="4/5" preload className="cast-shade" />
          </div>
        ) : null}
      </header>

      <section aria-labelledby="pd-rooms" className="site-grid gap-y-10 pb-[var(--spacing-section)]">
        <div className="col-span-full flex flex-col gap-4 border-t border-ink pt-8 lg:col-span-7">
          <h2 id="pd-rooms" className="t-heading-lg">
            {t('rooms')}
          </h2>
          <p className="t-body measure text-muted">{t('roomsIntro')}</p>
        </div>
        <ol className="col-span-full flex flex-col gap-[var(--spacing-stack)]">
          {rooms.map((r, i) => {
            const b = branchOf.get(r.branchId);
            return (
              <li key={r.id} id={`room-${r.slug}`} className="grid scroll-mt-24 gap-8 md:grid-cols-12 md:items-center">
                {r.image ? (
                  <Reveal as="div" variant="fade" className={cn('md:col-span-5', i % 2 === 1 && 'md:order-2 md:col-start-8')}>
                    <MediaImage media={r.image} locale={locale} sizes="(min-width: 768px) 40vw, 100vw" ratio="4/3" />
                  </Reveal>
                ) : null}
                <div className={cn('flex flex-col gap-4 md:col-span-6', i % 2 === 1 ? 'md:order-1 md:col-start-1' : 'md:col-start-7')}>
                  {b ? <p className="t-label text-muted">{t('atHouse', { house: tr(b.shortName, locale) })}</p> : null}
                  <h3 className="t-heading-lg">{tr(r.name, locale)}</h3>
                  <p className="t-small tabular">
                    {t('seated', plural(r.seated, locale))}
                    {r.standing ? <> · {t('standing', { n: formatNumber(r.standing, locale) })}</> : null}
                  </p>
                  <p className="t-body measure">{tr(r.description, locale)}</p>
                  {r.features.length ? (
                    <ul className="flex flex-col gap-2 border-t border-line pt-4">
                      {r.features.map((f) => (
                        <li key={f.en} className="t-small flex items-baseline gap-3">
                          <span aria-hidden="true" className="inline-block size-1.5 shrink-0 translate-y-[-0.15em] rounded-full bg-sun" />
                          {tr(f, locale)}
                        </li>
                      ))}
                    </ul>
                  ) : null}
                  <Link href={ask({ room: r.slug })} className="t-small inline-flex items-center gap-2 self-start underline underline-offset-4">
                    {t('ask')}
                  </Link>
                </div>
              </li>
            );
          })}
        </ol>
      </section>

      <section aria-labelledby="pd-how" className="site-grid gap-y-8 pb-[var(--spacing-section)]">
        <h2 id="pd-how" className="t-heading-lg col-span-full border-t border-ink pt-8">
          {t('how.title')}
        </h2>
        <ol className="col-span-full grid gap-8 sm:grid-cols-2 lg:grid-cols-4">
          {steps.map((s, i) => (
            <li key={s.title} className="flex flex-col gap-3 border-t border-line pt-5">
              <span className="t-display-md text-sun tabular" aria-hidden="true">
                {formatNumber(i + 1, locale)}
              </span>
              <h3 className="t-heading-sm">{s.title}</h3>
              <p className="t-small text-muted">{s.body}</p>
            </li>
          ))}
        </ol>
      </section>

      {dining.length ? (
        <section aria-labelledby="pd-packages" className="site-grid gap-y-8 pb-[var(--spacing-section)]">
          <div className="col-span-full flex flex-col gap-4 border-t border-ink pt-8 lg:col-span-7">
            <h2 id="pd-packages" className="t-heading-lg">
              {t('packages')}
            </h2>
            <p className="t-body measure text-muted">{t('packagesIntro')}</p>
          </div>
          <ul className="col-span-full border-t border-line">
            {dining.map((p) => (
              <li key={p.id} className="grid gap-4 border-b border-line py-7 md:grid-cols-12 md:gap-8">
                <h3 className="t-heading-md md:col-span-4">{tr(p.name, locale)}</h3>
                <p className="t-body md:col-span-5">{tr(p.description, locale)}</p>
                <div className="flex flex-col gap-2 md:col-span-3 md:items-end md:text-end">
                  <p className="t-heading-sm tabular">
                    <bdi>{t('perGuest', { price: formatMoney(p.pricePerGuest, locale) })}</bdi>
                  </p>
                  <p className="t-small text-muted">{t('minGuests', { n: formatNumber(p.minGuests, locale) })}</p>
                  <Link href={ask({ package: p.id })} className="t-small underline underline-offset-4">
                    {t('askPackage')}
                  </Link>
                </div>
              </li>
            ))}
          </ul>
        </section>
      ) : null}

      {catering.length ? (
        <section id="catering" aria-labelledby="pd-catering" data-phase="night" className="scroll-mt-16 bg-bg py-[var(--spacing-section)] text-ink">
          <div className="site-grid gap-y-10">
            <div className="col-span-full flex flex-col gap-5 lg:col-span-5">
              <h2 id="pd-catering" className="t-display-md">
                {t('catering')}
              </h2>
              <p className="t-body-lg measure">{t('cateringIntro')}</p>
              <ButtonLink href={ask({ kind: 'catering' })} size="lg" className="self-start">
                {t('askCatering')}
              </ButtonLink>
            </div>
            <ul className="col-span-full flex flex-col gap-8 lg:col-span-6 lg:col-start-7">
              {catering.map((p) => (
                <li key={p.id} className="flex flex-col gap-3 border-t border-line pt-6">
                  <h3 className="t-heading-md">{tr(p.name, locale)}</h3>
                  <p className="t-body">{tr(p.description, locale)}</p>
                  <p className="t-small tabular text-muted">
                    <bdi>{t('perGuest', { price: formatMoney(p.pricePerGuest, locale) })}</bdi> · {t('minGuests', { n: formatNumber(p.minGuests, locale) })}
                  </p>
                </li>
              ))}
            </ul>
          </div>
        </section>
      ) : null}

      <section id="inquiry" aria-labelledby="pd-inquiry" className="site-grid scroll-mt-16 gap-y-10 pt-[var(--spacing-section)] pb-[var(--spacing-section)]">
        <div className="col-span-full flex flex-col gap-4 lg:col-span-4">
          <h2 id="pd-inquiry" className="t-heading-lg">
            {t('inquiry.title')}
          </h2>
          <p className="t-body text-muted">{t('inquiry.body')}</p>
          <dl className="mt-4 flex flex-col gap-3">
            <div className="border-t border-line pt-3">
              <dt className="t-small text-muted">{t('host')}</dt>
              <dd>
                <a href={`mailto:${settings.contact.events}`} dir="ltr" className="t-body underline decoration-line underline-offset-4">
                  {settings.contact.events}
                </a>
              </dd>
            </div>
            {branches.map((b) => (
              <div key={b.id} className="border-t border-line pt-3">
                <dt className="t-small text-muted">{tr(b.shortName, locale)}</dt>
                <dd>
                  <a href={telUrl(b.phone)} dir="ltr" className="t-body tabular underline decoration-line underline-offset-4">
                    {b.phone}
                  </a>
                </dd>
              </div>
            ))}
          </dl>
        </div>
        <div className="col-span-full lg:col-span-7 lg:col-start-6">
          <InquiryForm key={`${initial.kind}:${initial.room}:${initial.package}`} houses={houses} packages={packages.map((p) => ({ id: p.id, name: tr(p.name, locale), kind: p.kind, minGuests: p.minGuests }))} today={toDateString(new Date(), restaurantConfig.defaultTimeZone)} initial={initial} />
        </div>
      </section>
    </>
  );
}
