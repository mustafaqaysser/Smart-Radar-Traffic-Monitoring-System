import type { Metadata } from 'next';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import restaurantConfig from '@config';
import { Hero } from '@/components/site/home/hero';
import { DayChapter, type DayHour } from '@/components/site/home/day-chapter';
import { ChefSection, ConceptSection, ExperiencesSection, FindUsSection, RoomSection, SignatureSection, VoicesSection } from '@/components/site/home/sections';
import { ReserveTeaser } from '@/components/site/home/reserve-teaser';
import { SectionHeading } from '@/components/site/ui/section-heading';
import { JsonLd, restaurantJsonLd } from '@/components/site/json-ld';
import type { Phase } from '@/lib/brand/palette';
import { minutesUntilServing } from '@/lib/domain/menus';
import { formatClock, formatWallTime } from '@/lib/i18n/format';
import { tr } from '@/lib/i18n/localized';
import { getBranches } from '@/lib/queries/branches';
import { getMediaIndex, getMenuCatalog } from '@/lib/queries/catalog';
import { getContentBlocks, getPress, getPublishedReviews, getReviewStats, getTeam, getUpcomingEvents } from '@/lib/queries/content';
import type { MenuDTO, MenuItemDTO } from '@/lib/queries/types';
import { getSettings } from '@/lib/server/settings';
import { getSelectedBranch } from '@/lib/site/selection';
import { getServingContext, servingNames } from '@/lib/site/serving';
import { blockText } from '@/lib/site/blocks';
import { sunTimes } from '@/lib/time/sun';
import { addDays } from '@/lib/time/zoned';
import { pageMetadata } from '@/lib/site/metadata';

export async function generateMetadata({ params }: { params: Promise<{ locale: string }> }): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: 'home.meta' });
  return pageMetadata({ locale, path: '', title: null, description: t('description'), image: 'home' });
}

const DAY_PHOTOS: Record<Phase, string[]> = {
  dawn: ['craft-bread-oven', 'place-palm-grove'],
  morning: ['dish-shakshuka-loomi', 'dish-tamees-foul'],
  noon: ['place-courtyard-sun', 'dish-lamb-mandi'],
  afternoon: ['dish-khawlani-pour-over', 'place-palm-shadow-2'],
  dusk: ['place-coral-house', 'place-lantern'],
  night: ['place-night-courtyard', 'place-window-beam'],
};
const DAY_MENU: Partial<Record<Phase, string>> = { morning: 'breakfast', noon: 'lunch', afternoon: 'afternoon', night: 'dinner' };
const DAY_TIME: Record<Phase, string> = { dawn: '05:30', morning: '07:00', noon: '12:30', afternoon: '16:00', dusk: '18:00', night: '19:00' };

export default async function HomePage({ params }: { params: Promise<{ locale: string }> }) {
  const { locale } = await params;
  setRequestLocale(locale);
  const [t, branch, branches, catalog, media, blocks, team, events, reviews, press, stats, settings] = await Promise.all([
    getTranslations('home'),
    getSelectedBranch(),
    getBranches(),
    getMenuCatalog(),
    getMediaIndex(),
    getContentBlocks('home'),
    getTeam(),
    getUpcomingEvents(),
    getPublishedReviews(),
    getPress(),
    getReviewStats(),
    getSettings(),
  ]);
  const serving = await getServingContext(branch);
  const pick = (...names: string[]) => names.map((n) => media[n]).find(Boolean) ?? null;
  const tz = branch?.timeZone ?? restaurantConfig.defaultTimeZone;

  // The menu of the hour: what is served now, or the next menu to open.
  const timed = serving.serving.filter((m) => m.schedule.length > 0);
  const next: MenuDTO | undefined =
    timed[0] ??
    [...serving.menus]
      .filter((m) => m.schedule.length > 0)
      .map((m) => ({ m, wait: minutesUntilServing(m, serving.now, tz) ?? Infinity }))
      .sort((a, b) => a.wait - b.wait)[0]?.m;
  const itemsOf = (menu: MenuDTO | undefined) => (menu ? menu.categories.flatMap((c) => c.items).map((slug) => catalog.items[slug]).filter((i): i is MenuItemDTO => Boolean(i)) : []);
  const pool = itemsOf(next);
  const signature = [...pool.filter((i) => i.isSignature), ...pool.filter((i) => !i.isSignature && i.image)].filter((i, idx, arr) => arr.indexOf(i) === idx).slice(0, 8);
  const heroPhoto = signature.find((i) => i.image)?.image ?? next?.image ?? pick('place-arch-shadow');

  // A day in the courtyard: real dawn and maghrib for the chosen house today.
  const sun = branch ? sunTimes(serving.today, branch.lat, branch.lng) : null;
  const menuByKind = (kind: string) => catalog.menus.find((m) => m.kind === kind && (!branch || !m.branchIds || m.branchIds.includes(branch.id)));
  const hours: DayHour[] = (['dawn', 'morning', 'noon', 'afternoon', 'dusk', 'night'] as Phase[]).map((phase) => {
    const real = phase === 'dawn' ? sun?.dawn : phase === 'dusk' ? sun?.sunset : null;
    const kind = DAY_MENU[phase];
    const menu = kind ? menuByKind(kind) : undefined;
    return {
      phase,
      time: real ? formatClock(real, locale, tz) : formatWallTime(DAY_TIME[phase], locale),
      title: t(`day.hours.${phase}.title`),
      body: t(`day.hours.${phase}.body`),
      menu: menu ? tr(menu.name, locale) : null,
      photo: pick(...DAY_PHOTOS[phase]),
    };
  });

  const chef = team[0] ?? null;
  const featured = reviews.filter((r) => r.featured);
  const upcoming = events.filter((e) => new Date(e.startsAt) > serving.now);
  const bookingDates = Array.from({ length: 14 }, (_, i) => addDays(serving.today, i));

  return (
    <>
      <JsonLd data={restaurantJsonLd(branches, locale)} />
      <Hero
        locale={locale}
        line={blockText(blocks.hero, 'line', locale) ?? t('hero.line')}
        intro={blockText(blocks.hero, 'intro', locale) ?? t('hero.intro')}
        servingNow={servingNames(serving, locale)}
        photo={heroPhoto}
        photoMenu={next ? tr(next.hour ?? next.name, locale) : null}
        reservations={settings.features.reservations}
      />
      <ConceptSection locale={locale} title={blockText(blocks.concept, 'title', locale) ?? t('concept.title')} body={blockText(blocks.concept, 'body', locale) ?? t('concept.body')} photo={pick('place-arch-shadow', 'place-lattice-light')} />
      <DayChapter locale={locale} eyebrow={t('day.eyebrow')} title={t('day.title')} skipLabel={t('day.skip')} hours={hours} />
      <SignatureSection locale={locale} items={signature} branch={branch} serving={timed.length > 0} />
      <RoomSection locale={locale} branches={branches} photos={[pick('place-coral-house', 'place-courtyard-sun'), pick('place-lantern'), pick('place-mudbrick', 'place-door')].filter((m) => m !== null)} />
      <ChefSection
        locale={locale}
        quote={blockText(blocks.chef, 'quote', locale) ?? t('chef.quote')}
        name={chef ? tr(chef.name, locale) : null}
        role={chef ? tr(chef.role, locale) : null}
        photo={chef?.image ?? pick('craft-plating')}
      />
      {settings.features.events ? <ExperiencesSection locale={locale} events={upcoming} branches={branches} /> : null}
      {settings.features.reviews ? <VoicesSection locale={locale} reviews={featured} press={press} stats={stats} /> : null}
      {settings.features.reservations ? (
        <section className="site-grid gap-y-10 pt-[var(--spacing-section)]" aria-labelledby="reserve-title">
          <SectionHeading id="reserve-title" className="col-span-full lg:col-span-8" eyebrow={t('reserve.eyebrow')} title={t('reserve.title')} intro={<p>{t('reserve.body')}</p>} />
          <div className="col-span-full border-t border-ink pt-8">
            <ReserveTeaser
              branches={branches.map((b) => ({ slug: b.slug, name: tr(b.shortName, locale) }))}
              selected={branch?.slug ?? null}
              dates={bookingDates}
              maxParty={restaurantConfig.reservations.maxPartyOnline}
            />
          </div>
        </section>
      ) : null}
      <FindUsSection locale={locale} branches={branches} seasons={serving.seasons} />
    </>
  );
}
