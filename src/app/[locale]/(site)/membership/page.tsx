import type { Metadata } from 'next';
import { notFound } from 'next/navigation';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import restaurantConfig from '@config';
import { Icon, type IconName } from '@/components/brand/icon';
import { Reveal } from '@/components/motion/reveal';
import { ButtonLink } from '@/components/site/ui/button';
import { PageHeader } from '@/components/site/ui/page-header';
import { getCurrentUser } from '@/lib/auth/session';
import { formatMoney, formatNumber } from '@/lib/i18n/format';
import { tr } from '@/lib/i18n/localized';
import { plural } from '@/lib/i18n/plural';
import { getLoyaltyRewards } from '@/lib/queries/content';
import { featureEnabled } from '@/lib/server/settings';
import { pageMetadata } from '@/lib/site/metadata';

export async function generateMetadata({ params }: { params: Promise<{ locale: string }> }): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: 'pages.membership.meta' });
  return pageMetadata({ locale, path: '/membership', title: t('title'), description: t('description'), image: 'membership' });
}

const PHASE_OF_TIER: Record<string, string> = { morning: 'morning', 'long-shade': 'afternoon', night: 'night' };

export default async function MembershipPage({ params }: { params: Promise<{ locale: string }> }) {
  const { locale } = await params;
  setRequestLocale(locale);
  if (!(await featureEnabled('loyalty'))) notFound();
  const [t, rewards, user] = await Promise.all([getTranslations('pages.membership'), getLoyaltyRewards(), getCurrentUser()]);
  const rules = restaurantConfig.loyalty;
  const tierName = (id: string) => {
    const tier = rules.tiers.find((x) => x.id === id);
    return tier ? tr(tier.name, locale) : id;
  };
  const how: { icon: IconName; text: string }[] = [
    { icon: 'star', text: t('how.earn', plural(rules.pointsPerUnit, locale)) },
    { icon: 'receipt', text: t('how.redeem', plural(rules.pointsPerUnitRedeemed, locale)) },
    { icon: 'gift', text: t('how.bonus', plural(rules.signupBonus, locale)) },
    { icon: 'pin', text: t('how.both') },
  ];
  return (
    <>
      <PageHeader eyebrow={t('eyebrow')} title={t('title')} intro={<p>{t('intro')}</p>}>
        <div>
          <ButtonLink href="/account" size="lg">
            {user ? t('join.signedIn') : t('join.cta')}
          </ButtonLink>
        </div>
      </PageHeader>
      <section className="site-grid gap-y-8" aria-labelledby="m-how">
        <h2 id="m-how" className="t-heading-lg col-span-full">
          {t('how.title')}
        </h2>
        <ul className="col-span-full grid gap-[var(--spacing-gutter)] border-t border-ink pt-8 md:grid-cols-2 lg:grid-cols-4">
          {how.map((h, i) => (
            <Reveal as="li" key={i} delay={i * 80} className="flex flex-col gap-4">
              <Icon name={h.icon} size={28} className="text-accent" />
              <p className="t-body">{h.text}</p>
            </Reveal>
          ))}
        </ul>
      </section>
      <section className="site-grid gap-y-8 pt-[var(--spacing-section)]" aria-labelledby="m-tiers">
        <h2 id="m-tiers" className="t-display-md col-span-full">
          {t('tiers.title')}
        </h2>
        <ol className="col-span-full grid gap-[var(--spacing-gutter)] md:grid-cols-3">
          {rules.tiers.map((tier, i) => (
            <Reveal as="li" key={tier.id} delay={i * 100}>
              {/* Each tier is shown in the palette of its hour. */}
              <div data-phase={PHASE_OF_TIER[tier.id] ?? 'morning'} className="flex h-full flex-col gap-4 bg-bg p-8 text-ink lg:p-10">
                <p className="t-instrument text-muted">{tier.minPoints > 0 ? t('tiers.threshold', plural(tier.minPoints, locale)) : t('tiers.start')}</p>
                <h3 className="t-display-md">{tierName(tier.id)}</h3>
                <p className="t-heading-sm text-accent">{t('tiers.multiplier', { n: formatNumber(tier.multiplier, locale) })}</p>
                {t.has(`tiers.${tier.id}.body`) ? <p className="t-body">{t(`tiers.${tier.id}.body`)}</p> : null}
              </div>
            </Reveal>
          ))}
        </ol>
      </section>
      {rewards.length ? (
        <section className="site-grid gap-y-8 pt-[var(--spacing-section)]" aria-labelledby="m-rewards">
          <h2 id="m-rewards" className="t-display-md col-span-full">
            {t('rewards.title')}
          </h2>
          <ul className="col-span-full border-t border-ink">
            {rewards.map((r) => (
              <li key={r.id} className="grid gap-3 border-b border-line py-8 md:grid-cols-12 md:items-baseline">
                <p className="t-heading-sm tabular text-accent md:col-span-3">{t('rewards.cost', plural(r.pointsCost, locale))}</p>
                <div className="flex flex-col gap-2 md:col-span-6">
                  <h3 className="t-heading-md">{tr(r.name, locale)}</h3>
                  {r.description ? <p className="t-body text-muted">{tr(r.description, locale)}</p> : null}
                </div>
                <p className="t-small text-muted md:col-span-3 md:text-end">
                  {t('rewards.value', { price: formatMoney(r.value, locale) })}
                  {r.minTier ? (
                    <>
                      <br />
                      {t('rewards.tier', { tier: tierName(r.minTier) })}
                    </>
                  ) : null}
                </p>
              </li>
            ))}
          </ul>
        </section>
      ) : null}
      <section className="site-grid gap-y-6 pt-[var(--spacing-section)]" aria-labelledby="m-join">
        <h2 id="m-join" className="t-display-md col-span-full">
          {t('join.title')}
        </h2>
        <p className="t-body-lg measure col-span-full">{t('join.body')}</p>
        <div className="col-span-full">
          <ButtonLink href="/account" size="lg">
            {user ? t('join.signedIn') : t('join.cta')}
          </ButtonLink>
        </div>
      </section>
    </>
  );
}
