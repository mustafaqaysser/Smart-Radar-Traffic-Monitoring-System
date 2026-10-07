import type { Metadata } from 'next';
import { notFound } from 'next/navigation';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import restaurantConfig from '@config';
import { AccountHeader, LedgerBlock } from '@/components/site/account/account-header';
import { Link } from '@/i18n/navigation';
import { getCurrentUser } from '@/lib/auth/session';
import { redemptionValue } from '@/lib/domain/loyalty';
import { formatDate, formatMoney, formatNumber } from '@/lib/i18n/format';
import { tr } from '@/lib/i18n/localized';
import { plural } from '@/lib/i18n/plural';
import { getLoyaltyRewards } from '@/lib/queries/content';
import { loyaltyHistory, loyaltyStanding } from '@/lib/server/account';
import { featureEnabled } from '@/lib/server/settings';
import { pageMetadata } from '@/lib/site/metadata';
import { cn } from '@/lib/utils/cn';

type Params = Promise<{ locale: string }>;

export async function generateMetadata({ params }: { params: Params }): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: 'account.meta' });
  return pageMetadata({ locale, path: '/account/loyalty', title: t('loyalty'), noindex: true });
}

const PHASE_OF_TIER: Record<string, string> = { morning: 'morning', 'long-shade': 'afternoon', night: 'night' };

export default async function LoyaltyPage({ params }: { params: Params }) {
  const { locale } = await params;
  setRequestLocale(locale);
  if (!(await featureEnabled('loyalty'))) notFound();
  const user = await getCurrentUser();
  if (!user) return null;
  const [t, tu, rewards, history] = await Promise.all([getTranslations('account'), getTranslations('common.units'), getLoyaltyRewards(), loyaltyHistory(user.id)]);
  const rules = restaurantConfig.loyalty;
  const standing = loyaltyStanding(user);
  const points = (n: number) => tu('points', plural(n, locale));
  const tz = restaurantConfig.defaultTimeZone;

  return (
    <>
      <AccountHeader title={t('loyalty.title')} intro={t('loyalty.intro')} />
      <section data-phase={PHASE_OF_TIER[standing.tier.id] ?? 'morning'} className="grid gap-8 bg-bg p-7 text-ink cast-shade md:grid-cols-2 md:p-10" aria-label={t('loyalty.balance')}>
        <div className="flex flex-col gap-2">
          <p className="t-label text-muted">{t('loyalty.balance')}</p>
          <p className="t-display-lg tabular">{formatNumber(standing.points, locale)}</p>
        </div>
        <div className="flex flex-col gap-3 md:items-end md:text-end">
          <p className="t-label text-muted">{t('loyalty.tier')}</p>
          <p className="t-display-md">{tr(standing.tier.name, locale)}</p>
          <p className="t-small text-accent">{t('loyalty.multiplier', { n: formatNumber(standing.tier.multiplier, locale) })}</p>
        </div>
        <div className="flex flex-col gap-3 md:col-span-2">
          <div className="h-1 w-full bg-line" role="progressbar" aria-valuemin={0} aria-valuemax={100} aria-valuenow={Math.round(standing.progress * 100)} aria-label={t('loyalty.tier')}>
            <div className="h-full bg-sun" style={{ width: `${Math.round(standing.progress * 100)}%` }} />
          </div>
          <p className="t-small text-muted">{standing.next ? t('overview.loyalty.toNext', { points: points(standing.next.pointsToGo), tier: tr(standing.next.tier.name, locale) }) : t('overview.loyalty.top')}</p>
        </div>
      </section>

      <p className="t-body measure">{t('loyalty.redeem', { points: points(rules.pointsPerUnitRedeemed * 10), amount: formatMoney(redemptionValue(rules.pointsPerUnitRedeemed * 10, rules), locale) })}</p>

      {rewards.length ? (
        <LedgerBlock title={t('loyalty.rewards')}>
          <ul className="-mt-5">
            {rewards.map((r) => {
              const tierOk = !r.minTier || rules.tiers.findIndex((x) => x.id === r.minTier) <= rules.tiers.findIndex((x) => x.id === standing.tier.id);
              const within = tierOk && standing.points >= r.pointsCost;
              return (
                <li key={r.id} className={cn('grid gap-2 border-b border-line py-5 md:grid-cols-12 md:items-baseline md:gap-6', !within && 'opacity-70')}>
                  <p className="t-heading-sm tabular text-accent md:col-span-3">{points(r.pointsCost)}</p>
                  <div className="flex flex-col gap-1 md:col-span-9">
                    <p className="t-heading-sm">{tr(r.name, locale)}</p>
                    {r.description ? <p className="t-small text-muted">{tr(r.description, locale)}</p> : null}
                  </div>
                </li>
              );
            })}
          </ul>
        </LedgerBlock>
      ) : null}

      <LedgerBlock
        title={t('loyalty.history')}
        action={
          <Link href="/membership" className="t-small underline underline-offset-4">
            {t('loyalty.about')}
          </Link>
        }
      >
        {history.length ? (
          <ul className="-mt-5">
            {history.map((h) => (
              <li key={h.id} className="flex flex-wrap items-baseline justify-between gap-3 border-b border-line py-4">
                <span className="t-body">
                  {t(`loyalty.kinds.${h.kind}`)}
                  <span className="t-small text-muted"> · {formatDate(h.at, locale, tz, { day: 'numeric', month: 'long', year: 'numeric' })}</span>
                </span>
                <span className={cn('t-body tabular', h.points < 0 ? 'text-muted' : 'text-accent')}>
                  <bdi>{h.points > 0 ? '+' : '−'}{formatNumber(Math.abs(h.points), locale)}</bdi>
                </span>
              </li>
            ))}
          </ul>
        ) : (
          <p className="t-body text-muted">{t('loyalty.noHistory')}</p>
        )}
      </LedgerBlock>
    </>
  );
}
