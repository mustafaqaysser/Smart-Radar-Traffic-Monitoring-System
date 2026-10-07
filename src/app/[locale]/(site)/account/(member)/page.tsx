import type { Metadata } from 'next';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import { Icon } from '@/components/brand/icon';
import { AccountHeader, LedgerBlock } from '@/components/site/account/account-header';
import { VerifyEmail } from '@/components/site/account/verify-email';
import { ButtonLink } from '@/components/site/ui/button';
import { Link } from '@/i18n/navigation';
import { getCurrentUser } from '@/lib/auth/session';
import { computeAtmosphere } from '@/lib/brand/atmosphere';
import { formatList } from '@/lib/i18n/format';
import { tr } from '@/lib/i18n/localized';
import { plural } from '@/lib/i18n/plural';
import { toDietProfile } from '@/lib/menu/filter';
import { getBranches } from '@/lib/queries/branches';
import { accountOrders, accountReservations, ACTIVE_ORDER_STATUSES, loyaltyStanding } from '@/lib/server/account';
import { trackingPath } from '@/lib/server/orders';
import { describeReservation } from '@/lib/server/reservations';
import { getSettings } from '@/lib/server/settings';
import { linkToken } from '@/lib/server/tokens';
import { pageMetadata } from '@/lib/site/metadata';
import { getSelectedBranch } from '@/lib/site/selection';

type Params = Promise<{ locale: string }>;

export async function generateMetadata({ params }: { params: Params }): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: 'account.meta' });
  return pageMetadata({ locale, path: '/account', title: t('overview'), noindex: true });
}

export default async function AccountOverviewPage({ params }: { params: Params }) {
  const { locale } = await params;
  setRequestLocale(locale);
  const user = await getCurrentUser();
  if (!user) return null;
  const now = new Date();
  const [t, tc, tu, settings, branches, selected, { upcoming }, orders] = await Promise.all([
    getTranslations('account'),
    getTranslations('common'),
    getTranslations('common.units'),
    getSettings(),
    getBranches(),
    getSelectedBranch(),
    accountReservations(user, now),
    accountOrders(user, 10),
  ]);
  const f = settings.features;
  const here = selected ?? branches[0];
  const phase = here ? computeAtmosphere(now, here).phase : 'noon';
  const firstName = user.name.trim().split(/\s+/)[0] ?? '';
  const greeting = firstName && firstName !== '—' ? t(`overview.greeting.${phase}`, { name: firstName }) : t(`overview.greetingPlain.${phase}`);

  const branchOf = new Map(branches.map((b) => [b.id, b]));
  const next = upcoming[0];
  const nextBranch = next ? branchOf.get(next.branchId) : undefined;
  const nextLabels = next && nextBranch ? await describeReservation(next, nextBranch, locale) : null;
  const active = orders.filter((o) => ACTIVE_ORDER_STATUSES.includes(o.status));
  const standing = f.loyalty ? loyaltyStanding(user) : null;
  const profile = toDietProfile(user.dietary);

  return (
    <>
      <AccountHeader eyebrow={t('auth.eyebrow')} title={greeting} intro={t('overview.intro')}>
        {!firstName || firstName === '—' ? (
          <Link href="/account/profile" className="t-small inline-flex items-center gap-2 self-start underline underline-offset-4">
            <Icon name="user" size={18} />
            {t('overview.addName')}
          </Link>
        ) : null}
      </AccountHeader>

      {!user.emailVerified ? <VerifyEmail email={user.email} /> : null}

      <div className="grid gap-x-[var(--spacing-gutter)] gap-y-12 md:grid-cols-2">
        {f.reservations ? (
          <LedgerBlock title={t('overview.next.title')} className="md:col-span-2">
            {next && nextLabels ? (
              <div className="flex flex-col gap-5 md:flex-row md:items-end md:justify-between">
                <div className="flex flex-col gap-2">
                  <p className="t-display-md">
                    <bdi>{nextLabels.timeLabel}</bdi>
                  </p>
                  <p className="t-body-lg">{nextLabels.dateLabel}</p>
                  <p className="t-small text-muted">
                    {nextLabels.branchName} · {nextLabels.partyLabel}
                    {nextLabels.occasionLabel ? ` · ${nextLabels.occasionLabel}` : ''}
                  </p>
                </div>
                <div className="flex flex-wrap gap-3">
                  <ButtonLink href={`/reserve/manage/${next.code}?token=${linkToken('reservation', next.id)}`} variant="secondary" icon={null}>
                    {t('overview.next.manage')}
                  </ButtonLink>
                </div>
              </div>
            ) : (
              <div className="flex flex-wrap items-center justify-between gap-4">
                <p className="t-body text-muted">{t('overview.next.none')}</p>
                <ButtonLink href="/reserve">{t('overview.next.book')}</ButtonLink>
              </div>
            )}
          </LedgerBlock>
        ) : null}

        {active.length ? (
          <LedgerBlock title={t('overview.active.title')} className="md:col-span-2">
            <ul className="flex flex-col">
              {active.map((o) => (
                <li key={o.id} className="flex flex-wrap items-baseline justify-between gap-3 border-b border-line py-3 first:pt-0">
                  <span className="t-heading-sm">
                    <bdi>{o.number}</bdi> · {tr(branchOf.get(o.branchId)?.shortName ?? { ar: '', en: '' }, locale)}
                  </span>
                  <span className="t-small text-accent">{t(`status.order.${o.status}`)}</span>
                  <Link href={trackingPath(o)} className="t-small underline underline-offset-4">
                    {t('overview.active.track')}
                  </Link>
                </li>
              ))}
            </ul>
          </LedgerBlock>
        ) : null}

        {standing ? (
          <LedgerBlock
            title={t('overview.loyalty.title')}
            action={
              <Link href="/account/loyalty" className="t-small underline underline-offset-4">
                {t('overview.loyalty.more')}
              </Link>
            }
          >
            <div className="flex flex-col gap-3">
              <p className="t-display-md tabular">{tu('points', plural(standing.points, locale))}</p>
              <p className="t-body">{tr(standing.tier.name, locale)}</p>
              <div className="h-1 w-full bg-line" aria-hidden="true">
                <div className="h-full bg-sun" style={{ width: `${Math.round(standing.progress * 100)}%` }} />
              </div>
              <p className="t-small text-muted">{standing.next ? t('overview.loyalty.toNext', { points: tu('points', plural(standing.next.pointsToGo, locale)), tier: tr(standing.next.tier.name, locale) }) : t('overview.loyalty.top')}</p>
            </div>
          </LedgerBlock>
        ) : null}

        <LedgerBlock
          title={t('overview.dietary.title')}
          action={
            profile ? (
              <Link href="/account/dietary" className="t-small underline underline-offset-4">
                {t('overview.dietary.edit')}
              </Link>
            ) : null
          }
        >
          {profile ? (
            <ul className="flex flex-col gap-2">
              {profile.avoid.length ? <li className="t-body">{t('overview.dietary.avoids', { list: formatList(profile.avoid.map((a) => tc(`allergens.${a}`)), locale) })}</li> : null}
              {profile.diets.length ? <li className="t-body">{t('overview.dietary.eats', { list: formatList(profile.diets.map((d) => tc(`dietary.${d}`)), locale) })}</li> : null}
              {profile.maxSpice !== null ? <li className="t-body">{t('overview.dietary.heat', { level: tc(`spice.${profile.maxSpice}`) })}</li> : null}
            </ul>
          ) : (
            <div className="flex flex-col items-start gap-4">
              <p className="t-body text-muted">{t('overview.dietary.none')}</p>
              <ButtonLink href="/account/dietary" variant="secondary" icon={null} leadingIcon="leaf">
                {t('overview.dietary.set')}
              </ButtonLink>
            </div>
          )}
        </LedgerBlock>
      </div>
    </>
  );
}
