import type { Metadata } from 'next';
import { notFound } from 'next/navigation';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import restaurantConfig from '@config';
import { AccountHeader, LedgerBlock } from '@/components/site/account/account-header';
import { CopyCode } from '@/components/site/account/copy-code';
import { ButtonLink } from '@/components/site/ui/button';
import { Link } from '@/i18n/navigation';
import { getCurrentUser } from '@/lib/auth/session';
import { formatDate, formatMoney } from '@/lib/i18n/format';
import { accountGiftCards } from '@/lib/server/account';
import { featureEnabled } from '@/lib/server/settings';
import { pageMetadata } from '@/lib/site/metadata';
import { cn } from '@/lib/utils/cn';

type Params = Promise<{ locale: string }>;

export async function generateMetadata({ params }: { params: Params }): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: 'account.meta' });
  return pageMetadata({ locale, path: '/account/gift-cards', title: t('giftCards'), noindex: true });
}

const PHASE: Record<string, string> = { dawn: 'dawn', noon: 'noon', 'long-shade': 'afternoon', night: 'night' };

export default async function GiftCardsAccountPage({ params }: { params: Params }) {
  const { locale } = await params;
  setRequestLocale(locale);
  if (!(await featureEnabled('giftCards'))) notFound();
  const user = await getCurrentUser();
  if (!user) return null;
  const [t, { given, received }] = await Promise.all([getTranslations('account'), accountGiftCards(user)]);
  const tz = restaurantConfig.defaultTimeZone;
  const date = (d: Date) => formatDate(d, locale, tz, { day: 'numeric', month: 'long', year: 'numeric' });
  return (
    <>
      <AccountHeader title={t('giftCards.title')} intro={t('giftCards.intro')}>
        <ButtonLink href="/gift-cards" variant="secondary" icon={null} leadingIcon="gift" className="self-start">
          {t('giftCards.buy')}
        </ButtonLink>
      </AccountHeader>
      <LedgerBlock title={t('giftCards.received')}>
        {!user.emailVerified ? (
          <p className="t-body text-muted">
            {t('giftCards.verify')}{' '}
            <Link href="/account" className="underline underline-offset-4">
              {t('nav.overview')}
            </Link>
          </p>
        ) : received.length ? (
          <ul className="grid gap-[var(--spacing-gutter)] md:grid-cols-2">
            {received.map((c) => (
              <li key={c.id} className="flex flex-col gap-4">
                <div data-phase={PHASE[c.design] ?? 'noon'} className="relative aspect-[1000/630] overflow-hidden bg-bg text-ink cast-shade">
                  {/* eslint-disable-next-line @next/next/no-img-element -- generated SVG artwork, served by our own route */}
                  <img src={`/api/gift-cards/art/${c.design}`} alt="" width={1000} height={630} className="absolute inset-0 h-full w-full" />
                  <p className="t-heading-md absolute end-[6%] top-[7%] tabular">
                    <bdi>{formatMoney(c.balance, locale)}</bdi>
                  </p>
                </div>
                <p className="t-small text-muted">{t('giftCards.from', { name: c.purchaserName })}</p>
                <p className="t-body">{t('giftCards.balance', { amount: formatMoney(c.balance, locale), initial: formatMoney(c.initialAmount, locale) })}</p>
                <p className={cn('t-label', c.status === 'active' ? 'text-accent' : 'text-muted')}>
                  {t(`status.giftCard.${c.status}`)}
                  {c.expiresAt && c.status === 'active' ? ` · ${t('giftCards.until', { date: date(c.expiresAt) })}` : ''}
                </p>
                {c.status === 'active' ? (
                  <div className="flex flex-col gap-2 border-t border-line pt-3">
                    <p className="t-label text-muted">{t('giftCards.code')}</p>
                    <CopyCode code={c.code} />
                    <p className="t-small text-muted">{t('giftCards.use')}</p>
                  </div>
                ) : null}
              </li>
            ))}
          </ul>
        ) : (
          <p className="t-body text-muted">{t('giftCards.noneReceived')}</p>
        )}
      </LedgerBlock>
      <LedgerBlock title={t('giftCards.given')}>
        {given.length ? (
          <ul className="-mt-5">
            {given.map((c) => (
              <li key={c.id} className="grid gap-2 border-b border-line py-5 md:grid-cols-12 md:items-baseline md:gap-6">
                <p className="t-heading-sm md:col-span-6">{t('giftCards.to', { name: c.recipientName })}</p>
                <p className="t-body tabular md:col-span-3">
                  <bdi>{formatMoney(c.initialAmount, locale)}</bdi>
                </p>
                <p className="t-label text-muted md:col-span-3 md:text-end">
                  {t(`status.giftCard.${c.status}`)}
                  {c.status === 'scheduled' && c.deliverAt ? ` · ${date(c.deliverAt)}` : ''}
                </p>
              </li>
            ))}
          </ul>
        ) : (
          <p className="t-body text-muted">{t('giftCards.noneGiven')}</p>
        )}
      </LedgerBlock>
    </>
  );
}
