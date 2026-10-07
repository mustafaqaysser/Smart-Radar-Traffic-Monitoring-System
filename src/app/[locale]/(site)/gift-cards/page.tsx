import type { Metadata } from 'next';
import { notFound } from 'next/navigation';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import restaurantConfig from '@config';
import { BalanceCheck } from '@/components/site/gift-cards/balance-check';
import { GiftCardBuilder } from '@/components/site/gift-cards/gift-card-builder';
import { PageHeader } from '@/components/site/ui/page-header';
import { getCurrentUser } from '@/lib/auth/session';
import { plural } from '@/lib/i18n/plural';
import { featureEnabled } from '@/lib/server/settings';
import { pageMetadata } from '@/lib/site/metadata';
import { toDateString } from '@/lib/time/zoned';

type Params = Promise<{ locale: string }>;

export async function generateMetadata({ params }: { params: Params }): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: 'gather.giftCards.meta' });
  return pageMetadata({ locale, path: '/gift-cards', title: t('title'), description: t('description'), image: 'gift-cards' });
}

export default async function GiftCardsPage({ params, searchParams }: { params: Params; searchParams: Promise<{ code?: string }> }) {
  const { locale } = await params;
  setRequestLocale(locale);
  if (!(await featureEnabled('giftCards'))) notFound();
  const [{ code }, t, tu, user] = await Promise.all([searchParams, getTranslations('gather.giftCards'), getTranslations('common.units'), getCurrentUser()]);
  const g = restaurantConfig.giftCards;
  const timeZone = restaurantConfig.defaultTimeZone;
  return (
    <>
      <PageHeader eyebrow={t('eyebrow')} title={t('title')} intro={<p>{t('intro')}</p>}>
        <p className="t-small text-muted">{t('validity', { months: tu('months', plural(g.validityMonths, locale)) })}</p>
      </PageHeader>
      <GiftCardBuilder designs={[...g.designs]} presets={g.presets.map((p) => p * 100)} min={g.min * 100} max={g.max * 100} timeZone={timeZone} today={toDateString(new Date(), timeZone)} user={user ? { name: user.name, email: user.email } : null} />
      <section id="balance" aria-labelledby="balance-title" className="site-grid scroll-mt-24 pb-[var(--spacing-section)]">
        <div className="col-span-full flex flex-col gap-6 border-t border-ink pt-8 lg:col-span-6">
          <h2 id="balance-title" className="t-heading-lg">
            {t('balance.title')}
          </h2>
          <BalanceCheck initialCode={typeof code === 'string' ? code.slice(0, 40) : ''} timeZone={timeZone} />
        </div>
      </section>
    </>
  );
}
