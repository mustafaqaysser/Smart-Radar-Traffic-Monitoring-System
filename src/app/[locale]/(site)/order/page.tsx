import type { Metadata } from 'next';
import { notFound } from 'next/navigation';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import { Icon } from '@/components/brand/icon';
import { BranchSwitch } from '@/components/site/chrome/branch-switch';
import { ChannelToggle } from '@/components/site/order/channel-toggle';
import { OrderMenu } from '@/components/site/order/order-menu';
import { ButtonAnchor } from '@/components/site/ui/button';
import { PageHeader } from '@/components/site/ui/page-header';
import { formatClock, formatDate } from '@/lib/i18n/format';
import { tr } from '@/lib/i18n/localized';
import { plural } from '@/lib/i18n/plural';
import { checkoutBranch, orderMenuData } from '@/lib/order/menu-data';
import { getCurrentUser } from '@/lib/auth/session';
import { toDietProfile } from '@/lib/menu/filter';
import { getBranches } from '@/lib/queries/branches';
import { kitchenStatus } from '@/lib/server/order-quote';
import { getSettings } from '@/lib/server/settings';
import { telUrl } from '@/lib/services/maps';
import { pageMetadata } from '@/lib/site/metadata';
import { getSelectedBranch } from '@/lib/site/selection';
import { addDays, toDateString } from '@/lib/time/zoned';
import { cn } from '@/lib/utils/cn';

type Params = Promise<{ locale: string }>;

export async function generateMetadata({ params }: { params: Params }): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: 'order.meta' });
  return pageMetadata({ locale, path: '/order', title: t('title'), description: t('description'), image: 'order' });
}

export default async function OrderPage({ params }: { params: Params }) {
  const { locale } = await params;
  setRequestLocale(locale);
  const settings = await getSettings();
  if (!settings.features.ordering) notFound();
  const [t, tb, tu, branches, selected, user] = await Promise.all([getTranslations('order'), getTranslations('common.branch'), getTranslations('common.units'), getBranches(), getSelectedBranch(), getCurrentUser()]);
  const branch = selected ?? branches[0];
  if (!branch) notFound();
  const now = new Date();
  const [data, kitchen, info] = await Promise.all([orderMenuData(branch, locale), kitchenStatus(branch, now), checkoutBranch(branch, locale)]);
  const name = tr(branch.shortName, locale);
  const canOrder = !kitchen.paused && (info.channels.delivery || info.channels.pickup);

  let kitchenLine: string;
  if (kitchen.paused) kitchenLine = t('kitchen.paused', { house: name });
  else if (kitchen.open && kitchen.readyInMinutes !== null) kitchenLine = t('kitchen.open', { duration: tu('mins', plural(kitchen.readyInMinutes, locale)) });
  else if (kitchen.nextOpen) {
    const at = new Date(kitchen.nextOpen);
    const today = toDateString(now, branch.timeZone);
    const day = toDateString(at, branch.timeZone);
    const time = formatClock(at, locale, branch.timeZone);
    const when = day === today ? tb('opensToday', { time }) : day === addDays(today, 1) ? tb('opensTomorrow', { time }) : tb('opensOn', { day: formatDate(at, locale, branch.timeZone, { weekday: 'long' }), time });
    kitchenLine = `${t('kitchen.closed')} ${t('kitchen.opens', { when })}`;
  } else kitchenLine = t('kitchen.closed');

  return (
    <>
      <PageHeader eyebrow={t('eyebrow')} title={t('title')} intro={<p>{t('intro')}</p>}>
        <div className="flex flex-col gap-6 sm:flex-row sm:flex-wrap sm:items-end sm:gap-10">
          <BranchSwitch branches={branches.map((b) => ({ slug: b.slug, name: tr(b.shortName, locale), city: tr(b.city, locale) }))} selected={branch.slug} />
          {canOrder ? <ChannelToggle channels={info.channels} /> : null}
        </div>
        <p role="status" className={cn('t-body flex items-start gap-3', kitchen.paused && 'text-danger')}>
          <span aria-hidden="true" className={cn('mt-[0.55em] size-2.5 shrink-0 rounded-full', kitchen.open && !kitchen.paused ? 'bg-success' : kitchen.paused ? 'bg-danger' : 'bg-muted')} />
          <span>
            {kitchenLine}
            {kitchen.busy && !kitchen.paused ? <span className="text-muted"> {t('kitchen.busy')}</span> : null}
          </span>
        </p>
        {kitchen.paused ? (
          <ButtonAnchor href={telUrl(branch.phone)} icon="phone" size="sm" className="self-start">
            {branch.phone}
          </ButtonAnchor>
        ) : null}
      </PageHeader>
      {data.menus.length ? (
        <OrderMenu menus={data.menus} items={data.items} branch={{ slug: branch.slug, name }} houses={Object.fromEntries(branches.map((b) => [b.slug, tr(b.shortName, locale)]))} canOrder={canOrder} profile={toDietProfile(user?.dietary)} />
      ) : (
        <div className="site-grid pb-[var(--spacing-section)]">
          <p className="t-body-lg col-span-full flex items-center gap-3">
            <Icon name="clock" size={22} />
            {t('menu.empty', { house: name })}
          </p>
        </div>
      )}
    </>
  );
}
