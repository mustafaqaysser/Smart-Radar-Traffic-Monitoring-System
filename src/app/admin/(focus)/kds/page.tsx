import type { Metadata } from 'next';
import { cookies } from 'next/headers';
import { getTranslations } from 'next-intl/server';
import { ADMIN_SOUND_COOKIE, adminPage, singleBranch } from '@/lib/admin/context';
import { livePulse } from '@/lib/admin/live';
import { kitchenOrders } from '@/lib/admin/orders';
import { can } from '@/lib/auth/permissions';
import { tr } from '@/lib/i18n/localized';
import { KitchenDisplay } from '@/components/admin/kds/kitchen-display';
import { LiveProvider, LiveRefresh } from '@/components/admin/shell/live';

export async function generateMetadata(): Promise<Metadata> {
  const t = await getTranslations('admin.kds');
  return { title: t('title') };
}

/** Full-screen kitchen display for one house: tickets in the order they were accepted, timers, bump to advance. */
export default async function KitchenDisplayPage({ searchParams }: { searchParams: Promise<{ branch?: string }> }) {
  const { user, locale, scope } = await adminPage('kds:view');
  const { branch: requested } = await searchParams;
  const branch = singleBranch(scope, requested);
  const [data, pulse, store] = await Promise.all([kitchenOrders(branch.id, scope, locale), livePulse(user, scope), cookies()]);
  return (
    <LiveProvider initial={pulse}>
      <LiveRefresh topics={['orders']} />
      <KitchenDisplay
        branch={{ id: branch.id, name: tr(branch.shortName, locale), timeZone: branch.timeZone }}
        branches={scope.branches.map((b) => ({ id: b.id, name: tr(b.shortName, locale) }))}
        cards={data.cards}
        incoming={data.incoming}
        canBump={can(user.role, 'orders:manage')}
        soundPreferred={store.get(ADMIN_SOUND_COOKIE)?.value === '1'}
        exitHref={can(user.role, 'orders:view') ? '/admin/orders' : '/admin'}
      />
    </LiveProvider>
  );
}
