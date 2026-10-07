import type { Metadata } from 'next';
import { cookies } from 'next/headers';
import { getTranslations } from 'next-intl/server';
import { Download } from 'lucide-react';
import { ADMIN_SOUND_COOKIE, adminPage } from '@/lib/admin/context';
import { boardData, orderHistory } from '@/lib/admin/orders';
import { can } from '@/lib/auth/permissions';
import { addDays, toDateString } from '@/lib/time/zoned';
import { OrdersBoard } from '@/components/admin/orders/orders-board';
import { OrderHistory } from '@/components/admin/orders/order-history';
import { LiveRefresh } from '@/components/admin/shell/live';
import { Button } from '@/components/admin/ui/button';
import { Card } from '@/components/admin/ui/card';
import { LinkTabs } from '@/components/admin/ui/link-tabs';
import { PageHeader } from '@/components/admin/ui/page-header';

export async function generateMetadata(): Promise<Metadata> {
  const t = await getTranslations('admin.orders');
  return { title: t('title') };
}

const DATE = /^\d{4}-\d{2}-\d{2}$/;

export default async function OrdersPage({ searchParams }: { searchParams: Promise<Record<string, string | string[] | undefined>> }) {
  const { user, locale, scope } = await adminPage('orders:view');
  const params = await searchParams;
  const view = params.view === 'history' ? 'history' : 'board';
  const t = await getTranslations('admin.orders');
  const today = toDateString(new Date(), scope.timeZone);
  const from = typeof params.from === 'string' && DATE.test(params.from) ? params.from : addDays(today, -6);
  const to = typeof params.to === 'string' && DATE.test(params.to) ? params.to : today;
  const canExport = can(user.role, 'reports:view');

  return (
    <div className="flex flex-col gap-5">
      <PageHeader
        title={t('title')}
        description={view === 'board' ? t('board.description') : t('history.description')}
        actions={
          view === 'history' && canExport ? (
            <Button asChild>
              <a href={`/api/admin/export/orders?from=${from}&to=${to}`} download>
                <Download aria-hidden="true" />
                {t('history.export')}
              </a>
            </Button>
          ) : null
        }
      />
      <LinkTabs
        label={t('views')}
        param="view"
        items={[
          { href: '/admin/orders', value: 'board', label: t('board.tab') },
          { href: '/admin/orders?view=history', value: 'history', label: t('history.tab') },
        ]}
      />
      {view === 'board' ? (
        <>
          <LiveRefresh topics={['orders']} />
          <OrdersBoard data={await boardData(scope, locale)} canManage={can(user.role, 'orders:manage')} soundPreferred={(await cookies()).get(ADMIN_SOUND_COOKIE)?.value === '1'} showBranch={!scope.branch} timeZone={scope.timeZone} />
        </>
      ) : (
        <Card>
          <OrderHistory rows={await orderHistory(scope, locale, { from, to })} from={from} to={to} showBranch={!scope.branch} />
        </Card>
      )}
    </div>
  );
}
