import type { Metadata } from 'next';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import { AccountHeader, LedgerBlock } from '@/components/site/account/account-header';
import { DeleteAccount } from '@/components/site/account/delete-account';
import { buttonClasses } from '@/components/site/ui/button';
import { Icon } from '@/components/brand/icon';
import { getCurrentUser } from '@/lib/auth/session';
import { formatDate } from '@/lib/i18n/format';
import { plural } from '@/lib/i18n/plural';
import { getBranches } from '@/lib/queries/branches';
import { deletionCheck } from '@/lib/server/account';
import { pageMetadata } from '@/lib/site/metadata';

type Params = Promise<{ locale: string }>;

export async function generateMetadata({ params }: { params: Params }): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: 'account.meta' });
  return pageMetadata({ locale, path: '/account/privacy', title: t('privacy'), noindex: true });
}

export default async function PrivacyPage({ params }: { params: Params }) {
  const { locale } = await params;
  setRequestLocale(locale);
  const user = await getCurrentUser();
  if (!user) return null;
  const [t, check, branches] = await Promise.all([getTranslations('account.privacy'), deletionCheck(user, new Date()), getBranches()]);
  const tz = branches[0]?.timeZone ?? 'Asia/Riyadh';
  const day = (d: Date) => formatDate(d, locale, tz, { weekday: 'long', day: 'numeric', month: 'long' });
  return (
    <>
      <AccountHeader title={t('title')} intro={t('intro')} />
      <LedgerBlock title={t('export.title')}>
        <div className="flex flex-col items-start gap-5">
          <p className="t-body measure">{t('export.body')}</p>
          <a href="/api/account/export" download className={buttonClasses('secondary', 'md')}>
            <Icon name="download" size={18} />
            <span>{t('export.cta')}</span>
          </a>
        </div>
      </LedgerBlock>
      <LedgerBlock title={t('delete.title')}>
        <div className="flex flex-col gap-5">
          <p className="t-body measure">{t('delete.body')}</p>
          {check.blockers.length ? (
            <div className="flex flex-col gap-2 border-s-2 border-danger ps-4">
              <p className="t-body">{t('delete.blocked')}</p>
              <ul className="flex flex-col gap-1">
                {check.blockers.map((b) => (
                  <li key={`${b.kind}-${'number' in b ? b.number : b.code}`} className="t-small">
                    {b.kind === 'order' ? t('delete.blockOrder', { number: b.number }) : b.kind === 'reservation' ? t('delete.blockReservation', { code: b.code, date: day(b.startsAt) }) : t('delete.blockTicket', { code: b.code, date: day(b.startsAt) })}
                  </li>
                ))}
              </ul>
            </div>
          ) : check.toCancel.length ? (
            <p className="t-body">{t('delete.cancels', plural(check.toCancel.length, locale))}</p>
          ) : null}
          <DeleteAccount email={user.email} blocked={check.blockers.length > 0} cancels={check.toCancel.length ? t('delete.cancels', plural(check.toCancel.length, locale)) : null} />
        </div>
      </LedgerBlock>
    </>
  );
}
