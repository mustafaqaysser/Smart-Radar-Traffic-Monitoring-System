import type { Metadata } from 'next';
import { notFound } from 'next/navigation';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import { ReserveFlow } from '@/components/site/reserve/reserve-flow';
import { ButtonAnchor } from '@/components/site/ui/button';
import { PageHeader } from '@/components/site/ui/page-header';
import { getCurrentUser } from '@/lib/auth/session';
import { normalizeDigits } from '@/lib/i18n/digits';
import { formatNumber } from '@/lib/i18n/format';
import { tr } from '@/lib/i18n/localized';
import { getBranches } from '@/lib/queries/branches';
import { reserveSetup } from '@/lib/server/reserve-setup';
import { featureEnabled } from '@/lib/server/settings';
import { telUrl } from '@/lib/services/maps';
import { pageMetadata } from '@/lib/site/metadata';
import { getSelectedBranch } from '@/lib/site/selection';

type Params = Promise<{ locale: string }>;
type Search = Promise<Record<string, string | string[] | undefined>>;

export async function generateMetadata({ params }: { params: Params }): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: 'reserve.meta' });
  return pageMetadata({ locale, path: '/reserve', title: t('title'), description: t('description'), image: 'reserve' });
}

const one = (v: string | string[] | undefined): string | null => (Array.isArray(v) ? (v[0] ?? null) : (v ?? null));

export default async function ReservePage({ params, searchParams }: { params: Params; searchParams: Search }) {
  const { locale } = await params;
  setRequestLocale(locale);
  if (!(await featureEnabled('reservations'))) notFound();
  const sp = await searchParams;
  const [t, setup, user, selected, newsletter] = await Promise.all([getTranslations('reserve'), reserveSetup(locale), getCurrentUser(), getSelectedBranch(), featureEnabled('newsletter')]);
  const partyParam = one(sp.party);
  const party = partyParam ? Number.parseInt(normalizeDigits(partyParam), 10) : null;

  if (!setup.branches.length) {
    const branches = await getBranches();
    return (
      <PageHeader eyebrow={t('eyebrow')} title={t('title')} intro={<p>{t('paused')}</p>}>
        <div className="flex flex-wrap gap-3">
          {branches.map((b) => (
            <ButtonAnchor key={b.id} href={telUrl(b.phone)} icon="phone" size="sm">
              {tr(b.shortName, locale)}
            </ButtonAnchor>
          ))}
        </div>
      </PageHeader>
    );
  }

  return (
    <>
      <PageHeader eyebrow={t('eyebrow')} title={t('title')} intro={<p>{t('intro', { n: formatNumber(setup.bookingWindowDays, locale) })}</p>} />
      <ReserveFlow
        setup={setup}
        guest={user ? { name: user.name, email: user.email, phone: user.phone ?? '' } : null}
        signedIn={Boolean(user)}
        newsletter={newsletter}
        initial={{ branch: one(sp.branch) ?? selected?.slug ?? null, date: one(sp.date), party: Number.isFinite(party) ? party : null }}
        layout="page"
      />
    </>
  );
}
