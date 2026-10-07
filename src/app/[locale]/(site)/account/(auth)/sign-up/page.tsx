import type { Metadata } from 'next';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import restaurantConfig from '@config';
import { AuthShell } from '@/components/site/account/auth-shell';
import { SignUpForm } from '@/components/site/account/auth-forms';
import { redirect } from '@/i18n/navigation';
import { safeNext } from '@/lib/account/next';
import { getCurrentUser } from '@/lib/auth/session';
import { plural } from '@/lib/i18n/plural';
import { getMediaIndex } from '@/lib/queries/catalog';
import { featureEnabled } from '@/lib/server/settings';
import { pageMetadata } from '@/lib/site/metadata';

type Params = Promise<{ locale: string }>;

export async function generateMetadata({ params }: { params: Params }): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: 'account.meta' });
  return pageMetadata({ locale, path: '/account/sign-up', title: t('signUp'), noindex: true });
}

export default async function SignUpPage({ params, searchParams }: { params: Params; searchParams: Promise<{ next?: string }> }) {
  const { locale } = await params;
  setRequestLocale(locale);
  const [search, user, t, tu, media, loyalty] = await Promise.all([searchParams, getCurrentUser(), getTranslations('account.auth'), getTranslations('common.units'), getMediaIndex(), featureEnabled('loyalty')]);
  const next = safeNext(search.next);
  if (user) redirect({ href: next, locale });
  const bonus = restaurantConfig.loyalty.signupBonus;
  return (
    <AuthShell eyebrow={t('eyebrow')} title={t('signUp.title')} body={t('signUp.body')} image={media['place-arch-shadow'] ?? null} locale={locale}>
      <SignUpForm next={next} bonusLabel={loyalty && bonus > 0 ? tu('points', plural(bonus, locale)) : null} />
    </AuthShell>
  );
}
