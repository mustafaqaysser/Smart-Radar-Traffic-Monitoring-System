import type { Metadata } from 'next';
import { redirect } from 'next/navigation';
import { getLocale, getTranslations } from 'next-intl/server';
import restaurantConfig from '@config';
import { getCurrentUser } from '@/lib/auth/session';
import { homeFor, isStaff } from '@/lib/auth/permissions';
import { tr } from '@/lib/i18n/localized';
import { isDemoMode } from '@/lib/site/url';
import { DEMO_PASSWORD, DEMO_STAFF } from '@/lib/demo';
import { Monogram } from '@/components/brand/logo';
import { StaffSignIn } from '@/components/admin/auth/staff-sign-in';
import { AuthLanguageSwitch } from '@/components/admin/auth/language-switch';

export async function generateMetadata(): Promise<Metadata> {
  const t = await getTranslations('admin.auth.signIn');
  return { title: t('title') };
}


export default async function StaffSignInPage() {
  const user = await getCurrentUser();
  if (user && isStaff(user.role)) redirect(homeFor(user.role));
  const [locale, t] = await Promise.all([getLocale(), getTranslations('admin.auth')]);
  const name = tr(restaurantConfig.name, locale);
  return (
    <main className="grid min-h-dvh place-items-center px-4 py-10">
      <div className="w-full max-w-sm">
        <div className="mb-8 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <Monogram className="h-9 w-auto" decorative />
            <div className="leading-tight">
              <p className="text-base font-semibold">{name}</p>
              <p className="text-xs text-muted">{t('backOffice')}</p>
            </div>
          </div>
          <AuthLanguageSwitch locale={locale} />
        </div>
        <h1 className="text-xl font-semibold">{t('signIn.title')}</h1>
        <p className="mt-1 text-[0.875rem] text-muted">{t('signIn.intro')}</p>
        {/* Demo mode only: one account per role, so reviewers can try every part of the back-office. */}
        <StaffSignIn demoAccounts={isDemoMode() ? DEMO_STAFF.map(({ role, email }) => ({ role, email })) : []} demoPassword={isDemoMode() ? DEMO_PASSWORD : null} />
        {user && !isStaff(user.role) ? <p className="mt-6 border-s-2 border-warning ps-3 text-[0.8125rem] text-muted">{t('signIn.customerSignedIn', { email: user.email })}</p> : null}
        <p className="mt-10 text-center text-xs text-muted">{tr(restaurantConfig.tagline, locale)}</p>
      </div>
    </main>
  );
}
