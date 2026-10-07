import type { Metadata } from 'next';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import { AuthShell } from '@/components/site/account/auth-shell';
import { SignInForm } from '@/components/site/account/auth-forms';
import { redirect } from '@/i18n/navigation';
import { safeNext } from '@/lib/account/next';
import { getCurrentUser } from '@/lib/auth/session';
import { getMediaIndex } from '@/lib/queries/catalog';
import { pageMetadata } from '@/lib/site/metadata';

type Params = Promise<{ locale: string }>;
type Search = Promise<{ next?: string; email?: string; mode?: string }>;

export async function generateMetadata({ params }: { params: Params }): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: 'account.meta' });
  return pageMetadata({ locale, path: '/account/sign-in', title: t('signIn'), noindex: true });
}

export default async function SignInPage({ params, searchParams }: { params: Params; searchParams: Search }) {
  const { locale } = await params;
  setRequestLocale(locale);
  const [search, user, t, media] = await Promise.all([searchParams, getCurrentUser(), getTranslations('account.auth'), getMediaIndex()]);
  const next = safeNext(search.next);
  if (user) redirect({ href: next, locale });
  const email = typeof search.email === 'string' ? search.email.slice(0, 254) : '';
  return (
    <AuthShell eyebrow={t('eyebrow')} title={t('signIn.title')} body={t('signIn.body')} image={media['place-door'] ?? null} locale={locale}>
      <SignInForm next={next} initialMode={search.mode === 'code' ? 'code' : 'password'} initialEmail={email} />
    </AuthShell>
  );
}
