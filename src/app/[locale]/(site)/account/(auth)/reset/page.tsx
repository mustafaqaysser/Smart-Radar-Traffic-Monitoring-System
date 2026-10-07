import type { Metadata } from 'next';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import { AuthShell } from '@/components/site/account/auth-shell';
import { ResetRequestForm } from '@/components/site/account/auth-forms';
import { getMediaIndex } from '@/lib/queries/catalog';
import { pageMetadata } from '@/lib/site/metadata';

type Params = Promise<{ locale: string }>;

export async function generateMetadata({ params }: { params: Params }): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: 'account.meta' });
  return pageMetadata({ locale, path: '/account/reset', title: t('reset'), noindex: true });
}

export default async function ResetPage({ params, searchParams }: { params: Params; searchParams: Promise<{ email?: string }> }) {
  const { locale } = await params;
  setRequestLocale(locale);
  const [{ email }, t, media] = await Promise.all([searchParams, getTranslations('account.auth'), getMediaIndex()]);
  return (
    <AuthShell eyebrow={t('eyebrow')} title={t('reset.title')} body={t('reset.body')} image={media['place-door'] ?? null} locale={locale}>
      <ResetRequestForm initialEmail={typeof email === 'string' ? email.slice(0, 254) : ''} />
    </AuthShell>
  );
}
