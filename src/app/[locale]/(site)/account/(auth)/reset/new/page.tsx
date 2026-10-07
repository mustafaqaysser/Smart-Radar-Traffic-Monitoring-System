import type { Metadata } from 'next';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import { AuthShell } from '@/components/site/account/auth-shell';
import { ResetNewForm } from '@/components/site/account/auth-forms';
import { getMediaIndex } from '@/lib/queries/catalog';
import { pageMetadata } from '@/lib/site/metadata';

type Params = Promise<{ locale: string }>;

export async function generateMetadata({ params }: { params: Params }): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: 'account.meta' });
  return pageMetadata({ locale, path: '/account/reset/new', title: t('resetNew'), noindex: true });
}

/** Better Auth sends guests here from the reset email with `?token=` (or `?error=INVALID_TOKEN`). */
export default async function ResetNewPage({ params, searchParams }: { params: Params; searchParams: Promise<{ token?: string; error?: string }> }) {
  const { locale } = await params;
  setRequestLocale(locale);
  const [{ token, error }, t, media] = await Promise.all([searchParams, getTranslations('account.auth'), getMediaIndex()]);
  const valid = typeof token === 'string' && /^[\w-]{8,200}$/.test(token) && !error;
  return (
    <AuthShell eyebrow={t('eyebrow')} title={t('resetNew.title')} body={valid ? t('resetNew.body') : undefined} image={media['place-door'] ?? null} locale={locale}>
      <ResetNewForm token={valid ? token : null} />
    </AuthShell>
  );
}
