import type { Metadata } from 'next';
import Link from 'next/link';
import { getTranslations } from 'next-intl/server';
import { getCurrentUser } from '@/lib/auth/session';
import { homeFor, isStaff } from '@/lib/auth/permissions';
import { ShieldAlert } from 'lucide-react';

export async function generateMetadata(): Promise<Metadata> {
  const t = await getTranslations('admin.auth.forbidden');
  return { title: t('title') };
}

/** A signed-in person reached a screen their role does not include. */
export default async function ForbiddenPage() {
  const [user, t] = await Promise.all([getCurrentUser(), getTranslations('admin.auth.forbidden')]);
  const home = user && isStaff(user.role) ? homeFor(user.role) : null;
  return (
    <main className="grid min-h-dvh place-items-center px-4">
      <div className="max-w-sm text-center">
        <ShieldAlert className="mx-auto mb-4 size-8 text-muted" aria-hidden="true" />
        <h1 className="text-xl font-semibold">{t('title')}</h1>
        <p className="mt-2 text-[0.875rem] text-muted">{home ? t('staff') : t('notStaff')}</p>
        <div className="mt-6 flex justify-center gap-3 text-[0.875rem]">
          {home ? (
            <Link href={home} className="text-link underline-offset-4 hover:underline">
              {t('home')}
            </Link>
          ) : (
            <Link href="/admin/sign-in" className="text-link underline-offset-4 hover:underline">
              {t('signIn')}
            </Link>
          )}
        </div>
      </div>
    </main>
  );
}
