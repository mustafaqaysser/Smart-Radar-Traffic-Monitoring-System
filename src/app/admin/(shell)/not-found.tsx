import Link from 'next/link';
import { getTranslations } from 'next-intl/server';
import { Compass } from 'lucide-react';

export default async function AdminNotFound() {
  const t = await getTranslations('admin.auth.notFound');
  return (
    <div className="grid min-h-[60vh] place-items-center">
      <div className="max-w-sm text-center">
        <Compass className="mx-auto mb-4 size-8 text-muted" aria-hidden="true" />
        <h1 className="text-xl font-semibold">{t('title')}</h1>
        <p className="mt-2 text-[0.875rem] text-muted">{t('body')}</p>
        <Link href="/admin" className="mt-6 inline-block text-[0.875rem] text-link underline-offset-4 hover:underline">
          {t('home')}
        </Link>
      </div>
    </div>
  );
}
