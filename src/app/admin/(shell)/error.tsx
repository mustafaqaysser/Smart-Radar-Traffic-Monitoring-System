'use client';

import { useTranslations } from 'next-intl';
import { TriangleAlert } from 'lucide-react';
import { Button } from '@/components/admin/ui/button';

/** A screen failed to render: say so plainly and offer a retry (the rest of the back-office keeps working). */
export default function AdminError({ reset, error }: { error: Error & { digest?: string }; reset: () => void }) {
  const t = useTranslations('admin.auth.error');
  return (
    <div className="grid min-h-[60vh] place-items-center">
      <div className="max-w-sm text-center">
        <TriangleAlert className="mx-auto mb-4 size-8 text-danger" aria-hidden="true" />
        <h1 className="text-xl font-semibold">{t('title')}</h1>
        <p className="mt-2 text-[0.875rem] text-muted">{t('body')}</p>
        {error.digest ? (
          <p className="mt-2 text-xs text-muted">
            {t('reference')}{' '}
            <code dir="ltr" className="font-mono">
              {error.digest}
            </code>
          </p>
        ) : null}
        <Button variant="primary" className="mt-6" onClick={() => reset()}>
          {t('retry')}
        </Button>
      </div>
    </div>
  );
}
