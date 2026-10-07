'use client';

import { useTranslations } from 'next-intl';
import { ErrorView } from '@/components/site/error-view';
import { Button } from '@/components/site/ui/button';

export default function LocaleError({ reset }: { error: Error & { digest?: string }; reset: () => void }) {
  const t = useTranslations('common');
  return (
    <main id="main">
      <ErrorView code="500" title={t('errors.serverTitle')} body={t('errors.serverBody')}>
        <Button onClick={reset} icon="refresh">
          {t('actions.retry')}
        </Button>
      </ErrorView>
    </main>
  );
}
