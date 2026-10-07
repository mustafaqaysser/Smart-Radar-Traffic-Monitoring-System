'use client';

import { useTranslations } from 'next-intl';
import { ErrorView } from '@/components/site/error-view';
import { Button, ButtonLink } from '@/components/site/ui/button';

export default function SiteError({ reset }: { error: Error & { digest?: string }; reset: () => void }) {
  const t = useTranslations('common');
  return (
    <ErrorView code="500" title={t('errors.serverTitle')} body={t('errors.serverBody')}>
      <Button onClick={reset} icon="refresh">
        {t('actions.retry')}
      </Button>
      <ButtonLink href="/" variant="secondary">
        {t('actions.goHome')}
      </ButtonLink>
    </ErrorView>
  );
}
