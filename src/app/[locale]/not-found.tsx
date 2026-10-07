import { getTranslations } from 'next-intl/server';
import { ErrorView } from '@/components/site/error-view';
import { ButtonLink } from '@/components/site/ui/button';

export default async function NotFound() {
  const t = await getTranslations('common');
  return (
    <main id="main">
      <ErrorView code="404" title={t('errors.notFoundTitle')} body={t('errors.notFoundBody')}>
        <ButtonLink href="/">{t('actions.goHome')}</ButtonLink>
      </ErrorView>
    </main>
  );
}
