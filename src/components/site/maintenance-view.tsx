import { getTranslations } from 'next-intl/server';
import { Monogram } from '@/components/brand/logo';
import { tr } from '@/lib/i18n/localized';
import type { BranchDTO } from '@/lib/queries/types';
import { telUrl } from '@/lib/services/maps';

/** Shown to visitors while maintenance mode is on (staff keep browsing the live site). */
export async function MaintenanceView({ message, branches, locale }: { message: string; branches: BranchDTO[]; locale: string }) {
  const t = await getTranslations('common');
  return (
    <main id="main" className="site-grid min-h-dvh content-center gap-y-10 py-16">
      <Monogram decorative shade className="col-span-full h-24 w-auto justify-self-start" />
      <div className="col-span-full lg:col-span-8">
        <h1 className="t-display-md">{t('errors.maintenanceTitle')}</h1>
        <p className="t-body-lg measure mt-6">{message}</p>
      </div>
      <ul className="col-span-full grid gap-8 md:grid-cols-2 lg:col-span-8">
        {branches.map((b) => (
          <li key={b.id} className="border-t border-line pt-4">
            <p className="t-heading-sm">{tr(b.name, locale)}</p>
            <p className="t-small text-muted">{tr(b.address, locale)}</p>
            <a href={telUrl(b.phone)} dir="ltr" className="t-small tabular mt-2 inline-block underline decoration-line underline-offset-4">
              {b.phone}
            </a>
          </li>
        ))}
      </ul>
    </main>
  );
}
