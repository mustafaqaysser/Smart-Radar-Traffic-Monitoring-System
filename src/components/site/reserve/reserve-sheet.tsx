'use client';

import { useLocale, useTranslations } from 'next-intl';
import { useEffect, useState } from 'react';
import { ButtonLink } from '@/components/site/ui/button';
import { Dialog } from '@/components/site/ui/dialog';
import type { FlowSetup, GuestPrefill } from '@/lib/reserve/types';
import { ReserveFlow } from './reserve-flow';

interface SheetData {
  setup: FlowSetup;
  guest: GuestPrefill | null;
  newsletter: boolean;
  selected: string | null;
}

/** The booking flow in a bottom sheet, opened from the mobile dock without leaving the page. */
export default function ReserveSheet({ onClose }: { onClose: () => void }) {
  const t = useTranslations('reserve');
  const tc = useTranslations('common');
  const locale = useLocale();
  const [data, setData] = useState<SheetData | 'error' | null>(null);

  useEffect(() => {
    const ctrl = new AbortController();
    fetch(`/api/reservations/setup?locale=${locale}`, { signal: ctrl.signal, cache: 'no-store' })
      .then((res) => (res.ok ? (res.json() as Promise<SheetData>) : Promise.reject(new Error(String(res.status)))))
      .then((json) => setData(json.setup.branches.length ? json : 'error'))
      .catch(() => {
        if (!ctrl.signal.aborted) setData('error');
      });
    return () => ctrl.abort();
  }, [locale]);

  return (
    <Dialog open onClose={onClose} title={t('title')} closeLabel={tc('a11y.close')} variant="sheet">
      {data === null ? (
        <div className="flex flex-col gap-4" aria-busy="true">
          <p role="status" className="t-small text-muted">
            {t('sheet.loading')}
          </p>
          {Array.from({ length: 4 }, (_, i) => (
            <span key={i} aria-hidden="true" className="block h-14 animate-pulse bg-surface" />
          ))}
        </div>
      ) : data === 'error' ? (
        <div className="flex flex-col items-start gap-5">
          <p className="t-body">{t('sheet.error')}</p>
          <ButtonLink href="/reserve" onClick={onClose}>
            {t('sheet.full')}
          </ButtonLink>
        </div>
      ) : (
        <ReserveFlow setup={data.setup} guest={data.guest} signedIn={Boolean(data.guest)} newsletter={data.newsletter} initial={{ branch: data.selected }} layout="sheet" onLeave={onClose} />
      )}
    </Dialog>
  );
}
