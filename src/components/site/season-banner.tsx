'use client';

import { useTranslations } from 'next-intl';
import { useState } from 'react';
import { Icon } from '@/components/brand/icon';

const KEY = 'zill_dismissed';

/** A slim seasonal notice under the header. Dismissal is remembered per season (applied before paint). */
export function SeasonBanner({ slug, name, text }: { slug: string; name: string; text: string }) {
  const t = useTranslations('common.seasonal');
  const [dismissed, setDismissed] = useState(false);
  if (dismissed) return null;
  return (
    <aside className="season-banner border-b border-line bg-surface" data-slug={slug} aria-label={name}>
      <div className="site-wrap flex items-start gap-4 py-3">
        <p className="t-small flex-1">
          <strong className="font-semibold">{name}</strong>
          <span aria-hidden="true"> · </span>
          {text}
        </p>
        <button
          type="button"
          className="-my-2 -me-2 inline-flex size-11 shrink-0 items-center justify-center"
          aria-label={t('dismiss')}
          onClick={() => {
            setDismissed(true);
            try {
              const list = new Set((window.localStorage.getItem(KEY) ?? '').split(' ').filter(Boolean));
              list.add(slug);
              window.localStorage.setItem(KEY, [...list].join(' '));
            } catch {
              // Not persisted; hidden for this page view.
            }
          }}
        >
          <Icon name="close" size={18} />
        </button>
      </div>
    </aside>
  );
}
