'use client';

import { useTranslations } from 'next-intl';
import { Icon } from '@/components/brand/icon';
import { toast } from '@/components/site/ui/toast';

/** A gift card code with a one-tap copy (codes are read left to right in both languages). */
export function CopyCode({ code }: { code: string }) {
  const t = useTranslations('account.giftCards');
  return (
    <span className="inline-flex items-center gap-3">
      <code dir="ltr" className="t-instrument tabular">
        {code}
      </code>
      <button
        type="button"
        aria-label={t('copy')}
        className="inline-flex size-11 items-center justify-center rounded-full border border-line hover-capable:hover:border-ink"
        onClick={async () => {
          try {
            await navigator.clipboard.writeText(code);
            toast(t('copied'));
          } catch {
            toast(code);
          }
        }}
      >
        <Icon name="receipt" size={16} />
      </button>
    </span>
  );
}
