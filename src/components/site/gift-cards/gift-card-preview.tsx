'use client';

import { useLocale, useTranslations } from 'next-intl';
import { formatMoney } from '@/lib/i18n/format';
import { cn } from '@/lib/utils/cn';

/** Which palette each card design is drawn in (so the overlaid text takes that hour's ink). */
const PHASE: Record<string, string> = { dawn: 'dawn', noon: 'noon', 'long-shade': 'afternoon', night: 'night' };

/** The card as it will arrive: the design's art with the amount, names and message laid over it. */
export function GiftCardPreview({ design, amount, to, from, message, label, className }: { design: string; amount: number | null; to: string; from: string; message?: string; label: string; className?: string }) {
  const t = useTranslations('gather.giftCards');
  const locale = useLocale();
  return (
    <figure className={cn('flex flex-col gap-4', className)}>
      <div data-phase={PHASE[design] ?? 'noon'} className="relative aspect-[1000/630] w-full overflow-hidden bg-bg text-ink cast-shade">
        {/* eslint-disable-next-line @next/next/no-img-element -- generated SVG artwork, served by our own route */}
        <img src={`/api/gift-cards/art/${design}`} alt={label} width={1000} height={630} className="absolute inset-0 h-full w-full" />
        <div className="absolute inset-x-[6%] top-[7%] flex items-start justify-between gap-4">
          <p className="t-small max-w-[60%] truncate">{to.trim() ? t('toName', { name: to.trim() }) : ' '}</p>
          <p className="t-heading-md tabular">{amount !== null ? <bdi>{formatMoney(amount, locale)}</bdi> : null}</p>
        </div>
        <p className="t-small absolute end-[6%] bottom-[7%] max-w-[50%] truncate text-end">{from.trim() ? t('fromName', { name: from.trim() }) : null}</p>
      </div>
      {message?.trim() ? <blockquote className="t-body measure border-s-2 border-sun ps-4 italic rtl:not-italic">{message.trim()}</blockquote> : null}
    </figure>
  );
}
