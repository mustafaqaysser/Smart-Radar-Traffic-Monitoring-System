import { formatMoney } from '@/lib/i18n/format';
import { cn } from '@/lib/utils/cn';

/** Money is stored in halalas and formatted only here, at the edge. */
export function Price({ amount, locale, className, strike }: { amount: number; locale: string; className?: string; strike?: boolean }) {
  const text = formatMoney(amount, locale);
  return strike ? (
    <s className={cn('tabular text-muted', className)}>
      <bdi>{text}</bdi>
    </s>
  ) : (
    <span className={cn('tabular', className)}>
      <bdi>{text}</bdi>
    </span>
  );
}
