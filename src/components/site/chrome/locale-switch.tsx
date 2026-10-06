'use client';

import { useLocale, useTranslations } from 'next-intl';
import { useSearchParams } from 'next/navigation';
import { Link, usePathname } from '@/i18n/navigation';
import { routing } from '@/i18n/routing';
import { cn } from '@/lib/utils/cn';

/** Switches language on the same page (keeps the path and query). */
export function LocaleSwitch({ className }: { className?: string }) {
  const locale = useLocale();
  const t = useTranslations('common.nav');
  const pathname = usePathname();
  const search = useSearchParams();
  const other = routing.locales.find((l) => l !== locale) ?? routing.defaultLocale;
  const query = Object.fromEntries(search.entries());
  return (
    <Link
      href={{ pathname, query }}
      locale={other}
      hrefLang={other}
      lang={other}
      className={cn('inline-flex min-h-11 items-center px-2 underline decoration-transparent underline-offset-[0.35em] transition-[text-decoration-color] hover-capable:hover:decoration-ink', other === 'ar' ? 'font-[family-name:var(--font-kufi-ar)] text-[1.0625rem]' : 'font-[family-name:var(--font-plex-mono)] text-[0.75rem] tracking-[0.14em] uppercase', className)}
      aria-label={t('switchToLabel')}
    >
      {t('switchTo')}
    </Link>
  );
}
