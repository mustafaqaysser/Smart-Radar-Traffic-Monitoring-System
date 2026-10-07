import type { ReactNode } from 'react';
import type { Metadata, Viewport } from 'next';
import { headers } from 'next/headers';
import { NextIntlClientProvider } from 'next-intl';
import { getLocale, getMessages, getTranslations } from 'next-intl/server';
import restaurantConfig from '@config';
import { adminThemeStylesheet, phases } from '@/lib/brand/palette';
import { adminFontVariables } from '@/lib/fonts-admin';
import { tr } from '@/lib/i18n/localized';
import { adminTheme } from '@/lib/admin/context';
import { Toaster } from '@/components/admin/ui/toast';
import { TooltipProvider } from '@/components/admin/ui/tooltip';
import '@/styles/admin.css';

/** Namespaces the back-office's client components use. */
const CLIENT_NAMESPACES = ['admin', 'forms', 'common'];

export async function generateMetadata(): Promise<Metadata> {
  const locale = await getLocale();
  const t = await getTranslations('admin.shell');
  const name = tr(restaurantConfig.name, locale);
  return {
    title: { default: t('title', { name }), template: `%s · ${t('title', { name })}` },
    robots: { index: false, follow: false, nocache: true },
    referrer: 'same-origin',
  };
}

export const viewport: Viewport = {
  width: 'device-width',
  initialScale: 1,
  themeColor: [
    { media: '(prefers-color-scheme: light)', color: phases.morning.bg },
    { media: '(prefers-color-scheme: dark)', color: phases.night.bg },
  ],
};

/** The back-office has its own document: its own type (IBM Plex), palette (morning/night) and language cookie. */
export default async function AdminRootLayout({ children }: { children: ReactNode }) {
  const [locale, messages, theme, nonce, t] = await Promise.all([getLocale(), getMessages(), adminTheme(), headers().then((h) => h.get('x-nonce') ?? undefined), getTranslations('admin.ui')]);
  const clientMessages = Object.fromEntries(Object.entries(messages).filter(([ns]) => CLIENT_NAMESPACES.includes(ns)));
  return (
    <html lang={locale} dir={locale === 'ar' ? 'rtl' : 'ltr'} data-theme={theme} className={adminFontVariables} suppressHydrationWarning>
      <head>
        <style nonce={nonce} dangerouslySetInnerHTML={{ __html: adminThemeStylesheet() }} />
      </head>
      <body>
        <NextIntlClientProvider messages={clientMessages}>
          <TooltipProvider delayDuration={350}>
            {children}
            <Toaster closeLabel={t('close')} />
          </TooltipProvider>
        </NextIntlClientProvider>
      </body>
    </html>
  );
}
