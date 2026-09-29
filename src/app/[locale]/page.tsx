import { getTranslations, setRequestLocale } from 'next-intl/server';

export default async function HomePage({ params }: { params: Promise<{ locale: string }> }) {
  const { locale } = await params;
  setRequestLocale(locale);
  const t = await getTranslations('meta');
  return (
    <main className="site-grid min-h-dvh items-center">
      <h1 className="t-display-xl col-span-full">{t('tagline')}</h1>
    </main>
  );
}
