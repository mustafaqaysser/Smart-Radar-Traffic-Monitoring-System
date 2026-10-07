import type { Metadata } from 'next';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import { Icon, type IconName } from '@/components/brand/icon';
import { Monogram } from '@/components/brand/logo';
import { LocaleSwitch } from '@/components/site/chrome/locale-switch';
import { Link } from '@/i18n/navigation';
import { tr } from '@/lib/i18n/localized';
import { getBranches } from '@/lib/queries/branches';
import { getSettings } from '@/lib/server/settings';
import { googleDirectionsUrl, whatsappUrl } from '@/lib/services/maps';
import { pageMetadata } from '@/lib/site/metadata';

export async function generateMetadata({ params }: { params: Promise<{ locale: string }> }): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: 'pages.links.meta' });
  return pageMetadata({ locale, path: '/links', title: t('title'), description: t('description') });
}

const row = 'flex min-h-16 w-full items-center justify-between gap-4 border border-ink px-6 py-4 transition-colors hover-capable:hover:bg-ink hover-capable:hover:text-bg';

/** A link-in-bio page: the few things people come from social profiles to do. */
export default async function LinksPage({ params }: { params: Promise<{ locale: string }> }) {
  const { locale } = await params;
  setRequestLocale(locale);
  const [t, tc, branches, settings] = await Promise.all([getTranslations('pages.links'), getTranslations('common.a11y'), getBranches(), getSettings()]);
  const f = settings.features;
  const internal: { href: string; label: string; icon: IconName; on: boolean; primary?: boolean }[] = [
    { href: '/reserve', label: t('reserve'), icon: 'calendar', on: f.reservations, primary: true },
    { href: '/menu', label: t('menu'), icon: 'utensils', on: true },
    { href: '/order', label: t('order'), icon: 'bag', on: f.ordering },
    { href: '/experiences', label: t('events'), icon: 'ticket', on: f.events },
    { href: '/gift-cards', label: t('giftCards'), icon: 'gift', on: f.giftCards },
  ];
  return (
    <main id="main" className="mx-auto flex min-h-dvh max-w-md flex-col gap-10 px-4 py-12">
      <header className="flex flex-col items-center gap-5 text-center">
        <Monogram decorative shade className="h-20 w-auto" />
        <h1 className="t-heading-lg">{t('title')}</h1>
        <LocaleSwitch />
      </header>
      <nav aria-label={t('meta.title')}>
        <ul className="flex flex-col gap-3">
          {internal
            .filter((l) => l.on)
            .map((l) => (
              <li key={l.href}>
                <Link href={l.href} className={l.primary ? `${row} border-accent bg-accent text-on-accent` : row}>
                  <span className="t-heading-sm">{l.label}</span>
                  <Icon name={l.icon} size={22} />
                </Link>
              </li>
            ))}
          {branches.map((b) => (
            <li key={`dir-${b.id}`}>
              <a href={googleDirectionsUrl({ lat: b.lat, lng: b.lng })} target="_blank" rel="noopener noreferrer" className={row}>
                <span className="t-heading-sm">
                  {t('directions', { house: tr(b.shortName, locale) })}
                  <span className="sr-only"> ({tc('newWindow')})</span>
                </span>
                <Icon name="pin" size={22} />
              </a>
            </li>
          ))}
          {branches
            .filter((b) => b.whatsapp)
            .map((b) => (
              <li key={`wa-${b.id}`}>
                <a href={whatsappUrl(b.whatsapp as string)} target="_blank" rel="noopener noreferrer" className={row}>
                  <span className="t-heading-sm">
                    {t('whatsapp', { house: tr(b.shortName, locale) })}
                    <span className="sr-only"> ({tc('newWindow')})</span>
                  </span>
                  <Icon name="chat" size={22} />
                </a>
              </li>
            ))}
          {f.newsletter ? (
            <li>
              <Link href="/newsletter" className={row}>
                <span className="t-heading-sm">{t('newsletter')}</span>
                <Icon name="mail" size={22} />
              </Link>
            </li>
          ) : null}
        </ul>
      </nav>
      <Link href="/" className="t-small self-center underline decoration-line underline-offset-4">
        {t('site')}
      </Link>
    </main>
  );
}
