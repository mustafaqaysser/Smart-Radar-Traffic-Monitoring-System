import type { Metadata } from 'next';
import Image from 'next/image';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import { AccountHeader } from '@/components/site/account/account-header';
import { FavouriteRemove } from '@/components/site/account/favourite-remove';
import { ButtonLink } from '@/components/site/ui/button';
import { Link } from '@/i18n/navigation';
import { getCurrentUser } from '@/lib/auth/session';
import { formatMoney } from '@/lib/i18n/format';
import { itemView } from '@/lib/menu/view';
import { getMenuCatalog } from '@/lib/queries/catalog';
import { favouriteItemIds } from '@/lib/server/account';
import { pageMetadata } from '@/lib/site/metadata';
import { getSelectedBranch } from '@/lib/site/selection';

type Params = Promise<{ locale: string }>;

export async function generateMetadata({ params }: { params: Params }): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: 'account.meta' });
  return pageMetadata({ locale, path: '/account/favourites', title: t('favourites'), noindex: true });
}

export default async function FavouritesPage({ params }: { params: Params }) {
  const { locale } = await params;
  setRequestLocale(locale);
  const user = await getCurrentUser();
  if (!user) return null;
  const [t, tm, ids, catalog, branch] = await Promise.all([getTranslations('account.favourites'), getTranslations('menu'), favouriteItemIds(user.id), getMenuCatalog(), getSelectedBranch()]);
  const byId = new Map(Object.values(catalog.items).map((i) => [i.id, i]));
  const items = ids.flatMap((id) => {
    const item = byId.get(id);
    return item && item.isActive ? [itemView(item, catalog, branch?.id ?? null, locale)] : [];
  });
  return (
    <>
      <AccountHeader title={t('title')} intro={t('intro')} />
      {items.length ? (
        <ul className="grid gap-x-[var(--spacing-gutter)] gap-y-10 sm:grid-cols-2 xl:grid-cols-3">
          {items.map((item) => (
            <li key={item.slug} data-card className="relative flex flex-col gap-4">
              <div className="arch-3x4 relative aspect-[3/4] overflow-hidden bg-surface">
                {item.image ? <Image src={item.image.src} alt="" fill sizes="(min-width: 1280px) 20vw, (min-width: 640px) 33vw, 90vw" quality={55} className="object-cover" style={{ objectPosition: `${item.image.focalX * 100}% ${item.image.focalY * 100}%` }} /> : null}
              </div>
              <div className="flex items-start justify-between gap-4">
                <div className="flex flex-col gap-1">
                  <h2 className="t-heading-sm">
                    <Link href={`/menu/dish/${item.slug}`} className="card-link">
                      {item.name}
                    </Link>
                  </h2>
                  <p className="t-small tabular text-muted">
                    <bdi>{formatMoney(item.price, locale)}</bdi>
                    {item.soldOut ? <span className="text-danger"> · {tm('item.soldOut')}</span> : null}
                  </p>
                </div>
                <FavouriteRemove slug={item.slug} name={item.name} />
              </div>
            </li>
          ))}
        </ul>
      ) : (
        <div className="flex flex-col items-start gap-5 border-t border-ink pt-6">
          <p className="t-body text-muted">{t('none')}</p>
          <ButtonLink href="/menu">{t('browse')}</ButtonLink>
        </div>
      )}
    </>
  );
}
