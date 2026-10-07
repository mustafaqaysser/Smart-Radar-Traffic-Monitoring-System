import type { Metadata } from 'next';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import { ContactForm } from '@/components/site/contact-form';
import { PageHeader } from '@/components/site/ui/page-header';
import { tr } from '@/lib/i18n/localized';
import { getBranches } from '@/lib/queries/branches';
import { getSettings } from '@/lib/server/settings';
import { telUrl } from '@/lib/services/maps';
import { pageMetadata } from '@/lib/site/metadata';

export async function generateMetadata({ params }: { params: Promise<{ locale: string }> }): Promise<Metadata> {
  const { locale } = await params;
  const t = await getTranslations({ locale, namespace: 'visit.contact.meta' });
  return pageMetadata({ locale, path: '/contact', title: t('title'), description: t('description'), image: 'contact' });
}

export default async function ContactPage({ params, searchParams }: { params: Promise<{ locale: string }>; searchParams: Promise<{ subject?: string }> }) {
  const { locale } = await params;
  const { subject } = await searchParams;
  setRequestLocale(locale);
  const [t, branches, settings] = await Promise.all([getTranslations('visit.contact'), getBranches(), getSettings()]);
  const emails = [
    { key: 'general', email: settings.contact.email },
    { key: 'events', email: settings.contact.events },
    { key: 'press', email: settings.contact.press },
    { key: 'careers', email: settings.contact.careers },
  ];
  return (
    <>
      <PageHeader eyebrow={t('eyebrow')} title={t('title')} intro={<p>{t('intro')}</p>} />
      <div className="site-grid gap-y-16">
        <div className="col-span-full lg:col-span-7">
          <ContactForm branches={branches.map((b) => ({ slug: b.slug, name: tr(b.shortName, locale) }))} defaultSubject={subject} />
        </div>
        <aside className="col-span-full flex flex-col gap-10 lg:col-span-4 lg:col-start-9">
          <section aria-labelledby="contact-houses" className="flex flex-col gap-4">
            <h2 id="contact-houses" className="t-label text-muted">
              {t('houses')}
            </h2>
            <ul className="flex flex-col gap-4">
              {branches.map((b) => (
                <li key={b.id} className="border-t border-line pt-4">
                  <p className="t-heading-sm">{tr(b.shortName, locale)}</p>
                  <a href={telUrl(b.phone)} dir="ltr" className="t-body tabular underline decoration-line underline-offset-4">
                    {b.phone}
                  </a>
                </li>
              ))}
            </ul>
          </section>
          <section aria-labelledby="contact-direct" className="flex flex-col gap-4">
            <h2 id="contact-direct" className="t-label text-muted">
              {t('direct')}
            </h2>
            <dl className="flex flex-col gap-3">
              {emails.map((e) => (
                <div key={e.key} className="border-t border-line pt-3">
                  <dt className="t-small text-muted">{t(`emails.${e.key}`)}</dt>
                  <dd>
                    <a href={`mailto:${e.email}`} dir="ltr" className="t-body underline decoration-line underline-offset-4">
                      {e.email}
                    </a>
                  </dd>
                </div>
              ))}
            </dl>
          </section>
        </aside>
      </div>
    </>
  );
}
