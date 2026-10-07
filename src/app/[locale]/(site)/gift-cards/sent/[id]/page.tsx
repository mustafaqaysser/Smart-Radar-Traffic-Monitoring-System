import type { Metadata } from 'next';
import { getTranslations, setRequestLocale } from 'next-intl/server';
import restaurantConfig from '@config';
import { SplitWords } from '@/components/motion/split-words';
import { GiftCardPreview } from '@/components/site/gift-cards/gift-card-preview';
import { PaymentResume } from '@/components/site/payment-resume';
import { ButtonLink } from '@/components/site/ui/button';
import { PageHeader } from '@/components/site/ui/page-header';
import { formatDateTime, formatMoney } from '@/lib/i18n/format';
import { giftCardByLink, giftCardPath } from '@/lib/server/gift-cards';
import { paymentFor, resumablePayment, syncPayment } from '@/lib/server/payments';
import { pageMetadata } from '@/lib/site/metadata';

type Params = Promise<{ locale: string; id: string }>;
type Search = Promise<Record<string, string | string[] | undefined>>;
const one = (v: string | string[] | undefined): string | null => (Array.isArray(v) ? (v[0] ?? null) : (v ?? null));

export async function generateMetadata({ params }: { params: Params }): Promise<Metadata> {
  const { locale, id } = await params;
  const t = await getTranslations({ locale, namespace: 'gather.giftCards.sent' });
  return pageMetadata({ locale, path: `/gift-cards/sent/${id}`, title: t('meta'), noindex: true });
}

export default async function GiftCardSentPage({ params, searchParams }: { params: Params; searchParams: Search }) {
  const { locale, id } = await params;
  setRequestLocale(locale);
  const sp = await searchParams;
  const token = one(sp.token);
  const t = await getTranslations('gather.giftCards');
  let card = await giftCardByLink(id, token);
  if (!card) {
    return (
      <PageHeader eyebrow={t('eyebrow')} title={t('sent.meta')} intro={<p>{t('sent.invalid')}</p>}>
        <ButtonLink href="/gift-cards" className="self-start">
          {t('sent.another')}
        </ButtonLink>
      </PageHeader>
    );
  }
  const paymentId = one(sp.payment);
  if (paymentId && card.status === 'pending_payment') {
    const payment = await paymentFor(paymentId, card.id);
    if (payment) {
      await syncPayment(payment.id);
      card = (await giftCardByLink(id, token)) ?? card;
    }
  }
  const tz = restaurantConfig.defaultTimeZone;
  const pending = card.status === 'pending_payment';
  const payment = pending ? await resumablePayment({ purpose: 'gift_card', referenceId: card.id, amount: card.initialAmount, description: `Zill gift card · ${formatMoney(card.initialAmount, 'en')}`, email: card.purchaserEmail, locale, returnPath: `/${locale}${giftCardPath(card)}` }) : null;
  return (
    <article className="site-grid gap-y-12 pt-10 pb-[var(--spacing-section)] lg:pt-16">
      <header className="col-span-full flex flex-col gap-6 lg:col-span-6">
        <p className="t-label text-muted">{t('eyebrow')}</p>
        <h1 className="t-display-lg">
          <SplitWords text={pending ? t('sent.pending') : t('sent.title')} />
        </h1>
        {!pending ? (
          <>
            <p className="t-body-lg measure">{card.status === 'scheduled' && card.deliverAt ? t('sent.scheduled', { name: card.recipientName, date: formatDateTime(card.deliverAt, locale, tz) }) : t('sent.now', { name: card.recipientName })}</p>
            <p className="t-body text-muted">{t('sent.receipt', { email: card.purchaserEmail })}</p>
            <ButtonLink href="/gift-cards" variant="secondary" className="self-start">
              {t('sent.another')}
            </ButtonLink>
          </>
        ) : payment ? (
          <PaymentResume payment={payment} />
        ) : null}
      </header>
      <div className="col-span-full lg:col-span-5 lg:col-start-8">
        <GiftCardPreview design={card.design} amount={card.initialAmount} to={card.recipientName} from={card.purchaserName} message={card.message ?? undefined} label={t('designAlt', { design: t(`designs.${card.design}`) })} />
      </div>
    </article>
  );
}
