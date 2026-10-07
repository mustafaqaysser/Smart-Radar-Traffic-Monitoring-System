import 'server-only';
import { and, eq, isNull, lte } from 'drizzle-orm';
import { getTranslations } from 'next-intl/server';
import restaurantConfig from '@config';
import { db } from '@/lib/db/client';
import * as s from '@/lib/db/schema';
import { checkGiftCard, normalizeGiftCardCode } from '@/lib/domain/gift-cards';
import { formatDate, formatDateTime, formatMoney } from '@/lib/i18n/format';
import { audit, notifyStaff } from '@/lib/server/audit';
import { mail } from '@/lib/server/mail';
import { linkToken, verifyLinkToken } from '@/lib/server/tokens';
import { absoluteUrl } from '@/lib/site/url';
import { createGiftCardCode, createId } from '@/lib/utils/id';
import type { StartedPayment } from './payments';

export type GiftCard = typeof s.giftCards.$inferSelect;

const rules = restaurantConfig.giftCards;
const TIME_ZONE = restaurantConfig.defaultTimeZone;

export function giftCardArtUrl(design: string, format: 'png' | 'svg' = 'png'): string {
  return absoluteUrl(`/api/gift-cards/art/${design}${format === 'png' ? '?format=png' : ''}`);
}

/** The purchaser's receipt page (no locale prefix). */
export function giftCardPath(card: Pick<GiftCard, 'id'>): string {
  return `/gift-cards/sent/${card.id}?token=${linkToken('gift', card.id)}`;
}

export async function giftCardByLink(id: string, token: string | null | undefined): Promise<GiftCard | null> {
  if (!verifyLinkToken('gift', id, token)) return null;
  return (await db.query.giftCards.findFirst({ where: eq(s.giftCards.id, id) })) ?? null;
}

export interface PurchaseInput {
  amount: number;
  design: string;
  purchaserName: string;
  purchaserEmail: string;
  recipientName: string;
  recipientEmail: string;
  message: string | null;
  /** Delivery time for the recipient's email; null sends it as soon as the payment clears. */
  deliverAt: Date | null;
  locale: string;
  userId: string | null;
}

/** Creates the card (waiting for payment) and its payment. The code is only revealed once paid. */
export async function purchaseGiftCard(input: PurchaseInput): Promise<{ card: GiftCard; payment: StartedPayment }> {
  const id = createId();
  const now = new Date();
  const expiresAt = new Date(now);
  expiresAt.setUTCMonth(expiresAt.getUTCMonth() + rules.validityMonths);
  const rows = await db
    .insert(s.giftCards)
    .values({
      id,
      code: createGiftCardCode(),
      initialAmount: input.amount,
      balance: input.amount,
      currency: restaurantConfig.currency,
      design: input.design,
      purchaserName: input.purchaserName,
      purchaserEmail: input.purchaserEmail,
      recipientName: input.recipientName,
      recipientEmail: input.recipientEmail,
      message: input.message,
      locale: input.locale,
      deliverAt: input.deliverAt,
      status: 'pending_payment',
      expiresAt,
      userId: input.userId,
    })
    .returning();
  const card = rows[0] as GiftCard;
  const { startPayment } = await import('./payments');
  const payment = await startPayment({
    purpose: 'gift_card',
    referenceId: card.id,
    amount: card.initialAmount,
    description: `Zill gift card · ${formatMoney(card.initialAmount, 'en')}`,
    email: card.purchaserEmail,
    locale: input.locale,
    returnPath: `/${input.locale}${giftCardPath(card)}`,
  });
  return { card, payment };
}

/** Payment cleared: the card becomes live, the purchaser gets a receipt, and the recipient their card (now or later). */
export async function confirmGiftCardPaid(cardId: string, paymentId: string): Promise<void> {
  const now = new Date();
  const card = await db.query.giftCards.findFirst({ where: eq(s.giftCards.id, cardId) });
  if (!card || card.status !== 'pending_payment') return;
  const scheduled = Boolean(card.deliverAt && card.deliverAt.getTime() > now.getTime() + 60_000);
  const updated = await db
    .update(s.giftCards)
    .set({ status: scheduled ? 'scheduled' : 'active', paymentId, updatedAt: now })
    .where(and(eq(s.giftCards.id, cardId), eq(s.giftCards.status, 'pending_payment')))
    .returning();
  const live = updated[0];
  if (!live) return;
  await db.insert(s.giftCardTransactions).values({ id: createId(), giftCardId: live.id, kind: 'issue', amount: live.initialAmount, note: live.purchaserEmail });
  const t = await getTranslations({ locale: live.locale, namespace: 'gather.giftCards.receipt' });
  await mail(live.purchaserEmail, {
    name: 'gift-card-receipt',
    props: {
      locale: live.locale,
      purchaserName: live.purchaserName,
      recipientName: live.recipientName,
      amountLabel: formatMoney(live.initialAmount, live.locale),
      deliveryLabel: scheduled && live.deliverAt ? t('on', { date: formatDateTime(live.deliverAt, live.locale, TIME_ZONE) }) : t('now'),
      code: live.code,
    },
  });
  if (!scheduled) await deliverGiftCard(live, now);
  await notifyStaff({ role: 'manager', kind: 'gift_card', title: { ar: `بطاقة هدية بقيمة ${formatMoney(live.initialAmount, 'ar')}`, en: `Gift card sold: ${formatMoney(live.initialAmount, 'en')}` }, body: { ar: live.purchaserName, en: live.purchaserName }, href: '/admin/gift-cards' });
}

async function deliverGiftCard(card: GiftCard, now: Date): Promise<void> {
  const claimed = await db.update(s.giftCards).set({ deliveredAt: now, status: card.status === 'scheduled' ? 'active' : card.status, updatedAt: now }).where(and(eq(s.giftCards.id, card.id), isNull(s.giftCards.deliveredAt))).returning();
  if (!claimed[0]) return;
  await mail(card.recipientEmail, {
    name: 'gift-card',
    props: {
      locale: card.locale,
      recipientName: card.recipientName,
      senderName: card.purchaserName,
      message: card.message,
      amountLabel: formatMoney(card.initialAmount, card.locale),
      code: card.code,
      designImageUrl: giftCardArtUrl(card.design),
      balanceUrl: absoluteUrl(`/${card.locale}/gift-cards?code=${encodeURIComponent(card.code)}#balance`),
      expiresLabel: card.expiresAt ? formatDate(card.expiresAt, card.locale, TIME_ZONE, { day: 'numeric', month: 'long', year: 'numeric' }) : '—',
    },
  });
}

/** Scheduled cards whose time has come (run every few minutes by cron). */
export async function deliverScheduledGiftCards(now = new Date()): Promise<number> {
  const due = await db.query.giftCards.findMany({ where: and(eq(s.giftCards.status, 'scheduled'), lte(s.giftCards.deliverAt, now), isNull(s.giftCards.deliveredAt)) });
  for (const card of due) await deliverGiftCard(card, now);
  return due.length;
}

/** Public balance check (the code alone; rate limited by the caller). */
export async function giftCardBalance(code: string, now = new Date()) {
  const card = await db.query.giftCards.findFirst({ where: eq(s.giftCards.code, normalizeGiftCardCode(code)) });
  if (!card || card.status === 'pending_payment') return { ok: false as const, reason: 'not_found' as const };
  const check = checkGiftCard({ balance: card.balance, status: card.status, expiresAt: card.expiresAt }, now);
  return { ok: true as const, balance: card.balance, initial: card.initialAmount, expiresAt: card.expiresAt?.toISOString() ?? null, usable: check.ok, reason: check.ok ? null : check.reason };
}

/** Staff: void a card (its balance can no longer be spent). */
export async function voidGiftCard(cardId: string, actor: { id: string; email: string }, note: string | null): Promise<boolean> {
  const card = await db.query.giftCards.findFirst({ where: eq(s.giftCards.id, cardId) });
  if (!card || card.status === 'void') return false;
  await db.update(s.giftCards).set({ status: 'void', updatedAt: new Date() }).where(eq(s.giftCards.id, cardId));
  await db.insert(s.giftCardTransactions).values({ id: createId(), giftCardId: cardId, kind: 'void', amount: -card.balance, note });
  await audit({ actor, action: 'gift_card.void', entity: 'gift_card', entityId: cardId, summary: card.code });
  return true;
}

/** Staff: adjust a balance up or down (e.g. a goodwill top-up), never below zero. */
export async function adjustGiftCard(cardId: string, delta: number, actor: { id: string; email: string }, note: string | null): Promise<GiftCard | null> {
  const card = await db.query.giftCards.findFirst({ where: eq(s.giftCards.id, cardId) });
  if (!card || card.status === 'void' || card.status === 'pending_payment') return null;
  const balance = Math.max(0, card.balance + Math.round(delta));
  const rows = await db
    .update(s.giftCards)
    .set({ balance, status: balance === 0 ? 'redeemed' : card.status === 'redeemed' ? 'active' : card.status, updatedAt: new Date() })
    .where(eq(s.giftCards.id, cardId))
    .returning();
  await db.insert(s.giftCardTransactions).values({ id: createId(), giftCardId: cardId, kind: 'adjust', amount: balance - card.balance, note });
  await audit({ actor, action: 'gift_card.adjust', entity: 'gift_card', entityId: cardId, summary: `${card.code}: ${card.balance} → ${balance}` });
  return rows[0] ?? null;
}
