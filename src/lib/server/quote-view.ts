import 'server-only';
import { tr } from '@/lib/i18n/localized';
import type { QuoteView } from '@/lib/order/types';
import type { Quote } from './order-quote';

/** The client-safe view of a quote: names in the guest's language, zones flattened. */
export function toQuoteView(q: Quote, locale: string): QuoteView {
  return {
    lines: q.lines.map((l) => ({ key: l.key, slug: l.slug, name: l.name, qty: l.qty, unitPrice: l.unitPrice, lineTotal: l.lineTotal, options: l.options.map((o) => tr(o.name, locale)), note: l.note, problem: l.problem })),
    pricing: q.pricing,
    promo: q.promo,
    giftCard: q.giftCard ? (q.giftCard.ok ? { code: q.giftCard.code, ok: true, balance: q.giftCard.balance, applied: q.giftCard.applied } : q.giftCard) : null,
    loyalty: q.loyalty,
    zone: q.zone ? { id: q.zone.id, name: tr(q.zone.name, locale), kind: q.zone.kind, areas: q.zone.areas.map((a) => tr(a, locale)), radiusKm: q.zone.radiusKm, fee: q.zone.fee, minOrder: q.zone.minOrder, etaMinutes: q.zone.etaMinutes } : null,
    zoneProblem: q.zoneProblem,
    timing: q.timing,
    tipAllowed: q.tipAllowed,
    serviceChargeRate: q.serviceChargeRate,
    taxRate: q.taxRate,
    problems: q.problems,
    canPlace: q.canPlace,
  };
}
