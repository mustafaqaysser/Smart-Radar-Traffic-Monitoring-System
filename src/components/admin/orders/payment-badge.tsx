'use client';

import type { OrderCard } from '@/lib/admin/orders';
import { Badge } from '../ui/badge';
import { usePaymentLabel } from './order-card';

export function PaymentBadge({ order }: { order: Pick<OrderCard, 'paymentMethod' | 'paymentStatus' | 'channel'> }) {
  const p = usePaymentLabel()(order);
  return <Badge tone={p.tone}>{p.text}</Badge>;
}
