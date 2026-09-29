import { SimulatedProvider } from './simulated-provider';
import { StripeProvider } from './stripe-provider';
import type { PaymentProvider } from './types';

/**
 * Provider registry. The first provider whose `enabled()` returns true is used.
 * To add a regional gateway: create `<name>-provider.ts` implementing PaymentProvider and add one entry here
 * (above `simulated`), enabled by the presence of its keys in the environment.
 */
const registry: { id: string; enabled: () => boolean; create: () => PaymentProvider }[] = [
  {
    id: 'stripe',
    enabled: () => Boolean(process.env.STRIPE_SECRET_KEY && process.env.NEXT_PUBLIC_STRIPE_PUBLISHABLE_KEY),
    create: () => new StripeProvider(process.env.STRIPE_SECRET_KEY as string),
  },
  { id: 'simulated', enabled: () => true, create: () => new SimulatedProvider() },
];

let cached: PaymentProvider | null = null;

export function getPaymentProvider(): PaymentProvider {
  if (!cached) {
    const entry = registry.find((r) => r.enabled()) ?? (registry[registry.length - 1] as (typeof registry)[number]);
    cached = entry.create();
  }
  return cached;
}

export function getProviderById(id: string): PaymentProvider | null {
  const entry = registry.find((r) => r.id === id);
  return entry ? entry.create() : null;
}
