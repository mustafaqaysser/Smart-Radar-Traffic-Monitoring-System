import type { AppLocale } from '@/i18n/routing';

/** Namespaced message files, merged per locale. Each language is written natively — never translated word for word. */
const NAMESPACES = ['common', 'home', 'menu', 'reserve', 'order', 'table', 'account', 'pages', 'visit', 'gather', 'legal', 'forms'] as const;

/** The back-office copy is split by area (src/messages/<locale>/admin/<part>.json) and merged under `admin`. */
export const ADMIN_PARTS = [
  'shell',
  'auth',
  'ui',
  'errors',
  'dashboard',
  'orders',
  'kds',
  'reservations',
  'tables',
  'menu',
  'customers',
  'events',
  'commerce',
  'guests',
  'content',
  'locations',
  'seasonal',
  'settings',
  'reports',
  'system',
] as const;

export async function loadMessages(locale: AppLocale): Promise<Record<string, unknown>> {
  const [parts, admin] = await Promise.all([
    Promise.all(NAMESPACES.map(async (ns) => [ns, (await import(`./${locale}/${ns}.json`)).default] as const)),
    Promise.all(ADMIN_PARTS.map(async (part) => [part, (await import(`./${locale}/admin/${part}.json`)).default] as const)),
  ]);
  return { ...Object.fromEntries(parts), admin: Object.fromEntries(admin) };
}
