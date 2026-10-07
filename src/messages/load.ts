import type { AppLocale } from '@/i18n/routing';

/** Namespaced message files, merged per locale. Each language is written natively — never translated word for word. */
const NAMESPACES = ['common', 'home', 'menu', 'reserve', 'order', 'table', 'account', 'pages', 'visit', 'gather', 'legal', 'forms', 'admin'] as const;

export async function loadMessages(locale: AppLocale): Promise<Record<string, unknown>> {
  const parts = await Promise.all(NAMESPACES.map(async (ns) => [ns, (await import(`./${locale}/${ns}.json`)).default] as const));
  return Object.fromEntries(parts);
}
