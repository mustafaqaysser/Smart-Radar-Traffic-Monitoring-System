/**
 * Picks the visitor's preferred language from an Accept-Language header, by quality, among the site's locales.
 * Region subtags are ignored ("ar-SA" counts as "ar"); anything unsupported falls back to the default locale.
 */
export function preferredLocale<L extends string>(header: string | null, locales: readonly L[], fallback: L): L {
  const wanted = (header ?? '')
    .split(',')
    .map((part, index) => {
      const [tag = '', ...params] = part.trim().split(';');
      const q = params.map((p) => p.trim()).find((p) => p.startsWith('q='));
      const quality = q === undefined ? 1 : Number(q.slice(2));
      return { lang: tag.trim().toLowerCase().split('-')[0] ?? '', q: Number.isFinite(quality) ? quality : 0, index };
    })
    .filter((x) => x.lang && x.q > 0)
    .sort((a, b) => b.q - a.q || a.index - b.index);
  return (wanted.find((x) => (locales as readonly string[]).includes(x.lang))?.lang as L | undefined) ?? fallback;
}
