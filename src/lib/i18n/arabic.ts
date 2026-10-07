/**
 * Arabic-aware search normalisation: strips tashkeel and tatweel, unifies alef variants, yaa / alef maqsura,
 * taa marbuta / haa and hamza carriers, folds Persian letter forms, and lowercases/de-accents Latin.
 */
const TASHKEEL = /[ؐ-ًؚ-ٰٟۖ-ۜ۟-۪ۨ-ۭ]/g;
const TATWEEL = /ـ/g;

export function normalizeArabic(input: string): string {
  return input
    .replace(TASHKEEL, '')
    .replace(TATWEEL, '')
    .replace(/[آأإٱٲٳ]/g, 'ا') // آ أ إ ٱ → ا
    .replace(/ى/g, 'ي') // ى → ي
    .replace(/ة/g, 'ه') // ة → ه
    .replace(/ؤ/g, 'و') // ؤ → و
    .replace(/ئ/g, 'ي') // ئ → ي
    .replace(/ک/g, 'ك') // ک → ك
    .replace(/[یے]/g, 'ي'); // ی ے → ي
}

export function normalizeForSearch(input: string): string {
  return normalizeArabic(input)
    .normalize('NFKD')
    .replace(/[̀-ͯ]/g, '')
    .toLowerCase()
    .replace(/[^\p{L}\p{N}\s]/gu, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

/** Leading Arabic article and attached particles (ال، وال، بال، كال، فال، لل) are dropped from query words. */
const ARTICLE = /^(?:وال|بال|كال|فال|لل|ال)(?=..)/;

/** Normalised query words, with the Arabic definite article removed so «الحمص» finds «حمص». */
export function searchTokens(query: string): string[] {
  const q = normalizeForSearch(query);
  return q ? q.split(' ').map((t) => t.replace(ARTICLE, '')) : [];
}

/** True when every query word appears in the haystack (both normalised). */
export function matchesSearch(haystack: string, query: string): boolean {
  const tokens = searchTokens(query);
  if (!tokens.length) return true;
  const h = normalizeForSearch(haystack);
  return tokens.every((token) => h.includes(token));
}
