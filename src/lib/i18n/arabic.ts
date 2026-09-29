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

/** True when every query token appears in the haystack (both normalised). */
export function matchesSearch(haystack: string, query: string): boolean {
  const q = normalizeForSearch(query);
  if (!q) return true;
  const h = normalizeForSearch(haystack);
  return q.split(' ').every((token) => h.includes(token));
}
