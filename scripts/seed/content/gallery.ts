import type { L } from './types';

/** The dial gallery: photographs arranged by the hour they belong to. */
export const gallery: { media: string; hour: 'dawn' | 'morning' | 'noon' | 'afternoon' | 'dusk' | 'night'; caption: L }[] = [
  { media: 'place-window-beam', hour: 'dawn', caption: { ar: 'أول ضوءٍ يدخل من النافذة الشرقية.', en: 'First light through the east window.' } },
  { media: 'craft-bread-oven', hour: 'dawn', caption: { ar: 'التنّور عند الخامسة إلا ثلثاً.', en: 'The tannour at twenty to five.' } },
  { media: 'craft-kneading', hour: 'dawn', caption: { ar: 'عجين الصباح قبل أن يستيقظ الزقاق.', en: 'Morning dough, before the lane wakes up.' } },
  { media: 'place-courtyard-sun', hour: 'morning', caption: { ar: 'الفناء والظلّ ما زال طويلاً على الجدار الغربي.', en: 'The courtyard, the shade still long on the west wall.' } },
  { media: 'dish-tamees-foul', hour: 'morning', caption: { ar: 'فولٌ وتميس، فطور جدّة.', en: "Foul and tamees — Jeddah's breakfast." } },
  { media: 'craft-coffee-pour', hour: 'morning', caption: { ar: 'القهوة تُصبّ قليلاً قليلاً.', en: 'Coffee, poured a little at a time.' } },
  { media: 'place-lattice-light', hour: 'noon', caption: { ar: 'الروشان يكسر شمس الظهيرة.', en: 'The rawshan breaks the noon sun.' } },
  { media: 'place-arch-shadow', hour: 'noon', caption: { ar: 'الظلّ في أقصره تحت القوس.', en: 'The shade at its shortest under the arch.' } },
  { media: 'dish-hassawi-kabsa', hour: 'noon', caption: { ar: 'الرزّ الحساوي الأحمر وكتف الخروف.', en: 'Red Hassawi rice and lamb shoulder.' } },
  { media: 'place-palm-shadow-1', hour: 'afternoon', caption: { ar: 'سعف النخل على الجص عند العصر.', en: 'Palm fronds on the plaster in the afternoon.' } },
  { media: 'dish-luqaimat', hour: 'afternoon', caption: { ar: 'لقيماتٌ بدبس التمر مع القهوة.', en: 'Luqaimat and date syrup with coffee.' } },
  { media: 'place-palm-grove', hour: 'afternoon', caption: { ar: 'نخيل الوادي قبل المغرب بساعة.', en: 'The valley palms an hour before sunset.' } },
  { media: 'place-desert-dusk', hour: 'dusk', caption: { ar: 'حين تغيب الشمس يبدأ المساء.', en: 'When the sun goes, the evening begins.' } },
  { media: 'place-door', hour: 'dusk', caption: { ar: 'بابٌ قديم يبرد مع الغروب.', en: 'An old door cooling at sunset.' } },
  { media: 'place-lantern', hour: 'night', caption: { ar: 'القناديل تُضاء واحداً واحداً.', en: 'The lamps, lit one by one.' } },
  { media: 'place-night-courtyard', hour: 'night', caption: { ar: 'فناء الليل.', en: 'The night courtyard.' } },
  { media: 'dish-lamb-chops-samr', hour: 'night', caption: { ar: 'ريش الغنم على جمر السمر.', en: 'Lamb chops over samr embers.' } },
  { media: 'place-candle-table', hour: 'night', caption: { ar: 'طاولةٌ لاثنين بعد العاشرة.', en: 'A table for two after ten.' } },
];
