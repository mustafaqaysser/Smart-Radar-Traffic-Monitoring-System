import type { SeedSeasonalMode } from './types';

/** Seasons the houses change for. Ramadan, Eid and New Year dates are computed by the seed. */
export const seasonalModes: SeedSeasonalMode[] = [
  {
    slug: 'date-harvest',
    kind: 'custom',
    name: { ar: 'موسم الصِّرام', en: 'Date harvest' },
    banner: {
      ar: 'وصل تمر عنيزة الجديد، فتجده هذا الشهر مع القهوة، وفي طبقٍ أو اثنين لم يكونا هنا الأسبوع الماضي.',
      en: 'The new dates from Unaizah are in: with the coffee all month, and in a dish or two that was not here last week.',
    },
    startInDays: -20,
    endInDays: 25,
    theme: 'none',
    enabled: true,
  },
  {
    slug: 'ramadan',
    kind: 'ramadan',
    name: { ar: 'رمضان', en: 'Ramadan' },
    banner: {
      ar: 'في رمضان يُفتح الفناء عند المغرب، ويبقى ساهراً حتى السحور.',
      en: 'In Ramadan the courtyard opens at sunset and stays awake until suhoor.',
    },
    startInDays: 0,
    endInDays: 0,
    theme: 'ramadan',
    enabled: true,
  },
  {
    slug: 'eid-al-fitr',
    kind: 'eid',
    name: { ar: 'عيد الفطر', en: 'Eid al-Fitr' },
    banner: {
      ar: 'عيدكم مبارك؛ يُفتح الفناء من صباح العيد، وتسبقكم الكليجا إلى الطاولة.',
      en: 'Eid Mubarak. The courtyard opens on Eid morning, and the kleija reaches the table before you do.',
    },
    startInDays: 0,
    endInDays: 0,
    theme: 'eid',
    enabled: true,
  },
  {
    slug: 'new-year',
    kind: 'new_year',
    name: { ar: 'ليلة رأس السنة', en: 'New Year’s Eve' },
    banner: {
      ar: 'في آخر ليلةٍ من السنة يسهر الفناء حتى الواحدة والنصف، وشاي النعناع عند منتصف الليل على حسابنا.',
      en: 'On the last night of the year the courtyard stays open until 01:30, and the mint tea at midnight is on us.',
    },
    startInDays: 0,
    endInDays: 0,
    theme: 'new_year',
    enabled: true,
  },
  {
    slug: 'winter-courtyard',
    kind: 'custom',
    name: { ar: 'شتاء الفناء', en: 'The winter courtyard' },
    banner: {
      ar: 'حين يبرد الليل نوقد المناقل في الفناء، ونترك على كلّ كرسيٍّ بطانية.',
      en: 'When the nights turn cold we light braziers in the courtyard and leave a blanket on every chair.',
    },
    startInDays: 56,
    endInDays: 146,
    theme: 'none',
    enabled: true,
  },
];
