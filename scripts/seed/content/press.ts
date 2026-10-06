import type { SeedPress } from './types';

/** What others wrote. Every publication and award here is fictional. */
export const press: SeedPress[] = [
  {
    kind: 'press',
    publication: { ar: 'فصلية مائدة الحجاز', en: 'Hejaz Table Quarterly' },
    title: { ar: 'بيتٌ يطبخ على ساعة الشمس', en: 'A house that cooks by the sun' },
    quote: {
      ar: 'المطعم الوحيد في جدّة الذي تتغيّر قائمته لأنّ الضوء تغيّر.',
      en: 'The only restaurant in Jeddah whose menu changes because the light did.',
    },
    year: 2022,
  },
  {
    kind: 'press',
    publication: { ar: 'دفاتر طعام الجزيرة', en: 'Peninsula Food Notes' },
    title: { ar: 'الخبز عند السادسة إلا ربعاً', en: 'Bread at quarter to six' },
    quote: {
      ar: 'تعال في السابعة لأجل التميس، وتعال في السادسة لترى لماذا يستحقّ أن تأتي في السابعة.',
      en: 'Come at seven for the tamees. Come at six and you will understand why seven is worth it.',
    },
    year: 2023,
  },
  {
    kind: 'press',
    publication: { ar: 'أطلس الموائد المتأنّية', en: 'The Atlas of Slow Tables' },
    title: { ar: 'أهدأ فناءٍ في الرياض', en: 'Riyadh’s quietest courtyard' },
    quote: {
      ar: 'تعبر مثلّثات الضوء الأرض وأنت تأكل، ولا أحد ينظر إلى هاتفه؛ وهذا في الرياض شهادةٌ قائمةٌ بذاتها.',
      en: 'Triangles of light cross the floor while you eat, and nobody looks at their phone. In Riyadh, that is a review in itself.',
    },
    year: 2024,
  },
  {
    kind: 'press',
    publication: { ar: 'مجلّة السفرة الطويلة', en: 'The Long Table Review' },
    title: { ar: 'سبعة أطباق، ونهارٌ واحد', en: 'Seven courses, one day' },
    quote: {
      ar: 'تنتهي «المزولة» بكأس شاي ونعناع وتمرةٍ واحدة، وهي أجرأ خاتمةٍ لقائمة تذوّقٍ ذقتها منذ سنوات.',
      en: 'The Sundial ends with a glass of mint tea and a single date. It is the bravest last course I have eaten in years.',
    },
    year: 2025,
  },
  {
    kind: 'award',
    publication: { ar: 'قائمة الفانوس لموائد الخليج', en: 'The Lantern List of Gulf Tables' },
    title: { ar: 'مطعم العام في المنطقة الغربية', en: 'Restaurant of the Year, Western Region' },
    quote: {
      ar: 'لمطبخٍ يعامل الوقت كما يعامل الملح: مكوّناً لا يُستغنى عنه.',
      en: 'For a kitchen that treats time the way it treats salt: as an ingredient it cannot do without.',
    },
    year: 2025,
  },
  {
    kind: 'award',
    publication: { ar: 'جائزة الطين والضوء لعمارة الضيافة', en: 'The Mud & Light Prize for Hospitality Architecture' },
    title: { ar: 'الجائزة الأولى: بيت ظل في وادي حنيفة', en: 'First prize: Zill · Wadi Hanifah' },
    quote: {
      ar: 'بيتٌ بُني من الوادي الذي يقف فيه، فيبرد كما يبرد الوادي، ويضيء كما يضيء.',
      en: 'A house built from the valley it stands in. It cools as the valley cools, and takes its light the same way.',
    },
    year: 2025,
  },
];
