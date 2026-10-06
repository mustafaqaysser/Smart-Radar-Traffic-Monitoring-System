import type { SeedPackage, SeedPrivateRoom } from './types';

/** Rooms that close their doors for one table. */
export const privateRooms: SeedPrivateRoom[] = [
  {
    slug: 'rawshan-room',
    branch: 'al-balad',
    name: { ar: 'غرفة الروشان', en: 'The rawshan room' },
    description: {
      ar: 'غرفةٌ في الطابق الأول خلف أكبر رواشين البيت. في العصر يمرّ الضوء من خشب الروشان فيرسم على المائدة شبكةً من الظلّ، وفي الليل تُغلق المصاريع ويبقى صوت الزقاق بعيداً. مائدةٌ واحدة طويلة لاثني عشر ضيفاً.',
      en: 'A first-floor room behind the largest rawshan in the house. In the afternoon the light comes through the wooden lattice and lays a net of shade across the table; at night the shutters close and the lane goes quiet. One long table, twelve guests.',
    },
    seated: 12,
    standing: null,
    features: [
      { ar: 'مائدةٌ واحدة طويلة من خشب الساج', en: 'A single long teak table' },
      { ar: 'روشانٌ يطلّ على الزقاق', en: 'A rawshan over the lane' },
      { ar: 'مضيفٌ خاصّ للغرفة طوال الأمسية', en: 'A host of your own for the evening' },
      { ar: 'يُصعد إليها بدرج', en: 'Reached by a staircase' },
    ],
    image: 'place-long-table',
  },
  {
    slug: 'roof',
    branch: 'al-balad',
    name: { ar: 'السطح', en: 'The roof' },
    description: {
      ar: 'فوق البيت سطحٌ مفتوح على سطوح البلد ومآذنها. لا يُحجز كاملاً إلّا مساءً، حين تهبّ نسمة البحر بعد المغرب وتُضاء الفوانيس واحداً واحداً. يتّسع لأربعةٍ وعشرين جالساً، أو لأربعين واقفين.',
      en: 'Above the house, a roof open to the rooftops and minarets of Al-Balad. It is booked whole in the evening only, when the sea breeze arrives after sunset and the lanterns are lit one by one. Twenty-four seated, or forty standing.',
    },
    seated: 24,
    standing: 40,
    features: [
      { ar: 'إطلالةٌ على سطوح البلد التاريخية', en: 'A view over the rooftops of historic Al-Balad' },
      { ar: 'فوانيس ومناقل في الشتاء', en: 'Lanterns, and braziers in winter' },
      { ar: 'ركنٌ للقهوة تُعدّ فيه الدلال أمام ضيوفك', en: 'A coffee corner where the dallahs are made in front of your guests' },
      { ar: 'يُصعد إليه بدرج', en: 'Reached by stairs only' },
    ],
    image: 'place-candle-table',
  },
  {
    slug: 'majlis',
    branch: 'wadi-hanifah',
    name: { ar: 'المجلس', en: 'The majlis' },
    description: {
      ar: 'مجلسٌ من الطين تحت سقفٍ من جذوع النخل، تدخله ريح الوادي من الفتحات المثلّثة في جداره. يجلس فيه الضيوف على المساند كما يجلسون في بيوت نجد، أو إلى مائدةٍ منخفضة إن شاؤوا. وله بابٌ خاصّ إلى ساحة النخيل.',
      en: 'A mud-brick majlis under a ceiling of palm trunks, with the valley air coming in through the triangular vents in its walls. Guests sit against cushions as they would in a Najdi house, or at a low table if they prefer. It has its own door to the palm yard.',
    },
    seated: 14,
    standing: null,
    features: [
      { ar: 'جلسةٌ أرضية أو مائدةٌ منخفضة', en: 'Floor seating or a low table' },
      { ar: 'بابٌ خاصّ إلى ساحة النخيل', en: 'A private door to the palm yard' },
      { ar: 'منحدرٌ متنقّل عند الطلب', en: 'A portable ramp on request' },
      { ar: 'وجارٌ للقهوة يُشعل في ليالي الشتاء', en: 'A coffee hearth, lit on winter nights' },
    ],
    image: 'place-majlis',
  },
];

/** Menus for private tables, and one for gatherings away from the houses. Prices per guest in halalas. */
export const packages: SeedPackage[] = [
  {
    kind: 'private_dining',
    name: { ar: 'سفرة الظهيرة', en: 'The noon table' },
    description: {
      ar: 'غداءٌ للمشاركة على مهل: حمّصٌ بالسمن، وفتّوش بالسماق، ثم صياديّة وكبسة بالأرز الحساوي، وأمّ علي في الختام، مع قهوةٍ وتمر. للظهيرات التي لا يريد أحدٌ أن تنتهي.',
      en: 'A slow lunch to share: hummus with ghee and fattoush with sumac, then sayadiyah and a kabsa of red Hassawi rice, umm ali to finish, coffee and dates. For afternoons nobody wants to end.',
    },
    pricePerGuest: 22000,
    minGuests: 10,
  },
  {
    kind: 'private_dining',
    name: { ar: 'سفرة المغرب', en: 'The sunset table' },
    description: {
      ar: 'عشاءٌ يبدأ مع آخر الضوء: مقبّلاتٌ من الجمر والبستان، وسمكٌ في الملح من سوق الفجر، وكتف خروفٍ مندي، ثم كنافة ومهلّبية بورد الطائف. ودلّة القهوة لا تفرغ.',
      en: 'A dinner that begins with the last of the light: small plates from the embers and the garden, fish baked in salt from the dawn market, mandi lamb shoulder, then kunafa and a Taif rose muhallabia. The dallah never runs dry.',
    },
    pricePerGuest: 34000,
    minGuests: 10,
  },
  {
    kind: 'private_dining',
    name: { ar: 'المزولة لكم وحدكم', en: 'The Sundial, behind closed doors' },
    description: {
      ar: 'قائمة التذوّق بأطباقها السبعة، من تمرة الفجر إلى شاي منتصف الليل، تُقدَّم لطاولتكم وحدها، ويقدّم الشيف كلّ طبقٍ بنفسه. نكيّفها مع الحساسية إن أخبرتمونا قبل ٤٨ ساعة.',
      en: 'The seven-course tasting menu, from a date at dawn to mint tea at midnight, served to your table alone, with the chef presenting each course. We adapt it to allergies with 48 hours’ notice.',
    },
    pricePerGuest: 52000,
    minGuests: 8,
  },
  {
    kind: 'catering',
    name: { ar: 'الفناء يأتي إليك', en: 'The courtyard comes to you' },
    description: {
      ar: 'لمجالسكم وأعراسكم ولقاءات العمل خارج البيتين: قدورٌ كبيرة من المندي والجريش والسليق، وخبزٌ من تنّور أبي صالح، ومُقهوِجٌ بدلاله وفناجينه، ومن يقدّم ويرتّب ويرفع. في جدّة والرياض، لخمسةٍ وعشرين ضيفاً فأكثر.',
      en: 'For gatherings, weddings and company lunches away from the houses: large pots of mandi, jareesh and saleeg, bread from Abu Saleh’s tannour, a coffee server with dallahs and cups, and people to serve, arrange and clear. In Jeddah and Riyadh, for twenty-five guests or more.',
    },
    pricePerGuest: 18000,
    minGuests: 25,
  },
];
