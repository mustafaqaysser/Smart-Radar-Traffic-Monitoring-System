/**
 * The two houses. Addresses and phone numbers are fictional (lanes named after the restaurant, 555 numbers).
 */
import type { OrderingSettings, ReservationSettings, TableArea } from '../../../src/lib/db/schema';
import type { BranchSlug, L } from './types';

export interface SeedBranch {
  slug: BranchSlug;
  codePrefix: string;
  name: L;
  shortName: L;
  city: L;
  district: L;
  address: L;
  story: L;
  lat: number;
  lng: number;
  phone: string;
  whatsapp: string;
  email: string;
  parking: L;
  accessibility: L;
  heroImage: string;
  reservation: ReservationSettings;
  ordering: OrderingSettings;
  venueHours: { weekdays: number[]; opens: string; closes: string }[];
  kitchenHours: { weekdays: number[]; opens: string; closes: string }[];
  periods: { key: string; name: L; weekdays: number[]; start: string; end: string; maxCovers?: number }[];
  tables: { label: string; area: TableArea; min: number; max: number; group: string | null; reservable?: boolean }[];
  zones: { name: L; kind: 'area' | 'radius'; areas?: L[]; radiusKm?: number; fee: number; minOrder: number; eta: number }[];
  specials: { inDays: number; label: L; closed: boolean; ranges?: { opens: string; closes: string }[]; reservationsBlocked?: boolean }[];
}

const SAT_TO_WED = [0, 1, 2, 3, 6];

const PERIODS: SeedBranch['periods'] = [
  { key: 'breakfast', name: { ar: 'الفطور', en: 'Breakfast' }, weekdays: [0, 1, 2, 3, 4, 6], start: '07:30', end: '10:30' },
  { key: 'breakfast-friday', name: { ar: 'فطور الجمعة', en: 'Friday breakfast' }, weekdays: [5], start: '08:00', end: '11:30' },
  { key: 'lunch', name: { ar: 'الغداء', en: 'Lunch' }, weekdays: [0, 1, 2, 3, 4, 5, 6], start: '12:30', end: '15:00' },
  { key: 'long-shade', name: { ar: 'الظلّ الطويل', en: 'The long shade' }, weekdays: [0, 1, 2, 3, 4, 5, 6], start: '16:00', end: '17:30', maxCovers: 10 },
  { key: 'dinner', name: { ar: 'العشاء', en: 'Dinner' }, weekdays: SAT_TO_WED, start: '19:00', end: '23:00' },
  { key: 'dinner-late', name: { ar: 'عشاء الخميس والجمعة', en: 'Thursday & Friday dinner' }, weekdays: [4, 5], start: '19:00', end: '00:00' },
];

export const branches: SeedBranch[] = [
  {
    slug: 'al-balad',
    codePrefix: 'BL',
    name: { ar: 'ظل · البلد', en: 'Zill · Al-Balad' },
    shortName: { ar: 'البلد', en: 'Al-Balad' },
    city: { ar: 'جدّة', en: 'Jeddah' },
    district: { ar: 'البلد التاريخية', en: 'Historic Al-Balad' },
    address: { ar: 'بيت ظل، زقاق الظلّ ٧، البلد التاريخية، جدّة', en: 'Bayt Zill, 7 Shade Lane, Historic Al-Balad, Jeddah' },
    story: {
      ar: 'البيت الأول: بيتٌ من الحجر المنقبي بُني من حجارة البحر، برواشين خشبية تكسر الشمس قبل أن تدخل، وفناءٍ تحت عريشة عنبٍ وشجرة ليمونٍ عجوز. هنا يمرّ الظلّ على الأرض مرّتين في اليوم، ومرّتين في السنة يختفي تماماً عند الظهيرة.',
      en: 'The first house: coral stone cut from the sea, wooden rawasheen that break the sun before it gets in, and a courtyard under a vine and an old lime tree. Here the shade crosses the floor twice a day — and twice a year, at noon, it disappears completely.',
    },
    lat: 21.4854,
    lng: 39.1869,
    phone: '+966125550170',
    whatsapp: '+966505550170',
    email: 'albalad@zill.test',
    parking: {
      ar: 'خدمة صفّ السيارات عند مدخل الزقاق ابتداءً من السادسة مساءً، ومواقف عامة على طريق الملك عبدالعزيز على بُعد أربع دقائق مشياً عبر البلد القديمة.',
      en: 'Valet at the end of the lane from 6 pm; public parking on King Abdulaziz Road, a four-minute walk through the old town.',
    },
    accessibility: {
      ar: 'مدخلٌ بلا درجاتٍ من الزقاق؛ الفناء والليوان ودورة مياهٍ واحدة مهيّأة للكراسي المتحرّكة. أمّا السطح فلا يُصعد إليه إلا بالدرج.',
      en: 'Step-free entrance from the lane; the courtyard, the liwan and one restroom are wheelchair accessible. The roof is reached by stairs only.',
    },
    heroImage: 'place-coral-house',
    reservation: {
      slotIntervalMinutes: 15,
      turnTimes: { '1': 75, '3': 95, '5': 120, '7': 150 },
      maxCoversPerSlot: 14,
      maxPartyOnline: 8,
      lastSeatingBeforeCloseMinutes: 45,
    },
    ordering: { maxOrdersPerWindow: 10, windowMinutes: 15, basePrepMinutes: 20, busyExtraMinutes: 15, radiusKm: 8 },
    venueHours: [
      { weekdays: SAT_TO_WED, opens: '07:00', closes: '00:30' },
      { weekdays: [4], opens: '07:00', closes: '01:30' },
      { weekdays: [5], opens: '08:00', closes: '01:30' },
    ],
    kitchenHours: [
      { weekdays: SAT_TO_WED, opens: '07:30', closes: '23:45' },
      { weekdays: [4], opens: '07:30', closes: '00:45' },
      { weekdays: [5], opens: '08:30', closes: '00:45' },
    ],
    periods: PERIODS,
    tables: [
      { label: 'C1', area: 'courtyard', min: 1, max: 2, group: 'courtyard-east' },
      { label: 'C2', area: 'courtyard', min: 1, max: 2, group: 'courtyard-east' },
      { label: 'C3', area: 'courtyard', min: 2, max: 4, group: 'courtyard-east' },
      { label: 'C4', area: 'courtyard', min: 2, max: 4, group: 'courtyard-east' },
      { label: 'C5', area: 'courtyard', min: 4, max: 6, group: 'courtyard-west' },
      { label: 'C6', area: 'courtyard', min: 4, max: 6, group: 'courtyard-west' },
      { label: 'C7', area: 'courtyard', min: 1, max: 2, group: null },
      { label: 'L1', area: 'liwan', min: 1, max: 2, group: 'liwan' },
      { label: 'L2', area: 'liwan', min: 1, max: 2, group: 'liwan' },
      { label: 'L3', area: 'liwan', min: 2, max: 4, group: 'liwan' },
      { label: 'L4', area: 'liwan', min: 2, max: 4, group: 'liwan' },
      { label: 'R1', area: 'roof', min: 1, max: 2, group: 'roof' },
      { label: 'R2', area: 'roof', min: 2, max: 4, group: 'roof' },
      { label: 'R3', area: 'roof', min: 4, max: 6, group: 'roof' },
      { label: 'P1', area: 'private', min: 6, max: 12, group: null },
    ],
    zones: [
      {
        name: { ar: 'البلد والبغدادية', en: 'Al-Balad & Al-Baghdadiyah' },
        kind: 'area',
        areas: [
          { ar: 'البلد', en: 'Al-Balad' },
          { ar: 'البغدادية', en: 'Al-Baghdadiyah' },
          { ar: 'الكندرة', en: 'Al-Kandarah' },
        ],
        fee: 8,
        minOrder: 50,
        eta: 30,
      },
      {
        name: { ar: 'الحمراء والأندلس', en: 'Al-Hamra & Al-Andalus' },
        kind: 'area',
        areas: [
          { ar: 'الحمراء', en: 'Al-Hamra' },
          { ar: 'الأندلس', en: 'Al-Andalus' },
          { ar: 'الرويس', en: 'Ar-Ruwais' },
        ],
        fee: 12,
        minOrder: 70,
        eta: 40,
      },
      {
        name: { ar: 'الروضة والزهراء', en: 'Ar-Rawdah & Az-Zahra' },
        kind: 'area',
        areas: [
          { ar: 'الروضة', en: 'Ar-Rawdah' },
          { ar: 'الزهراء', en: 'Az-Zahra' },
          { ar: 'الخالدية', en: 'Al-Khalidiyah' },
        ],
        fee: 18,
        minOrder: 90,
        eta: 50,
      },
      { name: { ar: 'ضمن ٨ كم من البيت', en: 'Within 8 km of the house' }, kind: 'radius', radiusKm: 8, fee: 20, minOrder: 100, eta: 55 },
    ],
    specials: [
      {
        inDays: 12,
        label: { ar: 'مناسبةٌ خاصة وقت الغداء', en: 'Private event at lunchtime' },
        closed: false,
        ranges: [
          { opens: '07:00', closes: '12:00' },
          { opens: '19:00', closes: '00:30' },
        ],
      },
      { inDays: 33, label: { ar: 'الفناء محجوزٌ لمناسبة', en: 'Courtyard booked for a celebration' }, closed: false, reservationsBlocked: true },
    ],
  },
  {
    slug: 'wadi-hanifah',
    codePrefix: 'WH',
    name: { ar: 'ظل · وادي حنيفة', en: 'Zill · Wadi Hanifah' },
    shortName: { ar: 'وادي حنيفة', en: 'Wadi Hanifah' },
    city: { ar: 'الرياض', en: 'Riyadh' },
    district: { ar: 'وادي حنيفة', en: 'Wadi Hanifah' },
    address: { ar: 'حوش ظل، درب النخل ٢١، وادي حنيفة، الرياض', en: 'Hosh Zill, 21 Palm Track, Wadi Hanifah, Riyadh' },
    story: {
      ar: 'البيت الثاني: حوشٌ من الطين على حافة نخيل الوادي، سقوفه من جذوع النخل، وفي جدرانه فتحاتٌ مثلّثة تُدخل الهواء وتُسقط على الأرض مثلّثاتٍ من الضوء تتحرّك مع الساعة.',
      en: 'The second house: a mud-brick courtyard at the edge of the valley palms, ceilings of palm trunks, and triangular vents in the walls that let the air in and drop triangles of light that move across the floor with the hour.',
    },
    lat: 24.7249,
    lng: 46.585,
    phone: '+966115550190',
    whatsapp: '+966555550190',
    email: 'wadihanifah@zill.test',
    parking: {
      ar: 'مواقف مجانية في ساحة النخيل بجوار البيت، وخدمة صفّ السيارات من السادسة مساءً.',
      en: 'Free parking in the palm-grove yard beside the house; valet from 6 pm.',
    },
    accessibility: {
      ar: 'دخولٌ مستوٍ في الطابق الأرضي كلّه، بما في ذلك الفناء والليوان ودورتا مياه؛ ومنحدرٌ متنقّل للمجلس الخاص.',
      en: 'Level access throughout the ground floor, including the courtyard, the liwan and two restrooms; a portable ramp for the private majlis.',
    },
    heroImage: 'place-mudbrick',
    reservation: {
      slotIntervalMinutes: 15,
      turnTimes: { '1': 75, '3': 95, '5': 120, '7': 150 },
      maxCoversPerSlot: 16,
      maxPartyOnline: 8,
      lastSeatingBeforeCloseMinutes: 45,
    },
    ordering: { maxOrdersPerWindow: 12, windowMinutes: 15, basePrepMinutes: 20, busyExtraMinutes: 15, radiusKm: 10 },
    venueHours: [
      { weekdays: SAT_TO_WED, opens: '07:30', closes: '00:30' },
      { weekdays: [4], opens: '07:30', closes: '01:30' },
      { weekdays: [5], opens: '08:00', closes: '01:30' },
    ],
    kitchenHours: [
      { weekdays: SAT_TO_WED, opens: '08:00', closes: '23:45' },
      { weekdays: [4], opens: '08:00', closes: '00:45' },
      { weekdays: [5], opens: '08:30', closes: '00:45' },
    ],
    periods: PERIODS.map((p) => (p.key === 'breakfast' ? { ...p, start: '07:45' } : p)),
    tables: [
      { label: 'C1', area: 'courtyard', min: 1, max: 2, group: 'courtyard' },
      { label: 'C2', area: 'courtyard', min: 1, max: 2, group: 'courtyard' },
      { label: 'C3', area: 'courtyard', min: 2, max: 4, group: 'courtyard' },
      { label: 'C4', area: 'courtyard', min: 2, max: 4, group: 'courtyard' },
      { label: 'C5', area: 'courtyard', min: 4, max: 6, group: 'courtyard' },
      { label: 'C6', area: 'courtyard', min: 4, max: 8, group: null },
      { label: 'L1', area: 'liwan', min: 1, max: 2, group: 'liwan' },
      { label: 'L2', area: 'liwan', min: 1, max: 2, group: 'liwan' },
      { label: 'L3', area: 'liwan', min: 2, max: 4, group: 'liwan' },
      { label: 'L4', area: 'liwan', min: 2, max: 4, group: 'liwan' },
      { label: 'L5', area: 'liwan', min: 4, max: 6, group: 'liwan' },
      { label: 'P1', area: 'private', min: 6, max: 14, group: null },
    ],
    zones: [
      {
        name: { ar: 'الدرعية وعرقة', en: 'Diriyah & Irqah' },
        kind: 'area',
        areas: [
          { ar: 'الدرعية', en: 'Diriyah' },
          { ar: 'عرقة', en: 'Irqah' },
          { ar: 'الخزامى', en: 'Al-Khuzama' },
        ],
        fee: 10,
        minOrder: 60,
        eta: 35,
      },
      {
        name: { ar: 'حطين والنخيل', en: 'Hittin & An-Nakheel' },
        kind: 'area',
        areas: [
          { ar: 'حطين', en: 'Hittin' },
          { ar: 'النخيل', en: 'An-Nakheel' },
          { ar: 'الرحمانية', en: 'Ar-Rahmaniyah' },
        ],
        fee: 15,
        minOrder: 80,
        eta: 45,
      },
      {
        name: { ar: 'العليا والسليمانية', en: 'Al-Olaya & As-Sulimaniyah' },
        kind: 'area',
        areas: [
          { ar: 'العليا', en: 'Al-Olaya' },
          { ar: 'السليمانية', en: 'As-Sulimaniyah' },
          { ar: 'الورود', en: 'Al-Wurud' },
        ],
        fee: 20,
        minOrder: 100,
        eta: 55,
      },
      { name: { ar: 'ضمن ١٠ كم من البيت', en: 'Within 10 km of the house' }, kind: 'radius', radiusKm: 10, fee: 22, minOrder: 120, eta: 60 },
    ],
    specials: [{ inDays: 25, label: { ar: 'صيانة الفناء', en: 'Courtyard maintenance' }, closed: true }],
  },
];
