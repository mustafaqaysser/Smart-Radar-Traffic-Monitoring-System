/**
 * restaurant.config.ts — the white-label heart of the platform.
 *
 * To re-skin the platform for another restaurant, change this file, the design tokens in
 * `src/styles/tokens.css`, the fonts in `src/lib/fonts.ts` and the logo in `src/components/brand/`.
 * Operational data (branches, hours, menus, tables, zones, content) is edited in the admin.
 * See docs/REBRAND.md for the step-by-step guide.
 *
 * Feature flags here are defaults; owners can override them at runtime in Admin → Settings → Features.
 */
import { defineRestaurantConfig } from './src/lib/config/define';

const restaurantConfig = defineRestaurantConfig({
  id: 'zill',
  name: { ar: 'ظل', en: 'Zill' },
  legalName: { ar: 'ظل للضيافة', en: 'Zill Hospitality' },
  tagline: { ar: 'لكلّ ساعةٍ ظلُّها', en: 'Every hour has its shade.' },
  description: {
    ar: 'مطبخ الجزيرة العربية في فناءٍ يتبع الظلّ من قهوة الصباح إلى منتصف الليل — في جدة التاريخية ووادي حنيفة بالرياض.',
    en: 'Cooking of the Arabian Peninsula in a courtyard that follows the shade from first coffee to midnight — in historic Jeddah and Wadi Hanifah, Riyadh.',
  },
  cuisine: { ar: 'مطبخ الجزيرة العربية', en: 'Arabian Peninsula' },
  priceRange: '$$$',

  locales: ['ar', 'en'],
  defaultLocale: 'ar',
  // Arabic-Indic digits in Arabic by default. Set `ar: 'latn'` to use Western digits everywhere.
  numerals: { ar: 'arab', en: 'latn' },
  currency: 'SAR',
  country: 'SA',
  defaultTimeZone: 'Asia/Riyadh',
  flagshipBranchSlug: 'al-balad',

  contact: {
    email: 'hello@zill.test',
    press: 'press@zill.test',
    careers: 'careers@zill.test',
    events: 'gatherings@zill.test',
    privacy: 'privacy@zill.test',
  },

  features: {
    reservations: true,
    ordering: true,
    delivery: true,
    pickup: true,
    dineInQr: true,
    events: true,
    giftCards: true,
    loyalty: true,
    journal: true,
    careers: true,
    reviews: true,
    privateDining: true,
    newsletter: true,
    // Also requires ANTHROPIC_API_KEY in the environment.
    aiConcierge: false,
    // Platform-level switch for markets that serve alcohol. Off: the beverage programme is alcohol-free.
    alcohol: false,
    ambientSound: true,
    preloader: true,
    webgl: true,
    analytics: true,
    seasonalModes: true,
  },

  tax: {
    name: { ar: 'ضريبة القيمة المضافة', en: 'VAT' },
    rate: 0.15,
    pricesIncludeTax: true,
    registrationNumber: '300000000000003',
  },
  // Disclosed on the dine-in menu; not applied to delivery or pickup.
  serviceCharge: { rate: 0.1, channels: ['dine_in'] },
  tips: { enabled: true, presets: [0, 0.05, 0.1, 0.15], channels: ['delivery', 'pickup', 'dine_in'] },

  loyalty: {
    pointsPerUnit: 1,
    pointsPerUnitRedeemed: 20,
    maxRedeemShare: 0.5,
    signupBonus: 100,
    tiers: [
      { id: 'morning', name: { ar: 'ظلّ الصباح', en: 'Morning Shade' }, minPoints: 0, multiplier: 1 },
      { id: 'long-shade', name: { ar: 'الظلّ الطويل', en: 'Long Shade' }, minPoints: 1500, multiplier: 1.25 },
      { id: 'night', name: { ar: 'فناء الليل', en: 'Night Courtyard' }, minPoints: 5000, multiplier: 1.5 },
    ],
  },

  giftCards: { min: 100, max: 5000, presets: [200, 350, 500, 1000], designs: ['dawn', 'noon', 'long-shade', 'night'], validityMonths: 24 },

  reservations: {
    maxPartyOnline: 8,
    privateDiningThreshold: 9,
    holdMinutes: 7,
    modifyCutoffHours: 4,
    deposit: { minParty: 6, perGuest: 100 },
    reminderHoursBefore: 24,
    bookingWindowDays: 60,
  },

  ordering: { asapBufferMinutes: 10, scheduleDaysAhead: 3, slotMinutes: 15 },

  seo: {
    titleTemplate: { ar: '%s — ظل', en: '%s — Zill' },
    defaultOgImage: '/og/default',
  },

  // Only first-party, cookieless analytics are used, so no consent banner is needed by default.
  nonEssentialCookies: false,
});

export default restaurantConfig;
export type AppConfig = typeof restaurantConfig;
