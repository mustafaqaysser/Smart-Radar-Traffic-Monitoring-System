/**
 * Types for the seed world. Every text is written natively in both languages (never translated word for
 * word). Dates are relative to the day the seed runs, so the demo never goes stale.
 */

export interface L {
  ar: string;
  en: string;
}

export type BranchSlug = 'al-balad' | 'wadi-hanifah';

/** A media name from media/manifest (e.g. 'place-palm-shadow-1', 'craft-kneading', 'dish-kunafa-nabulsi'). */
export type MediaName = string;

export interface SeedJournalPost {
  slug: string;
  kind: 'article' | 'recipe';
  title: L;
  excerpt: L;
  /** Clean HTML: <p>, <h2>, <h3>, <blockquote>, <ul>/<ol>/<li>, <strong>, <em>. No inline styles, no links. */
  body: L;
  author: L;
  readingMinutes: number;
  cover: MediaName;
  publishedDaysAgo: number;
  recipe?: {
    serves: number;
    minutes: number;
    ingredients: L[];
    steps: L[];
  };
}

export interface SeedFaq {
  category: 'visiting' | 'reservations' | 'menu' | 'ordering' | 'events' | 'gift-cards' | 'loyalty' | 'accessibility';
  question: L;
  answer: L;
}

export interface SeedTeamMember {
  name: L;
  role: L;
  bio: L;
  image: MediaName;
  branch: BranchSlug | null;
}

export interface SeedReview {
  name: string;
  rating: 1 | 2 | 3 | 4 | 5;
  title?: string;
  body: string;
  locale: 'ar' | 'en';
  branch: BranchSlug;
  daysAgo: number;
  status: 'published' | 'pending' | 'rejected';
  featured?: boolean;
  response?: L;
}

export interface SeedPress {
  kind: 'press' | 'award';
  publication: L;
  title: L;
  quote: L;
  year: number;
}

export interface SeedJob {
  slug: string;
  title: L;
  branch: BranchSlug | null;
  employment: 'full_time' | 'part_time' | 'seasonal';
  summary: L;
  /** Clean HTML as for journal bodies. */
  description: L;
}

export interface SeedEvent {
  slug: string;
  kind: 'chefs_table' | 'tasting' | 'class' | 'gathering';
  title: L;
  summary: L;
  /** Clean HTML. */
  body: L;
  branch: BranchSlug;
  daysFromNow: number;
  /** Branch-local start time 'HH:mm'. */
  startTime: string;
  durationMinutes: number;
  capacity: number;
  image: MediaName;
  tickets: { name: L; description?: L; price: number; capacity?: number }[];
}

export interface SeedPrivateRoom {
  slug: string;
  branch: BranchSlug;
  name: L;
  description: L;
  seated: number;
  standing: number | null;
  features: L[];
  image: MediaName;
}

export interface SeedPackage {
  kind: 'private_dining' | 'catering';
  name: L;
  description: L;
  /** Minor units (halalas). */
  pricePerGuest: number;
  minGuests: number;
}

export interface SeedSeasonalMode {
  slug: string;
  kind: 'ramadan' | 'eid' | 'new_year' | 'national_day' | 'custom';
  name: L;
  banner: L;
  /** Relative to seed day; inclusive. */
  startInDays: number;
  endInDays: number;
  theme: 'ramadan' | 'eid' | 'new_year' | 'none';
  enabled: boolean;
}

export interface SeedLoyaltyReward {
  name: L;
  description: L;
  pointsCost: number;
  /** Minor units. */
  value: number;
  minTier: string | null;
}

export interface SeedPromotion {
  code: string;
  description: L;
  kind: 'percent' | 'fixed';
  value: number;
  maxDiscount: number | null;
  minOrder: number;
  startsInDays: number | null;
  endsInDays: number | null;
  usageLimit: number | null;
  perCustomerLimit: number | null;
  firstOrderOnly: boolean;
  branches: BranchSlug[] | null;
  channels: ('delivery' | 'pickup' | 'dine_in')[] | null;
  active: boolean;
}
