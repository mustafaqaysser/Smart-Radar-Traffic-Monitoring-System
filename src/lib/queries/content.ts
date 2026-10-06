import 'server-only';
import { and, asc, desc, eq, gte, inArray, isNotNull, lte, ne, sql } from 'drizzle-orm';
import { cache } from 'react';
import { db } from '@/lib/db/client';
import * as s from '@/lib/db/schema';
import type { LocalizedText } from '@/lib/i18n/localized';
import { cached, TAGS } from './cache';
import { mediaMap, toMediaDTO } from './media';
import type { MediaDTO } from './types';

// ——— DTOs ———

export interface FaqDTO {
  id: string;
  category: string;
  question: LocalizedText;
  answer: LocalizedText;
}

export interface TeamMemberDTO {
  id: string;
  name: LocalizedText;
  role: LocalizedText;
  bio: LocalizedText;
  image: MediaDTO | null;
  branchId: string | null;
}

export interface PressDTO {
  id: string;
  kind: 'press' | 'award';
  publication: LocalizedText;
  title: LocalizedText;
  quote: LocalizedText;
  year: number;
}

export interface ReviewDTO {
  id: string;
  branchId: string | null;
  name: string;
  rating: number;
  title: string | null;
  body: string;
  locale: string;
  visitDate: string | null;
  response: LocalizedText | null;
  featured: boolean;
  createdAt: string;
}

export interface JournalSummaryDTO {
  id: string;
  slug: string;
  kind: 'article' | 'recipe';
  title: LocalizedText;
  excerpt: LocalizedText;
  author: LocalizedText;
  readingMinutes: number;
  cover: MediaDTO | null;
  publishedAt: string | null;
}

export interface JournalPostDTO extends JournalSummaryDTO {
  body: LocalizedText;
  recipe: s.RecipeData | null;
  updatedAt: string;
}

export interface GalleryItemDTO {
  id: string;
  media: MediaDTO;
  caption: LocalizedText | null;
  hour: string | null;
}

export interface TicketTypeDTO {
  id: string;
  name: LocalizedText;
  description: LocalizedText | null;
  price: number;
  capacity: number | null;
  sold: number;
}

export interface EventDTO {
  id: string;
  slug: string;
  kind: 'chefs_table' | 'tasting' | 'class' | 'gathering';
  title: LocalizedText;
  summary: LocalizedText;
  body: LocalizedText | null;
  branchId: string;
  startsAt: string;
  endsAt: string;
  capacity: number;
  seatsTaken: number;
  image: MediaDTO | null;
  tickets: TicketTypeDTO[];
}

export interface PrivateRoomDTO {
  id: string;
  slug: string;
  branchId: string;
  name: LocalizedText;
  description: LocalizedText;
  seated: number;
  standing: number | null;
  features: LocalizedText[];
  image: MediaDTO | null;
}

export interface PrivatePackageDTO {
  id: string;
  name: LocalizedText;
  description: LocalizedText;
  pricePerGuest: number;
  minGuests: number;
  kind: 'private_dining' | 'catering';
}

export interface JobDTO {
  id: string;
  slug: string;
  title: LocalizedText;
  branchId: string | null;
  employment: 'full_time' | 'part_time' | 'seasonal';
  summary: LocalizedText;
  description: LocalizedText;
}

export interface LoyaltyRewardDTO {
  id: string;
  name: LocalizedText;
  description: LocalizedText | null;
  pointsCost: number;
  value: number;
  minTier: string | null;
}

export type ContentBlock = Record<string, LocalizedText | string | number | boolean | null>;

// ——— queries ———

export const getFaqs = cached(
  async (): Promise<FaqDTO[]> => (await db.select().from(s.faqs).orderBy(asc(s.faqs.sortOrder))).map(({ id, category, question, answer }) => ({ id, category, question, answer })),
  'faqs',
  [TAGS.content],
);

export const getTeam = cached(
  async (): Promise<TeamMemberDTO[]> => {
    const [rows, mediaRows] = await Promise.all([db.select().from(s.teamMembers).orderBy(asc(s.teamMembers.sortOrder)), db.select().from(s.media)]);
    const m = mediaMap(mediaRows);
    return rows.map((r) => ({ id: r.id, name: r.name, role: r.role, bio: r.bio, image: r.imageId ? (m.get(r.imageId) ?? null) : null, branchId: r.branchId }));
  },
  'team',
  [TAGS.content],
);

export const getPress = cached(
  async (): Promise<PressDTO[]> => (await db.select().from(s.pressItems).orderBy(asc(s.pressItems.sortOrder))).map(({ id, kind, publication, title, quote, year }) => ({ id, kind, publication, title, quote, year })),
  'press',
  [TAGS.content],
);

function toReview(r: typeof s.reviews.$inferSelect): ReviewDTO {
  return {
    id: r.id,
    branchId: r.branchId,
    name: r.name,
    rating: r.rating,
    title: r.title,
    body: r.body,
    locale: r.locale,
    visitDate: r.visitDate,
    response: r.response,
    featured: r.featured,
    createdAt: r.createdAt.toISOString(),
  };
}

export const getPublishedReviews = cached(
  async (): Promise<ReviewDTO[]> => (await db.select().from(s.reviews).where(eq(s.reviews.status, 'published')).orderBy(desc(s.reviews.createdAt))).map(toReview),
  'reviews-published',
  [TAGS.reviews],
);

export const getReviewStats = cached(
  async (): Promise<{ count: number; average: number }> => {
    const [row] = await db
      .select({ count: sql<number>`count(*)`, average: sql<number>`coalesce(avg(${s.reviews.rating}), 0)` })
      .from(s.reviews)
      .where(eq(s.reviews.status, 'published'));
    return { count: Number(row?.count ?? 0), average: Number(row?.average ?? 0) };
  },
  'reviews-stats',
  [TAGS.reviews],
);

function toJournalSummary(r: typeof s.journalPosts.$inferSelect, m: Map<string, MediaDTO>): JournalSummaryDTO {
  return {
    id: r.id,
    slug: r.slug,
    kind: r.kind,
    title: r.title,
    excerpt: r.excerpt,
    author: r.author,
    readingMinutes: r.readingMinutes,
    cover: r.coverImageId ? (m.get(r.coverImageId) ?? null) : null,
    publishedAt: r.publishedAt ? r.publishedAt.toISOString() : null,
  };
}

const publishedNow = () => and(eq(s.journalPosts.isPublished, true), isNotNull(s.journalPosts.publishedAt), lte(s.journalPosts.publishedAt, new Date()));

export const getJournal = cached(
  async (): Promise<JournalSummaryDTO[]> => {
    const [rows, mediaRows] = await Promise.all([db.select().from(s.journalPosts).where(publishedNow()).orderBy(desc(s.journalPosts.publishedAt)), db.select().from(s.media)]);
    const m = mediaMap(mediaRows);
    return rows.map((r) => toJournalSummary(r, m));
  },
  'journal',
  [TAGS.content],
  600,
);

export const getJournalPost = cache(async (slug: string): Promise<JournalPostDTO | null> => {
  const [row] = await db.select().from(s.journalPosts).where(and(eq(s.journalPosts.slug, slug), publishedNow())).limit(1);
  if (!row) return null;
  const cover = row.coverImageId ? (await db.select().from(s.media).where(eq(s.media.id, row.coverImageId)).limit(1))[0] : null;
  const m = new Map<string, MediaDTO>();
  const dto = toMediaDTO(cover);
  if (dto && row.coverImageId) m.set(row.coverImageId, dto);
  return { ...toJournalSummary(row, m), body: row.body, recipe: row.recipe, updatedAt: row.updatedAt.toISOString() };
});

export const getGallery = cached(
  async (): Promise<GalleryItemDTO[]> => {
    const [rows, mediaRows] = await Promise.all([db.select().from(s.galleryItems).orderBy(asc(s.galleryItems.sortOrder)), db.select().from(s.media)]);
    const m = mediaMap(mediaRows);
    return rows.flatMap((r) => {
      const media = m.get(r.mediaId);
      return media ? [{ id: r.id, media, caption: r.caption, hour: r.hour }] : [];
    });
  },
  'gallery',
  [TAGS.content],
);

async function loadEvents(fromIso: string): Promise<EventDTO[]> {
  const from = new Date(fromIso);
  const rows = await db
    .select()
    .from(s.events)
    .where(and(eq(s.events.isPublished, true), gte(s.events.endsAt, from)))
    .orderBy(asc(s.events.startsAt));
  if (rows.length === 0) return [];
  const eventIds = rows.map((r) => r.id);
  const [tickets, sold, mediaRows] = await Promise.all([
    db.select().from(s.eventTicketTypes).where(inArray(s.eventTicketTypes.eventId, eventIds)).orderBy(asc(s.eventTicketTypes.sortOrder)),
    db
      .select({ ticketTypeId: s.eventBookings.ticketTypeId, eventId: s.eventBookings.eventId, qty: sql<number>`sum(${s.eventBookings.quantity})` })
      .from(s.eventBookings)
      .where(and(inArray(s.eventBookings.eventId, eventIds), ne(s.eventBookings.status, 'cancelled')))
      .groupBy(s.eventBookings.ticketTypeId, s.eventBookings.eventId),
    db.select().from(s.media),
  ]);
  const m = mediaMap(mediaRows);
  return rows.map((e) => ({
    id: e.id,
    slug: e.slug,
    kind: e.kind,
    title: e.title,
    summary: e.summary,
    body: e.body,
    branchId: e.branchId,
    startsAt: e.startsAt.toISOString(),
    endsAt: e.endsAt.toISOString(),
    capacity: e.capacity,
    seatsTaken: sold.filter((x) => x.eventId === e.id).reduce((n, x) => n + Number(x.qty), 0),
    image: e.imageId ? (m.get(e.imageId) ?? null) : null,
    tickets: tickets
      .filter((t) => t.eventId === e.id)
      .map((t) => ({
        id: t.id,
        name: t.name,
        description: t.description,
        price: t.price,
        capacity: t.capacity,
        sold: Number(sold.find((x) => x.ticketTypeId === t.id)?.qty ?? 0),
      })),
  }));
}

const getEventsCached = cached(loadEvents, 'events-upcoming', [TAGS.events], 60);

/** Upcoming (and in-progress) published events. Keyed by the hour so the cache key changes over time. */
export function getUpcomingEvents(): Promise<EventDTO[]> {
  const hour = new Date();
  hour.setUTCMinutes(0, 0, 0);
  return getEventsCached(hour.toISOString());
}

export const getEvent = cache(async (slug: string): Promise<EventDTO | null> => {
  const all = await getUpcomingEvents();
  return all.find((e) => e.slug === slug) ?? null;
});

export const getPrivateDining = cached(
  async (): Promise<{ rooms: PrivateRoomDTO[]; packages: PrivatePackageDTO[] }> => {
    const [rooms, packages, mediaRows] = await Promise.all([
      db.select().from(s.privateRooms).orderBy(asc(s.privateRooms.sortOrder)),
      db.select().from(s.privatePackages).orderBy(asc(s.privatePackages.sortOrder)),
      db.select().from(s.media),
    ]);
    const m = mediaMap(mediaRows);
    return {
      rooms: rooms.map((r) => ({
        id: r.id,
        slug: r.slug,
        branchId: r.branchId,
        name: r.name,
        description: r.description,
        seated: r.seated,
        standing: r.standing,
        features: r.features ?? [],
        image: r.imageId ? (m.get(r.imageId) ?? null) : null,
      })),
      packages: packages.map(({ id, name, description, pricePerGuest, minGuests, kind }) => ({ id, name, description, pricePerGuest, minGuests, kind })),
    };
  },
  'private-dining',
  [TAGS.content],
);

export const getOpenJobs = cached(
  async (): Promise<JobDTO[]> =>
    (await db.select().from(s.jobPostings).where(eq(s.jobPostings.isOpen, true)).orderBy(desc(s.jobPostings.createdAt))).map(({ id, slug, title, branchId, employment, summary, description }) => ({
      id,
      slug,
      title,
      branchId,
      employment,
      summary,
      description,
    })),
  'jobs',
  [TAGS.content],
);

export const getLoyaltyRewards = cached(
  async (): Promise<LoyaltyRewardDTO[]> =>
    (await db.select().from(s.loyaltyRewards).where(eq(s.loyaltyRewards.isActive, true)).orderBy(asc(s.loyaltyRewards.sortOrder))).map(({ id, name, description, pointsCost, value, minTier }) => ({
      id,
      name,
      description,
      pointsCost,
      value,
      minTier,
    })),
  'loyalty-rewards',
  [TAGS.content],
);

const getAllBlocks = cached(
  async (): Promise<Record<string, Record<string, ContentBlock>>> => {
    const rows = await db.select().from(s.contentBlocks);
    const out: Record<string, Record<string, ContentBlock>> = {};
    for (const r of rows) (out[r.page] ??= {})[r.key] = r.data;
    return out;
  },
  'content-blocks',
  [TAGS.content],
);

/** Editable copy for a page, keyed by section. */
export async function getContentBlocks(page: string): Promise<Record<string, ContentBlock>> {
  return (await getAllBlocks())[page] ?? {};
}
