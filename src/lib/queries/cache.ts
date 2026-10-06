import 'server-only';
import { unstable_cache } from 'next/cache';

/**
 * Cache tags. Public reads are cached across requests and invalidated by tag from admin mutations
 * (`updateTag()` inside server actions for read-your-own-writes, `revalidateTag(tag, 'max')` elsewhere).
 */
export const TAGS = {
  catalog: 'catalog',
  branches: 'branches',
  content: 'content',
  events: 'events',
  seasonal: 'seasonal',
  reviews: 'reviews',
  settings: 'settings',
} as const;

export type CacheTag = (typeof TAGS)[keyof typeof TAGS];

/**
 * Cross-request cache for a query. Results are JSON-serialised, so queries return plain DTOs
 * (instants as ISO strings, never Date objects).
 */
export function cached<Args extends unknown[], R>(fn: (...args: Args) => Promise<R>, key: string, tags: CacheTag[], revalidate = 3600) {
  return unstable_cache(fn, [key], { tags, revalidate });
}
