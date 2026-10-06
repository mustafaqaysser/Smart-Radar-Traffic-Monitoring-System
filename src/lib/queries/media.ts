import type { media } from '@/lib/db/schema';
import type { MediaDTO } from './types';

type MediaRow = typeof media.$inferSelect;

export function toMediaDTO(row: MediaRow | null | undefined): MediaDTO | null {
  if (!row) return null;
  return {
    id: row.id,
    kind: row.kind,
    src: row.src,
    width: row.width,
    height: row.height,
    blur: row.blurDataUrl,
    focalX: row.focalX,
    focalY: row.focalY,
    alt: row.alt,
    poster: row.poster,
    sources: row.sources,
    credit: row.credit ? { author: row.credit.author, source: row.credit.source, license: row.credit.license } : null,
  };
}

export function mediaMap(rows: MediaRow[]): Map<string, MediaDTO> {
  const map = new Map<string, MediaDTO>();
  for (const row of rows) {
    const dto = toMediaDTO(row);
    if (dto) map.set(row.id, dto);
  }
  return map;
}
