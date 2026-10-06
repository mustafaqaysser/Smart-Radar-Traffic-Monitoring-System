import type { ContentBlock } from '@/lib/queries/content';

/** A localised string from an editable content block, or null when the editor has not set one. */
export function blockText(block: ContentBlock | undefined, field: string, locale: string): string | null {
  const value = block?.[field];
  if (!value) return null;
  if (typeof value === 'string') return value.trim() || null;
  if (typeof value === 'object') {
    const text = (value as Record<string, string | undefined>)[locale];
    return text && text.trim() ? text : null;
  }
  return null;
}
