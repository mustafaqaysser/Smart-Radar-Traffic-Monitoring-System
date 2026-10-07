import 'server-only';
import sanitizeHtml from 'sanitize-html';

/** Rich text from the admin editor: structural tags only, no attributes except safe links. */
export function sanitizeRichText(html: string): string {
  return sanitizeHtml(html, {
    allowedTags: ['p', 'h2', 'h3', 'blockquote', 'ul', 'ol', 'li', 'strong', 'em', 'a', 'br'],
    allowedAttributes: { a: ['href', 'title'] },
    allowedSchemes: ['https', 'mailto', 'tel'],
    allowProtocolRelative: false,
    transformTags: {
      a: (tagName, attribs) => ({ tagName, attribs: { ...attribs, rel: 'noopener noreferrer', ...(attribs.href?.startsWith('https://') ? { target: '_blank' } : {}) } }),
    },
  });
}
