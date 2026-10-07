/** URL-safe random identifiers and human-friendly codes (Web Crypto; works in Node and edge runtimes). */

const ALPHABET = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
// No 0/O, 1/I/L: easy to read aloud and type from a phone.
const CODE_ALPHABET = '23456789ABCDEFGHJKMNPQRSTUVWXYZ';

function randomString(length: number, alphabet: string): string {
  const bytes = new Uint8Array(length);
  crypto.getRandomValues(bytes);
  let out = '';
  // Rejection-free modulo is fine here: 256 % 62 bias is negligible for ids, and codes use 31 symbols over 248.
  const limit = 256 - (256 % alphabet.length);
  let i = 0;
  while (out.length < length) {
    const b = bytes[i++];
    if (b === undefined) {
      crypto.getRandomValues(bytes);
      i = 0;
      continue;
    }
    if (b < limit) out += alphabet[b % alphabet.length];
  }
  return out;
}

export function createId(): string {
  return randomString(21, ALPHABET);
}

/** e.g. 'ZL-7K3M9Q' — reservations, tickets. */
/** Public code prefixes: bookings read ZL-…, gathering tickets ZT-… (checked by the lookup routes). */
export const CODE_PREFIX = { reservation: 'ZL', ticket: 'ZT' } as const;

export function createCode(prefix: string, length = 6): string {
  return `${prefix}-${randomString(length, CODE_ALPHABET)}`;
}

/** e.g. 'ZILL-4H7Q-M2PK-9XRA' — gift cards. */
export function createGiftCardCode(): string {
  return ['ZILL', randomString(4, CODE_ALPHABET), randomString(4, CODE_ALPHABET), randomString(4, CODE_ALPHABET)].join('-');
}

/** A secret, URL-safe token (e.g. manage links, tracking links). */
export function createToken(bytes = 24): string {
  const buf = new Uint8Array(bytes);
  crypto.getRandomValues(buf);
  return btoa(String.fromCharCode(...buf)).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

export async function sha256(value: string): Promise<string> {
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(value));
  return Array.from(new Uint8Array(digest), (b) => b.toString(16).padStart(2, '0')).join('');
}

/** Constant-time string comparison for secrets. */
export function safeEqual(a: string, b: string): boolean {
  if (a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++) diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return diff === 0;
}
