import createMiddleware from 'next-intl/middleware';
import { NextRequest, NextResponse } from 'next/server';
import { routing } from './i18n/routing';
import { preferredLocale } from './lib/i18n/negotiate';

const handleI18nRouting = createMiddleware(routing);

/**
 * Builds a strict, nonce-based Content-Security-Policy that allows only the origins the platform uses.
 * Third-party origins are added only when the matching service is configured.
 */
function buildCsp(nonce: string): string {
  const isDev = process.env.NODE_ENV !== 'production';
  const stripe = Boolean(process.env.STRIPE_SECRET_KEY && process.env.NEXT_PUBLIC_STRIPE_PUBLISHABLE_KEY);
  const turnstile = Boolean(process.env.TURNSTILE_SECRET_KEY && process.env.NEXT_PUBLIC_TURNSTILE_SITE_KEY);
  const storage = process.env.STORAGE_PUBLIC_URL ? new URL(process.env.STORAGE_PUBLIC_URL).origin : '';
  const tiles = 'https://tiles.openfreemap.org';

  const script = [`'self'`, `'nonce-${nonce}'`, `'strict-dynamic'`];
  if (isDev) script.push(`'unsafe-eval'`);
  if (stripe) script.push('https://js.stripe.com');
  if (turnstile) script.push('https://challenges.cloudflare.com');

  const connect = [`'self'`, tiles];
  if (stripe) connect.push('https://api.stripe.com', 'https://merchant-ui-api.stripe.com');
  if (isDev) connect.push('ws:', 'wss:');

  const frame: string[] = [];
  if (stripe) frame.push('https://js.stripe.com', 'https://hooks.stripe.com');
  if (turnstile) frame.push('https://challenges.cloudflare.com');

  const img = [`'self'`, 'data:', 'blob:', tiles];
  if (storage) img.push(storage);
  if (stripe) img.push('https://*.stripe.com');

  const directives: Record<string, string[]> = {
    'default-src': [`'self'`],
    'script-src': script,
    'style-src': [`'self'`, `'unsafe-inline'`],
    'img-src': img,
    'font-src': [`'self'`, 'data:'],
    'connect-src': connect,
    'media-src': [`'self'`, 'blob:', ...(storage ? [storage] : [])],
    'worker-src': [`'self'`, 'blob:'],
    'frame-src': frame.length ? frame : [`'none'`],
    'object-src': [`'none'`],
    'base-uri': [`'self'`],
    'form-action': [`'self'`],
    'frame-ancestors': [`'none'`],
    'manifest-src': [`'self'`],
  };
  const policy = Object.entries(directives).map(([key, values]) => `${key} ${values.join(' ')}`);
  if (!isDev) policy.push('upgrade-insecure-requests');
  return policy.join('; ');
}

export default function proxy(request: NextRequest) {
  const { pathname } = request.nextUrl;
  // A table's QR code (/t/CODE) opens in the language of the guest's phone; the rest of the site defaults to Arabic.
  if (/^\/t\/[^/]+\/?$/.test(pathname)) {
    const url = request.nextUrl.clone();
    url.pathname = `/${preferredLocale(request.headers.get('accept-language'), routing.locales, routing.defaultLocale)}${pathname.replace(/\/$/, '')}`;
    return NextResponse.redirect(url);
  }

  const nonce = btoa(crypto.randomUUID());
  const csp = buildCsp(nonce);

  const requestHeaders = new Headers(request.headers);
  requestHeaders.set('x-nonce', nonce);
  requestHeaders.set('content-security-policy', csp);

  const response = pathname === '/admin' || pathname.startsWith('/admin/')
    ? NextResponse.next({ request: { headers: requestHeaders } })
    : handleI18nRouting(new NextRequest(request, { headers: requestHeaders }));

  response.headers.set('content-security-policy', csp);
  return response;
}

export const config = {
  // Everything except API routes, Next internals, static files and generated metadata routes.
  matcher: ['/((?!api|_next|_vercel|files|media|fonts|brand|og|menu-pdf|sw\\.js|manifest\\.webmanifest|robots\\.txt|sitemap\\.xml|llms\\.txt|icon|apple-icon|.*\\..*).*)'],
};
