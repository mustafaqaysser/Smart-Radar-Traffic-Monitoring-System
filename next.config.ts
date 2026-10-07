import type { NextConfig } from 'next';
import createNextIntlPlugin from 'next-intl/plugin';

const withNextIntl = createNextIntlPlugin('./src/i18n/request.ts');

/** Optional public host for S3/R2 uploads (e.g. https://media.example.com). */
const storagePublicUrl = process.env.STORAGE_PUBLIC_URL;
const remotePatterns: NonNullable<NextConfig['images']>['remotePatterns'] = [];
if (storagePublicUrl) {
  const url = new URL(storagePublicUrl);
  remotePatterns.push({ protocol: url.protocol.replace(':', '') as 'https' | 'http', hostname: url.hostname, pathname: `${url.pathname.replace(/\/$/, '')}/**` });
}

// Content-Security-Policy is generated per request (with a nonce) in src/proxy.ts.
const securityHeaders = [
  { key: 'Strict-Transport-Security', value: 'max-age=63072000; includeSubDomains; preload' },
  { key: 'X-Content-Type-Options', value: 'nosniff' },
  { key: 'X-Frame-Options', value: 'DENY' },
  { key: 'Referrer-Policy', value: 'strict-origin-when-cross-origin' },
  { key: 'Cross-Origin-Opener-Policy', value: 'same-origin-allow-popups' },
  {
    key: 'Permissions-Policy',
    value: 'camera=(), microphone=(), geolocation=(self), payment=(self "https://js.stripe.com"), usb=(), browsing-topics=()',
  },
];

const nextConfig: NextConfig = {
  poweredByHeader: false,
  reactStrictMode: true,
  serverExternalPackages: ['@libsql/client', 'libsql', 'sharp'],
  images: {
    formats: ['image/avif', 'image/webp'],
    qualities: [55, 70, 82],
    deviceSizes: [360, 480, 640, 768, 960, 1200, 1440, 1920, 2560],
    imageSizes: [48, 96, 160, 240, 320],
    minimumCacheTTL: 2678400,
    localPatterns: [
      { pathname: '/media/**', search: '' },
      { pathname: '/files/**', search: '' },
      { pathname: '/brand/**', search: '' },
    ],
    remotePatterns,
  },
  experimental: {
    optimizePackageImports: ['gsap', 'motion', 'lucide-react', 'recharts'],
    // CV uploads (up to 5 MB) are sent through server actions; leave room for multipart overhead.
    serverActions: { bodySizeLimit: '6mb' },
  },
  async headers() {
    return [
      { source: '/:path*', headers: securityHeaders },
      {
        source: '/media/:path*',
        headers: [{ key: 'Cache-Control', value: 'public, max-age=31536000, immutable' }],
      },
      {
        source: '/fonts/:path*',
        headers: [{ key: 'Cache-Control', value: 'public, max-age=31536000, immutable' }],
      },
      {
        source: '/sw.js',
        headers: [
          { key: 'Cache-Control', value: 'no-cache, no-store, must-revalidate' },
          { key: 'Service-Worker-Allowed', value: '/' },
        ],
      },
    ];
  },
};

export default withNextIntl(nextConfig);
