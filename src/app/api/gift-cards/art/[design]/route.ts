import sharp from 'sharp';
import type { NextRequest } from 'next/server';
import { giftCardSvg, isGiftCardDesign } from '@/lib/brand/gift-card-art';

const pngs = new Map<string, Buffer>();

/** Gift card artwork: SVG for the site, PNG (rendered once per design) for emails, which cannot show SVG. */
export async function GET(request: NextRequest, { params }: { params: Promise<{ design: string }> }) {
  const { design } = await params;
  if (!isGiftCardDesign(design)) return new Response('Not found', { status: 404 });
  const svg = giftCardSvg(design);
  const headers = { 'cache-control': 'public, max-age=86400, stale-while-revalidate=604800' };
  if (request.nextUrl.searchParams.get('format') !== 'png') {
    return new Response(svg, { headers: { ...headers, 'content-type': 'image/svg+xml; charset=utf-8' } });
  }
  let png = pngs.get(design);
  if (!png) {
    png = await sharp(Buffer.from(svg)).png({ compressionLevel: 9 }).toBuffer();
    pngs.set(design, png);
  }
  return new Response(new Uint8Array(png), { headers: { ...headers, 'content-type': 'image/png' } });
}
