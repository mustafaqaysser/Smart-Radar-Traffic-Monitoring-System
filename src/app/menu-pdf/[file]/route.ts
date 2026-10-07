import { NextResponse, type NextRequest } from 'next/server';
import { z } from 'zod';
import { routing } from '@/i18n/routing';
import { getBranches } from '@/lib/queries/branches';
import { getStorage } from '@/lib/services/storage';

const FILE = z.string().regex(/^[a-z0-9-]+-(ar|en)\.pdf$/);

/**
 * Menu PDFs are rendered with headless Chromium by `npm run menu:pdf` and kept in storage
 * (`menu-pdf/<branch>-<menu>-<locale>.pdf`). If a PDF has not been rendered yet, the visitor is sent to the
 * print view, which opens the browser's "Save as PDF" dialog — so the link always leads somewhere useful.
 */
export async function GET(request: NextRequest, { params }: { params: Promise<{ file: string }> }) {
  const { file } = await params;
  const parsed = FILE.safeParse(file);
  if (!parsed.success) return new NextResponse('Not found', { status: 404 });
  const stored = await getStorage().get(`menu-pdf/${parsed.data}`);
  if (stored) {
    return new NextResponse(Buffer.from(stored.data), {
      headers: {
        'content-type': 'application/pdf',
        'content-disposition': `inline; filename="zill-${parsed.data}"`,
        'cache-control': 'public, max-age=3600',
      },
    });
  }
  const locale = parsed.data.endsWith('-ar.pdf') ? 'ar' : 'en';
  const base = parsed.data.replace(/-(ar|en)\.pdf$/, '');
  const branch = (await getBranches()).map((b) => b.slug).find((slug) => base.startsWith(`${slug}-`)) ?? null;
  const menu = branch ? base.slice(branch.length + 1) : base;
  const url = new URL(`/${routing.locales.includes(locale) ? locale : routing.defaultLocale}/menu/print`, request.url);
  url.searchParams.set('m', menu);
  if (branch) url.searchParams.set('branch', branch);
  url.searchParams.set('auto', '1');
  return NextResponse.redirect(url, 307);
}
