import { NextResponse } from 'next/server';
import { getStorage } from '@/lib/services/storage';

/**
 * Public files from local storage (uploaded photographs). Only keys under `public/` are served here;
 * private files (CVs, exports) are only available to signed-in staff through /api/admin/files.
 */
export async function GET(_request: Request, { params }: { params: Promise<{ key: string[] }> }) {
  const { key } = await params;
  const path = key.join('/');
  if (!path.startsWith('public/') || path.includes('..')) return new NextResponse('Not found', { status: 404 });
  const file = await getStorage().get(path);
  if (!file) return new NextResponse('Not found', { status: 404 });
  return new NextResponse(Buffer.from(file.data), {
    headers: {
      'content-type': file.contentType,
      'cache-control': 'public, max-age=31536000, immutable',
      'x-content-type-options': 'nosniff',
    },
  });
}
