import { timingSafeEqual } from 'node:crypto';
import { NextResponse, type NextRequest } from 'next/server';
import { isJobName, runJob } from '@/lib/server/jobs';

export const maxDuration = 60;

function authorised(request: NextRequest): boolean {
  const secret = process.env.CRON_SECRET;
  if (!secret) return false;
  // Vercel Cron sends `Authorization: Bearer <CRON_SECRET>`; `npm run cron` does the same.
  const given = Buffer.from(request.headers.get('authorization') ?? '');
  const expected = Buffer.from(`Bearer ${secret}`);
  return given.length === expected.length && timingSafeEqual(given, expected);
}

/** Scheduled jobs (see src/lib/server/jobs.ts and vercel.json). */
export async function GET(request: NextRequest, { params }: { params: Promise<{ job: string }> }) {
  if (!authorised(request)) return NextResponse.json({ error: 'unauthorized' }, { status: 401 });
  const { job } = await params;
  if (!isJobName(job)) return NextResponse.json({ error: 'notFound' }, { status: 404 });
  const started = Date.now();
  const result = await runJob(job);
  return NextResponse.json({ job, ...result, ms: Date.now() - started }, { headers: { 'cache-control': 'no-store' } });
}
