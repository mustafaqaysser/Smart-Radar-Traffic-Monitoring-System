#!/usr/bin/env node
/**
 * Runs a scheduled job on a running server — for Docker/VPS crontabs or by hand:
 *   npm run cron -- reminders      (hourly: reminder emails, lapsed deposits, waitlist offers)
 *   npm run cron -- gift-cards     (every 15 minutes: scheduled gift card deliveries)
 *   npm run cron -- housekeeping   (daily: expired holds and rate-limit windows)
 * On Vercel the same endpoints are called by Vercel Cron (see vercel.json).
 */
import { existsSync } from 'node:fs';

if (existsSync('.env')) process.loadEnvFile('.env');

const job = process.argv[2] ?? 'reminders';
const base = process.env.CRON_BASE_URL || process.env.NEXT_PUBLIC_SITE_URL || 'http://localhost:3000';
const secret = process.env.CRON_SECRET;
if (!secret) {
  console.error('CRON_SECRET is not set (run `npm run setup` or add it to .env).');
  process.exit(1);
}

try {
  const res = await fetch(new URL(`/api/cron/${encodeURIComponent(job)}`, base), { headers: { authorization: `Bearer ${secret}` } });
  const body = await res.text();
  console.log(`${job}: ${res.status} ${body}`);
  process.exit(res.ok ? 0 : 1);
} catch (error) {
  console.error(`${job}: could not reach ${base} — is the server running? (${error instanceof Error ? error.message : String(error)})`);
  process.exit(1);
}
