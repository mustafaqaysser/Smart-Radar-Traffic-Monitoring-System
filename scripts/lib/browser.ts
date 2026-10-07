import { existsSync } from 'node:fs';
import { chromium, type Browser } from '@playwright/test';

/**
 * Headless Chromium for rendering (PDF menus, Open Graph images, screenshots). Uses CHROMIUM_PATH when set,
 * otherwise Playwright's own browser (install once with `npx playwright install chromium`).
 */
export async function launchBrowser(): Promise<Browser> {
  const candidates = [process.env.CHROMIUM_PATH, '/opt/pw-browsers/chromium-1194/chrome-linux/chrome'].filter((p): p is string => Boolean(p));
  const executablePath = candidates.find((p) => existsSync(p));
  return chromium.launch({ executablePath });
}

/** Base URL of a running instance of the site (npm run dev / npm start). */
export function baseUrl(): string {
  return (process.env.RENDER_BASE_URL ?? process.env.NEXT_PUBLIC_SITE_URL ?? 'http://localhost:3000').replace(/\/$/, '');
}

export async function assertServerUp(url: string): Promise<void> {
  try {
    const res = await fetch(url, { redirect: 'manual' });
    if (res.status >= 500) throw new Error(`status ${res.status}`);
  } catch (e) {
    throw new Error(`The site is not reachable at ${url} — start it with "npm run dev" or "npm start" (or set RENDER_BASE_URL). ${(e as Error).message}`);
  }
}
