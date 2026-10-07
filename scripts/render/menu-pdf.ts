/**
 * Renders every menu of every house, in every language, to an A4 PDF with headless Chromium (so Arabic
 * shapes correctly and the brand fonts embed), and stores them as `menu-pdf/<branch>-<menu>-<locale>.pdf`
 * through the storage service (local folder, or S3/R2 when configured). The site serves them at
 * /menu-pdf/<file>. Requires a running instance: `npm run dev` (or `npm start`) in another terminal.
 *
 *   npm run menu:pdf
 */
import { loadEnv } from '../db/env';
import { connect } from '../../src/lib/db/connect';
import * as s from '../../src/lib/db/schema';
import { getStorage } from '../../src/lib/services/storage';
import restaurantConfig from '../../restaurant.config';
import { assertServerUp, baseUrl, launchBrowser } from '../lib/browser';

loadEnv();
const { client, db } = connect();
const base = baseUrl();
await assertServerUp(`${base}/${restaurantConfig.defaultLocale}`);

const branches = await db.select().from(s.branches);
const menus = (await db.select().from(s.menus)).filter((m) => m.isActive && !m.seasonalModeId);
const storage = getStorage();
const browser = await launchBrowser();
const page = await browser.newPage();
let count = 0;
for (const branch of branches.filter((b) => b.isActive)) {
  for (const menu of menus.filter((m) => !m.branchIds || m.branchIds.includes(branch.id))) {
    for (const locale of restaurantConfig.locales) {
      const url = `${base}/${locale}/menu/print?m=${menu.slug}&branch=${branch.slug}`;
      await page.goto(url, { waitUntil: 'networkidle' });
      await page.evaluate(() => document.fonts.ready);
      const pdf = await page.pdf({ format: 'A4', printBackground: true, preferCSSPageSize: true });
      const file = `${branch.slug}-${menu.slug}-${locale}.pdf`;
      await storage.put(`menu-pdf/${file}`, new Uint8Array(pdf), 'application/pdf');
      count++;
      console.info(`  ✓ ${file} (${Math.round(pdf.length / 1024)} KB)`);
    }
  }
}
await browser.close();
client.close();
console.info(`✓ ${count} menu PDFs rendered with ${storage.name} storage`);
