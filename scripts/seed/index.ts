/**
 * npm run db:seed — resets and seeds the database.
 *   DEMO_MODE=true  (default): the full fictional world + 90 days of synthetic history
 *   DEMO_MODE=false: only what a real restaurant needs to start (settings, the owner account)
 * All dates are relative to the moment the seed runs, so the demo never goes stale.
 */
import { sql } from 'drizzle-orm';
import { connect } from '../../src/lib/db/connect';
import * as schema from '../../src/lib/db/schema';
import { loadEnv } from '../db/env';
import { createRandom } from './random';
import { seedWorld } from './world';
import { DEMO_PASSWORD, seedPeople, STAFF } from './people';
import { seedHistory } from './history';
import type { SeedContext } from './types';

loadEnv();
const demo = (process.env.DEMO_MODE ?? 'true') !== 'false';
const { client, db } = connect();
const warnings: string[] = [];
const ctx: SeedContext = {
  db,
  now: new Date(),
  random: createRandom(),
  log: (m) => console.info(`  • ${m}`),
  warn: (m) => {
    if (!warnings.includes(m)) warnings.push(m);
  },
};

const started = Date.now();
await client.execute('PRAGMA foreign_keys = OFF');
const tables = Object.values(schema).filter((v): v is typeof schema.users => typeof v === 'object' && v !== null && Symbol.for('drizzle:IsDrizzleTable') in v);
for (const table of tables) await db.delete(table);
await client.execute('PRAGMA foreign_keys = ON');
await db.run(sql`VACUUM`);
console.info('✓ database cleared');

if (demo) {
  await seedWorld(ctx);
  const people = await seedPeople(ctx);
  await seedHistory(ctx, people);
} else {
  const { hashPassword } = await import('better-auth/crypto');
  const owner = STAFF[0];
  if (owner) {
    await db.insert(schema.users).values({ id: 'us_owner', name: owner.name, email: owner.email, emailVerified: true, role: 'owner', locale: 'en' });
    await db.insert(schema.accounts).values({ id: 'ac_us_owner', accountId: 'us_owner', providerId: 'credential', userId: 'us_owner', password: await hashPassword(DEMO_PASSWORD) });
  }
}

client.close();
console.info(`\n✓ seeded in ${((Date.now() - started) / 1000).toFixed(1)}s${demo ? ' (demo world)' : ''}`);
if (warnings.length) {
  console.warn(`\n⚠ ${warnings.length} warning(s):`);
  for (const w of warnings) console.warn(`  - ${w}`);
}
console.info('\nStaff accounts (password for all: ' + DEMO_PASSWORD + '):');
for (const s of STAFF) console.info(`  ${s.role.padEnd(8)} ${s.email}`);
if (demo) console.info(`  guest    guest@zill.test  (demo customer)`);
