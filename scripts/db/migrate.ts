import { mkdirSync } from 'node:fs';
import { migrate } from 'drizzle-orm/libsql/migrator';
import { connect } from '../../src/lib/db/connect';
import { loadEnv } from './env';

loadEnv();
const url = process.env.DATABASE_URL ?? 'file:./data/zill.db';
if (url.startsWith('file:')) mkdirSync('data', { recursive: true });
const { client, db } = connect(url);
await client.execute('PRAGMA foreign_keys = ON');
await migrate(db, { migrationsFolder: './drizzle' });
client.close();
console.log(`✓ migrations applied (${url.replace(/\/\/.*@/, '//***@')})`);
