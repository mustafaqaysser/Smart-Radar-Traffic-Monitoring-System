import { createClient } from '@libsql/client';
import { drizzle } from 'drizzle-orm/libsql';
import * as schema from './schema';

/** Creates a Drizzle instance for scripts and tests (the app uses the shared instance in ./client). */
export function connect(url = process.env.DATABASE_URL ?? 'file:./data/zill.db', authToken = process.env.DATABASE_AUTH_TOKEN) {
  const client = createClient({ url, authToken: authToken || undefined });
  return { client, db: drizzle(client, { schema }) };
}
