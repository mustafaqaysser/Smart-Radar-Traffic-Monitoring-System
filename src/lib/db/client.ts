import 'server-only';
import type { Client } from '@libsql/client';
import type { LibSQLDatabase } from 'drizzle-orm/libsql';
import { connect } from './connect';
import * as schema from './schema';

export type Database = LibSQLDatabase<typeof schema>;

const globalForDb = globalThis as unknown as { __zillDb?: Database; __zillClient?: Client };

function init(): Database {
  const { client, db } = connect();
  globalForDb.__zillClient = client;
  return db;
}

/** The shared Drizzle instance (one per process; survives dev hot reloads). */
export const db: Database = globalForDb.__zillDb ?? (globalForDb.__zillDb = init());

export { schema };
