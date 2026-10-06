import type { LibSQLDatabase } from 'drizzle-orm/libsql';
import type * as schema from '../../src/lib/db/schema';
import type { Random } from './random';

export interface SeedContext {
  db: LibSQLDatabase<typeof schema>;
  now: Date;
  random: Random;
  log: (message: string) => void;
  warn: (message: string) => void;
}

export type Seeder = (ctx: SeedContext) => Promise<void>;
