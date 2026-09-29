import { defineConfig } from 'drizzle-kit';

const url = process.env.DATABASE_URL ?? 'file:./data/zill.db';
const authToken = process.env.DATABASE_AUTH_TOKEN;

export default defineConfig({
  schema: './src/lib/db/schema.ts',
  out: './drizzle',
  dialect: url.startsWith('libsql:') || url.startsWith('https:') ? 'turso' : 'sqlite',
  dbCredentials: authToken ? { url, authToken } : { url },
  strict: true,
  verbose: false,
});
