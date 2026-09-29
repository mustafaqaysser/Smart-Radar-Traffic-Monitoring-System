import { existsSync, rmSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { loadEnv } from './env';

loadEnv();
const url = process.env.DATABASE_URL ?? 'file:./data/zill.db';
if (!url.startsWith('file:')) {
  console.error('db:reset only deletes local SQLite files. For Turso, drop and recreate the database, then run db:migrate and db:seed.');
  process.exit(1);
}
const file = url.replace(/^file:/, '');
for (const suffix of ['', '-journal', '-wal', '-shm']) {
  if (existsSync(file + suffix)) rmSync(file + suffix);
}
console.log(`✓ removed ${file}`);
const npm = process.platform === 'win32' ? 'npm.cmd' : 'npm';
for (const script of ['db:migrate', 'db:seed']) {
  const r = spawnSync(npm, ['run', script], { stdio: 'inherit', shell: process.platform === 'win32' });
  if (r.status !== 0) process.exit(r.status ?? 1);
}
