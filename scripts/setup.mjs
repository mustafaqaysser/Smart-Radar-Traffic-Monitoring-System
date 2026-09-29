/**
 * First-time setup: creates .env from .env.example with generated secrets, applies migrations and seeds data.
 * Cross-platform (plain Node). Usage: npm run setup
 */
import { existsSync, readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { randomBytes } from 'node:crypto';
import { spawnSync } from 'node:child_process';

const secret = () => randomBytes(32).toString('base64');

if (!existsSync('.env')) {
  const template = readFileSync('.env.example', 'utf8');
  const filled = template.replace(/^(BETTER_AUTH_SECRET|CRON_SECRET|ANALYTICS_SALT)=generated$/gm, (_, key) => `${key}=${secret()}`);
  writeFileSync('.env', filled);
  console.info('✓ .env created with generated secrets');
} else {
  const current = readFileSync('.env', 'utf8');
  const patched = current.replace(/^(BETTER_AUTH_SECRET|CRON_SECRET|ANALYTICS_SALT)=generated$/gm, (_, key) => `${key}=${secret()}`);
  if (patched !== current) {
    writeFileSync('.env', patched);
    console.info('✓ .env: filled missing generated secrets');
  } else {
    console.info('• .env already exists — kept as is');
  }
}

mkdirSync('data', { recursive: true });
mkdirSync('storage', { recursive: true });

const npm = process.platform === 'win32' ? 'npm.cmd' : 'npm';
for (const script of ['db:migrate', 'db:seed']) {
  console.info(`\n→ npm run ${script}`);
  const result = spawnSync(npm, ['run', script], { stdio: 'inherit', shell: process.platform === 'win32' });
  if (result.status !== 0) {
    console.error(`✗ ${script} failed`);
    process.exit(result.status ?? 1);
  }
}

console.info('\n✓ Setup complete. Start the app with: npm run dev');
