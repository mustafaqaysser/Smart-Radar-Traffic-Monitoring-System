import { existsSync } from 'node:fs';

/** Loads .env for standalone scripts (Next loads it automatically for the app). */
export function loadEnv() {
  if (existsSync('.env')) process.loadEnvFile('.env');
}
