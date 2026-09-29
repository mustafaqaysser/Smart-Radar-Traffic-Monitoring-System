import { fileURLToPath } from 'node:url';
import { defineConfig } from 'vitest/config';

export default defineConfig({
  resolve: {
    alias: {
      '@config': fileURLToPath(new URL('./restaurant.config.ts', import.meta.url)),
      '@': fileURLToPath(new URL('./src', import.meta.url)),
      'server-only': fileURLToPath(new URL('./tests/unit/support/server-only.ts', import.meta.url)),
    },
  },
  test: {
    include: ['tests/unit/**/*.test.ts'],
    environment: 'node',
    globals: false,
    pool: 'threads',
    reporters: ['default'],
  },
});
