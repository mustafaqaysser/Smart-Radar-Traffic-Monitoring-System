/**
 * Copies browser files that must be served as-is (not bundled) into public/vendor. Runs after npm install.
 * MapLibre loads its web worker relative to its own module URL, which no longer exists once bundled, so the
 * worker and the shared chunk it imports are served from /vendor/maplibre/ (see src/components/site/house-map.tsx).
 */
import { copyFileSync, existsSync, mkdirSync } from 'node:fs';
import { join } from 'node:path';

const files = [
  ['node_modules/maplibre-gl/dist/maplibre-gl-worker.mjs', 'public/vendor/maplibre/maplibre-gl-worker.mjs'],
  ['node_modules/maplibre-gl/dist/maplibre-gl-shared.mjs', 'public/vendor/maplibre/maplibre-gl-shared.mjs'],
];

for (const [from, to] of files) {
  if (!existsSync(from)) {
    console.warn(`vendor: ${from} not found (skipped)`);
    continue;
  }
  mkdirSync(join(to, '..'), { recursive: true });
  copyFileSync(from, to);
}
console.info('✓ vendor files copied to public/vendor');
