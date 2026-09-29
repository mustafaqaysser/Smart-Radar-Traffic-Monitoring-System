import { mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import { dirname, join, normalize, resolve } from 'node:path';
import type { StorageProvider } from './types';

/** Local filesystem storage (development and single-server deployments). Files are served by /files/[...key]. */
export class LocalStorageProvider implements StorageProvider {
  readonly name = 'local';
  private root: string;

  constructor(dir = process.env.LOCAL_STORAGE_DIR ?? './storage') {
    this.root = resolve(dir);
  }

  private path(key: string): string {
    const full = resolve(this.root, normalize(key));
    if (!full.startsWith(this.root)) throw new Error('Invalid storage key');
    return full;
  }

  async put(key: string, data: Uint8Array, contentType: string) {
    const p = this.path(key);
    await mkdir(dirname(p), { recursive: true });
    await writeFile(p, data);
    await writeFile(`${p}.type`, contentType);
  }

  async get(key: string) {
    try {
      const p = this.path(key);
      const [data, type] = await Promise.all([readFile(p), readFile(`${p}.type`, 'utf8').catch(() => 'application/octet-stream')]);
      return { data: new Uint8Array(data), contentType: type };
    } catch {
      return null;
    }
  }

  async delete(key: string) {
    const p = this.path(key);
    await rm(p, { force: true });
    await rm(`${p}.type`, { force: true });
  }

  publicUrl(): string | null {
    return null;
  }

  static keyPath(root: string, key: string) {
    return join(root, key);
  }
}
