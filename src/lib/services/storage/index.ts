import 'server-only';
import { LocalStorageProvider } from './local-provider';
import { S3StorageProvider } from './s3-provider';
import type { StorageProvider } from './types';

let cached: StorageProvider | null = null;

/** S3-compatible storage when S3_* is configured, the local filesystem otherwise. */
export function getStorage(): StorageProvider {
  if (cached) return cached;
  const { S3_ENDPOINT, S3_BUCKET, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY } = process.env;
  cached =
    S3_ENDPOINT && S3_BUCKET && S3_ACCESS_KEY_ID && S3_SECRET_ACCESS_KEY
      ? new S3StorageProvider({
          endpoint: S3_ENDPOINT,
          region: process.env.S3_REGION ?? 'auto',
          bucket: S3_BUCKET,
          accessKeyId: S3_ACCESS_KEY_ID,
          secretAccessKey: S3_SECRET_ACCESS_KEY,
          publicBaseUrl: process.env.STORAGE_PUBLIC_URL || null,
        })
      : new LocalStorageProvider();
  return cached;
}

/** URL a browser can use for a stored public file. */
export function fileUrl(key: string): string {
  return getStorage().publicUrl(key) ?? `/files/${key}`;
}
