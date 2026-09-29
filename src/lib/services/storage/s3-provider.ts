import { AwsClient } from 'aws4fetch';
import type { StorageProvider } from './types';

/** Any S3-compatible store (AWS S3, Cloudflare R2, MinIO…) through signed fetch requests. */
export class S3StorageProvider implements StorageProvider {
  readonly name = 's3';
  private client: AwsClient;
  private base: string;

  constructor(
    private readonly config: { endpoint: string; region: string; bucket: string; accessKeyId: string; secretAccessKey: string; publicBaseUrl: string | null },
  ) {
    this.client = new AwsClient({ accessKeyId: config.accessKeyId, secretAccessKey: config.secretAccessKey, region: config.region, service: 's3' });
    this.base = `${config.endpoint.replace(/\/$/, '')}/${config.bucket}`;
  }

  private url(key: string) {
    return `${this.base}/${key.split('/').map(encodeURIComponent).join('/')}`;
  }

  async put(key: string, data: Uint8Array, contentType: string) {
    const body = new Uint8Array(data).buffer as ArrayBuffer;
    const res = await this.client.fetch(this.url(key), { method: 'PUT', body, headers: { 'content-type': contentType } });
    if (!res.ok) throw new Error(`S3 put failed: ${res.status}`);
  }

  async get(key: string) {
    const res = await this.client.fetch(this.url(key));
    if (res.status === 404) return null;
    if (!res.ok) throw new Error(`S3 get failed: ${res.status}`);
    return { data: new Uint8Array(await res.arrayBuffer()), contentType: res.headers.get('content-type') ?? 'application/octet-stream' };
  }

  async delete(key: string) {
    const res = await this.client.fetch(this.url(key), { method: 'DELETE' });
    if (!res.ok && res.status !== 404) throw new Error(`S3 delete failed: ${res.status}`);
  }

  publicUrl(key: string) {
    return this.config.publicBaseUrl ? `${this.config.publicBaseUrl.replace(/\/$/, '')}/${key}` : null;
  }
}
