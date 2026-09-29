export interface StoredObject {
  data: Uint8Array;
  contentType: string;
}

export interface StorageProvider {
  readonly name: string;
  put(key: string, data: Uint8Array, contentType: string): Promise<void>;
  get(key: string): Promise<StoredObject | null>;
  delete(key: string): Promise<void>;
  /** Public URL for publicly readable keys; null when files are served through /files/*. */
  publicUrl(key: string): string | null;
}
