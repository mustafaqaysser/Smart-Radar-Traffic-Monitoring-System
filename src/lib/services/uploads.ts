/**
 * Upload validation by content (magic bytes), not by the file name or the browser-declared type.
 */
export type UploadKind = 'image' | 'cv';

const SIGNATURES: { mime: string; ext: string; test: (b: Uint8Array) => boolean }[] = [
  { mime: 'image/jpeg', ext: 'jpg', test: (b) => b[0] === 0xff && b[1] === 0xd8 && b[2] === 0xff },
  { mime: 'image/png', ext: 'png', test: (b) => b[0] === 0x89 && b[1] === 0x50 && b[2] === 0x4e && b[3] === 0x47 },
  { mime: 'image/webp', ext: 'webp', test: (b) => b[0] === 0x52 && b[1] === 0x49 && b[2] === 0x46 && b[3] === 0x46 && b[8] === 0x57 && b[9] === 0x45 && b[10] === 0x42 && b[11] === 0x50 },
  { mime: 'image/avif', ext: 'avif', test: (b) => b[4] === 0x66 && b[5] === 0x74 && b[6] === 0x79 && b[7] === 0x70 && b[8] === 0x61 && b[9] === 0x76 && b[10] === 0x69 && b[11] === 0x66 },
  { mime: 'application/pdf', ext: 'pdf', test: (b) => b[0] === 0x25 && b[1] === 0x50 && b[2] === 0x44 && b[3] === 0x46 },
  // .docx is a zip container; accepted for CVs only.
  { mime: 'application/vnd.openxmlformats-officedocument.wordprocessingml.document', ext: 'docx', test: (b) => b[0] === 0x50 && b[1] === 0x4b && b[2] === 0x03 && b[3] === 0x04 },
];

const RULES: Record<UploadKind, { maxBytes: number; mimes: string[] }> = {
  image: { maxBytes: 12 * 1024 * 1024, mimes: ['image/jpeg', 'image/png', 'image/webp', 'image/avif'] },
  cv: { maxBytes: 5 * 1024 * 1024, mimes: ['application/pdf', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'] },
};

export type UploadCheck = { ok: true; mime: string; ext: string } | { ok: false; reason: 'empty' | 'too_large' | 'type' };

export function checkUpload(bytes: Uint8Array, kind: UploadKind): UploadCheck {
  if (bytes.byteLength === 0) return { ok: false, reason: 'empty' };
  const rule = RULES[kind];
  if (bytes.byteLength > rule.maxBytes) return { ok: false, reason: 'too_large' };
  const sig = SIGNATURES.find((s) => s.test(bytes));
  if (!sig || !rule.mimes.includes(sig.mime)) return { ok: false, reason: 'type' };
  return { ok: true, mime: sig.mime, ext: sig.ext };
}

/** A safe display name for an uploaded file (no paths, no control characters). */
export function safeFileName(name: string): string {
  return name.replace(/[/\\]/g, '_').replace(/[\u0000-\u001f]/g, '').slice(-120) || 'file';
}
