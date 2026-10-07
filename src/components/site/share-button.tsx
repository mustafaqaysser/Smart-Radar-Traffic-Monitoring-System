'use client';

import { useState } from 'react';
import { Icon } from '@/components/brand/icon';

/** Native share sheet where available, otherwise copies the link. */
export function ShareButton({ url, title, label, copiedLabel }: { url: string; title: string; label: string; copiedLabel: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <button
      type="button"
      className="t-small inline-flex min-h-11 items-center gap-2 underline decoration-line underline-offset-4"
      onClick={async () => {
        if (navigator.share) {
          try {
            await navigator.share({ url, title });
            return;
          } catch {
            // Dismissed or unsupported target: fall back to copying.
          }
        }
        await navigator.clipboard.writeText(url);
        setCopied(true);
        window.setTimeout(() => setCopied(false), 2500);
      }}
    >
      <Icon name="share" size={18} />
      <span aria-live="polite">{copied ? copiedLabel : label}</span>
    </button>
  );
}
