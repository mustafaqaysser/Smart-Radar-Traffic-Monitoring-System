'use client';

import { Check, Copy } from 'lucide-react';
import { useState } from 'react';
import { Button } from './ui/button';

/** Copies a value (a tracking link, a code) and confirms in place for two seconds. */
export function CopyButton({ value, label, copiedLabel, size = 'sm' }: { value: string; label: string; copiedLabel: string; size?: 'sm' | 'md' }) {
  const [copied, setCopied] = useState(false);
  return (
    <Button
      size={size}
      onClick={async () => {
        await navigator.clipboard.writeText(value);
        setCopied(true);
        window.setTimeout(() => setCopied(false), 2000);
      }}
      aria-live="polite"
    >
      {copied ? <Check aria-hidden="true" /> : <Copy aria-hidden="true" />}
      {copied ? copiedLabel : label}
    </Button>
  );
}
