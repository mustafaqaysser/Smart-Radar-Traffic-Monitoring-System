'use client';

import { Printer } from 'lucide-react';
import { useEffect } from 'react';
import { Button } from './ui/button';

/** Opens the print dialog once the sheet has rendered (when asked to), and offers a print button on screen. */
export function PrintOnLoad({ auto, label }: { auto: boolean; label: string }) {
  useEffect(() => {
    if (!auto) return;
    const timer = window.setTimeout(() => {
      void document.fonts.ready.then(() => window.print());
    }, 150);
    return () => window.clearTimeout(timer);
  }, [auto]);
  return (
    <Button variant="primary" onClick={() => window.print()} data-admin-chrome>
      <Printer aria-hidden="true" />
      {label}
    </Button>
  );
}
