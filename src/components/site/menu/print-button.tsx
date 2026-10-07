'use client';

import { useEffect } from 'react';
import { Button } from '@/components/site/ui/button';

/** Opens the browser's print dialog (and does so automatically when the PDF fallback sends visitors here). */
export function PrintButton({ label, auto }: { label: string; auto: boolean }) {
  useEffect(() => {
    if (!auto) return;
    const timer = window.setTimeout(() => window.print(), 600);
    return () => window.clearTimeout(timer);
  }, [auto]);
  return (
    <Button size="sm" icon={null} leadingIcon="print" onClick={() => window.print()}>
      {label}
    </Button>
  );
}
