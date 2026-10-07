'use client';

import dynamic from 'next/dynamic';
import { useState, type ReactNode } from 'react';
import { Link, usePathname } from '@/i18n/navigation';

const ReserveSheet = dynamic(() => import('./reserve-sheet'), { ssr: false });

/**
 * The dock's Reserve action: a link to /reserve that, once the page is interactive, opens the booking flow in a
 * bottom sheet instead (modified clicks still open the page). The sheet closes itself on navigation.
 */
export function ReserveDockLink({ className, children }: { className?: string; children: ReactNode }) {
  const pathname = usePathname();
  const [openOn, setOpenOn] = useState<string | null>(null);
  const open = openOn === pathname;
  return (
    <>
      <Link
        href="/reserve"
        className={className}
        aria-haspopup="dialog"
        onClick={(e) => {
          if (e.defaultPrevented || e.button !== 0 || e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) return;
          e.preventDefault();
          setOpenOn(pathname);
        }}
      >
        {children}
      </Link>
      {open ? <ReserveSheet onClose={() => setOpenOn(null)} /> : null}
    </>
  );
}
