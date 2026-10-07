'use client';

import { useRouter } from 'next/navigation';
import { useTransition } from 'react';
import { setSignInLocale } from '@/lib/actions/admin/auth';

/** Language switch before signing in (stored in the admin language cookie). */
export function AuthLanguageSwitch({ locale }: { locale: string }) {
  const router = useRouter();
  const [pending, start] = useTransition();
  const other = locale === 'ar' ? 'en' : 'ar';
  return (
    <button
      type="button"
      lang={other}
      disabled={pending}
      onClick={() =>
        start(async () => {
          await setSignInLocale(other);
          router.refresh();
        })
      }
      className="rounded-hair px-2 py-1 text-[0.8125rem] text-muted hover-capable:hover:bg-surface hover-capable:hover:text-ink"
    >
      {other === 'ar' ? 'العربية' : 'English'}
    </button>
  );
}
