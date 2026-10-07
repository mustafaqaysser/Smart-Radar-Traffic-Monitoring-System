'use client';

/**
 * Last-resort error page (the root layout itself failed), so it cannot rely on fonts, messages or styles.
 * Both languages are shown; the reload button retries the whole page.
 */
export default function GlobalError({ reset }: { error: Error & { digest?: string }; reset: () => void }) {
  return (
    <html lang="ar" dir="rtl">
      <body style={{ margin: 0, minHeight: '100dvh', display: 'grid', placeItems: 'center', background: '#F4EDE2', color: '#221D27', fontFamily: 'Georgia, serif' }}>
        <main style={{ maxWidth: '34rem', padding: '2rem', display: 'grid', gap: '1.25rem' }}>
          <h1 style={{ fontSize: '2rem', margin: 0 }}>تعثّر شيءٌ في المطبخ</h1>
          <p style={{ margin: 0, lineHeight: 1.8 }}>لم نتمكّن من عرض الصفحة. حاول مجدداً بعد لحظة.</p>
          <p lang="en" dir="ltr" style={{ margin: 0, lineHeight: 1.6 }}>
            Something slipped in the kitchen. Please try again in a moment.
          </p>
          <button type="button" onClick={() => reset()} style={{ justifySelf: 'start', padding: '0.75rem 1.5rem', background: '#A3402A', color: '#FBF8F4', border: 0, fontSize: '1rem', cursor: 'pointer' }}>
            حاول مجدداً · Try again
          </button>
        </main>
      </body>
    </html>
  );
}
