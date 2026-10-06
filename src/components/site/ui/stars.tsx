import { iconPaths } from '@/components/brand/icon-paths';

/** A 1–5 rating drawn with the brand's star; announced as text. */
export function Stars({ rating, label, size = 16 }: { rating: number; label: string; size?: number }) {
  return (
    <p className="flex items-center gap-1 text-accent" role="img" aria-label={label}>
      {Array.from({ length: 5 }, (_, i) => (
        <svg key={i} viewBox="0 0 24 24" width={size} height={size} aria-hidden="true" focusable="false" fill={i < rating ? 'currentColor' : 'none'} stroke="currentColor" strokeWidth={1.5} strokeLinejoin="miter" className={i < rating ? undefined : 'opacity-40'}>
          <path d={iconPaths.star} />
        </svg>
      ))}
    </p>
  );
}
