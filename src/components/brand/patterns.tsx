import { useId } from 'react';

/**
 * Vents — the triangular openings of Najdi mud walls, redrawn as a sparse lattice.
 * Architecture, not ornament: one band per page at most.
 */
export function VentsBand({ className, rows = 2 }: { className?: string; rows?: number }) {
  const id = useId();
  const height = rows * 28;
  return (
    <svg className={className} width="100%" height={height} aria-hidden="true" focusable="false">
      <defs>
        <pattern id={`vents-${id}`} width="36" height="28" patternUnits="userSpaceOnUse">
          <path d="M8 22 14 10l6 12z" fill="currentColor" />
        </pattern>
      </defs>
      <rect width="100%" height={height} fill={`url(#vents-${id})`} />
    </svg>
  );
}

/** Crenellation — the stepped parapet of a mud-brick roof, as a divider. */
export function Crenellation({ className }: { className?: string }) {
  const id = useId();
  return (
    <svg className={className} width="100%" height="14" aria-hidden="true" focusable="false">
      <defs>
        <pattern id={`cren-${id}`} width="28" height="14" patternUnits="userSpaceOnUse">
          <path d="M0 14V6h6V0h8v6h6v8z" fill="currentColor" />
        </pattern>
      </defs>
      <rect width="100%" height="14" fill={`url(#cren-${id})`} />
    </svg>
  );
}

/**
 * Hour marks on a dial arc. `hours` are labelled with pre-formatted strings (Intl, per locale);
 * the arc spans `from`–`to` degrees (0 = east, clockwise).
 */
export function HourMarks({
  labels,
  radius = 160,
  from = 180,
  to = 360,
  className,
}: {
  labels: string[];
  radius?: number;
  from?: number;
  to?: number;
  className?: string;
}) {
  const size = radius * 2 + 80;
  const c = size / 2;
  const steps = Math.max(1, labels.length - 1);
  return (
    <svg viewBox={`0 0 ${size} ${size / 2 + 40}`} className={className} aria-hidden="true" focusable="false">
      {labels.map((label, i) => {
        const angle = ((from + ((to - from) * i) / steps) * Math.PI) / 180;
        const r = (v: number) => Math.round(v * 100) / 100;
        const x1 = r(c + Math.cos(angle) * radius);
        const y1 = r(c + Math.sin(angle) * radius);
        const x2 = r(c + Math.cos(angle) * (radius - 14));
        const y2 = r(c + Math.sin(angle) * (radius - 14));
        const tx = r(c + Math.cos(angle) * (radius + 22));
        const ty = r(c + Math.sin(angle) * (radius + 22));
        return (
          <g key={`${label}-${i}`}>
            <line x1={x1} y1={y1} x2={x2} y2={y2} stroke="currentColor" strokeWidth={1.5} />
            <text x={tx} y={ty} textAnchor="middle" dominantBaseline="middle" fontSize="13" fill="currentColor" className="t-instrument">
              {label}
            </text>
          </g>
        );
      })}
    </svg>
  );
}
