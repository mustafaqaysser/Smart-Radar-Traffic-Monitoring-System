/**
 * Procedural palm-frond shadows cast on the plaster — drawn, not filmed, so they always fall in the direction
 * of the real sun. Deterministic geometry (no randomness) so server and client render identical markup.
 */

function frondPath(length: number, curve: number, leaflets: number): string {
  // Rachis: a quadratic curve from the base (0,0) towards +x, bending by `curve`.
  const cx = length * 0.5;
  const cy = curve;
  const point = (t: number) => {
    const x = 2 * (1 - t) * t * cx + t * t * length;
    const y = 2 * (1 - t) * t * cy;
    return [x, y] as const;
  };
  const tangent = (t: number) => {
    const dx = 2 * (1 - t) * cx + 2 * t * (length - cx);
    const dy = 2 * (1 - t) * cy - 2 * t * cy;
    const n = Math.hypot(dx, dy) || 1;
    return [dx / n, dy / n] as const;
  };
  const parts: string[] = [];
  // Rachis as a thin tapered band.
  const spine: string[] = [];
  for (let i = 0; i <= 24; i++) {
    const t = i / 24;
    const [x, y] = point(t);
    spine.push(`${x.toFixed(1)},${(y - (1 - t) * 3).toFixed(1)}`);
  }
  for (let i = 24; i >= 0; i--) {
    const t = i / 24;
    const [x, y] = point(t);
    spine.push(`${x.toFixed(1)},${(y + (1 - t) * 3).toFixed(1)}`);
  }
  parts.push(`M${spine.join('L')}Z`);
  // Leaflets: narrow blades on both sides, longest near the middle, angled toward the tip.
  for (let i = 1; i <= leaflets; i++) {
    const t = 0.08 + (i / (leaflets + 1)) * 0.9;
    const [x, y] = point(t);
    const [tx, ty] = tangent(t);
    const size = length * (0.34 * Math.sin(Math.PI * Math.min(1, t * 1.08)) + 0.06);
    for (const side of [-1, 1]) {
      const angle = (side * (52 - t * 22) * Math.PI) / 180;
      const dx = tx * Math.cos(angle) - ty * Math.sin(angle);
      const dy = tx * Math.sin(angle) + ty * Math.cos(angle);
      const ex = x + dx * size;
      const ey = y + dy * size + size * 0.18; // gravity droop
      const w = Math.max(2.2, size * 0.045);
      const nx = -dy * w;
      const ny = dx * w;
      parts.push(`M${x.toFixed(1)},${y.toFixed(1)}Q${(x + dx * size * 0.5 + nx).toFixed(1)},${(y + dy * size * 0.5 + ny + size * 0.05).toFixed(1)} ${ex.toFixed(1)},${ey.toFixed(1)}Q${(x + dx * size * 0.5 - nx).toFixed(1)},${(y + dy * size * 0.5 - ny + size * 0.05).toFixed(1)} ${x.toFixed(1)},${y.toFixed(1)}Z`);
    }
  }
  return parts.join('');
}

const FRONDS = [
  { length: 560, curve: 70, leaflets: 26, rotate: 158, x: 760, y: -20 },
  { length: 480, curve: -50, leaflets: 22, rotate: 128, x: 800, y: 10 },
  { length: 420, curve: 60, leaflets: 20, rotate: 192, x: 780, y: -40 },
  { length: 360, curve: -40, leaflets: 18, rotate: 104, x: 820, y: 30 },
].map((f) => ({ ...f, d: frondPath(f.length, f.curve, f.leaflets) }));

export function PalmShade({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 900 640" preserveAspectRatio="xMaxYMin slice" aria-hidden="true" focusable="false">
      <g fill="var(--c-shade)">
        {FRONDS.map((f, i) => (
          <g key={i} transform={`translate(${f.x} ${f.y}) rotate(${f.rotate})`}>
            <path d={f.d} />
          </g>
        ))}
      </g>
    </svg>
  );
}
