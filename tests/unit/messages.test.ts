import { readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

const root = join(process.cwd(), 'src', 'messages');
const locales = readdirSync(root).filter((d) => !d.includes('.'));

function flatten(value: unknown, prefix = ''): Record<string, string> {
  if (typeof value === 'string') return { [prefix]: value };
  if (!value || typeof value !== 'object') return {};
  return Object.fromEntries(Object.entries(value).flatMap(([k, v]) => Object.entries(flatten(v, prefix ? `${prefix}.${k}` : k))));
}

/** Every message file of a locale, including sub-folders (admin/orders.json → admin.orders.*). */
function load(locale: string, dir = join(root, locale), prefix = ''): Record<string, string> {
  const out: Record<string, string> = {};
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const name = entry.name.replace(/\.json$/, '');
    const key = prefix ? `${prefix}.${name}` : name;
    if (entry.isDirectory()) Object.assign(out, load(locale, join(dir, entry.name), key));
    else if (entry.name.endsWith('.json')) Object.assign(out, flatten(JSON.parse(readFileSync(join(dir, entry.name), 'utf8')), key));
  }
  return out;
}

describe('UI messages', () => {
  const all = Object.fromEntries(locales.map((l) => [l, load(l)]));

  it('has the same keys in every language', () => {
    const [first, ...rest] = locales;
    const base = Object.keys(all[first as string] ?? {}).sort();
    for (const l of rest) expect(Object.keys(all[l] ?? {}).sort()).toEqual(base);
  });

  it('never uses ICU # (it ignores the configured digits) — plural messages use {n}', () => {
    for (const l of locales) for (const [key, msg] of Object.entries(all[l] ?? {})) expect(msg.includes('#'), `${l}:${key}`).toBe(false);
  });

  it('contains no placeholder copy', () => {
    for (const l of locales)
      for (const [key, msg] of Object.entries(all[l] ?? {})) expect(/lorem|ipsum|TODO|TBD|coming soon|قريباً جداً/i.test(msg), `${l}:${key}`).toBe(false);
  });

  it('keeps Latin digits out of Arabic copy (numbers are formatted at runtime)', () => {
    for (const [key, msg] of Object.entries(all.ar ?? {})) {
      const stripped = msg.replace(/\{[^}]*\}/g, '').replace(/https?:\S+/g, '').replace(/\+\d[\d\s]+/g, '').replace(/PDF|Word/g, '');
      expect(/[0-9]/.test(stripped), `ar:${key} → ${msg}`).toBe(false);
    }
  });
});
