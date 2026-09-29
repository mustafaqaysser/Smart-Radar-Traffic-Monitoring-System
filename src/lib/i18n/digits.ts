/** Converts Arabic-Indic (٠-٩) and Eastern Arabic-Indic (۰-۹) digits and separators to ASCII. */
export function normalizeDigits(input: string): string {
  return input
    .replace(/[٠-٩]/g, (d) => String(d.charCodeAt(0) - 0x0660))
    .replace(/[۰-۹]/g, (d) => String(d.charCodeAt(0) - 0x06f0))
    .replace(/٫/g, '.') // Arabic decimal separator
    .replace(/[٬،]/g, ','); // Arabic thousands separator / comma
}

/** Parses a user-typed number in any supported digits; returns NaN when not a number. */
export function parseLocaleNumber(input: string): number {
  const normalized = normalizeDigits(input).replace(/[\s,]/g, '').trim();
  if (!/^-?\d+(\.\d+)?$/.test(normalized)) return Number.NaN;
  return Number(normalized);
}

/** Parses a money amount typed in major units ("45.5", "٤٥٫٥") into minor units. */
export function parseMoneyInput(input: string): number {
  const value = parseLocaleNumber(input);
  return Number.isFinite(value) ? Math.round(value * 100) : Number.NaN;
}
