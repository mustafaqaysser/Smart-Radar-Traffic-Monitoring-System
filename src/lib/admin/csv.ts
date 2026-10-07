/**
 * CSV for spreadsheets: UTF-8 with a byte-order mark (so Excel reads Arabic correctly), CRLF line endings,
 * every field quoted when needed, and cells that look like formulas neutralised (CSV injection).
 */
export function toCsv(header: string[], rows: (string | number | null | undefined)[][]): string {
  const cell = (value: string | number | null | undefined) => {
    if (value === null || value === undefined) return '';
    let text = String(value);
    // A leading = @ tab or CR — or + / - followed by anything but a number (phone numbers stay as they are).
    if (typeof value === 'string' && (/^[=@\t\r]/.test(text) || /^[+-](?![\d\s()-]*$)/.test(text))) text = `'${text}`;
    return /[",\r\n']/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
  };
  return '﻿' + [header, ...rows].map((r) => r.map(cell).join(',')).join('\r\n') + '\r\n';
}

/** Minor units → a plain decimal for spreadsheets (always Western digits, no currency symbol). */
export function csvMoney(minor: number): string {
  return (minor / 100).toFixed(2);
}

export function csvResponse(filename: string, body: string): Response {
  return new Response(body, {
    headers: {
      'content-type': 'text/csv; charset=utf-8',
      'content-disposition': `attachment; filename="${filename}"`,
      'cache-control': 'no-store',
    },
  });
}
