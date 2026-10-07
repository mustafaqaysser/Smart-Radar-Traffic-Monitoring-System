import { describe, expect, it } from 'vitest';
import { csvMoney, toCsv } from '@/lib/admin/csv';

describe('CSV export', () => {
  it('starts with a BOM, quotes when needed and uses CRLF', () => {
    const csv = toCsv(['a', 'b'], [['plain', 'with, comma'], ['say "hi"', null]]);
    expect(csv.startsWith('﻿')).toBe(true);
    expect(csv).toBe('﻿a,b\r\nplain,"with, comma"\r\n"say ""hi""",\r\n');
  });
  it('neutralises spreadsheet formulas in text cells but not numbers', () => {
    const csv = toCsv(['x'], [['=HYPERLINK("http://evil")'], ['+cmd|calc'], ['@SUM(A1)'], ['+966 50 000 0000'], [-5]]);
    expect(csv).toContain(`"'=HYPERLINK(""http://evil"")"`);
    expect(csv).toContain(`"'+cmd|calc"`);
    expect(csv).toContain(`"'@SUM(A1)"`);
    expect(csv).toContain('\r\n+966 50 000 0000\r\n');
    expect(csv).toContain('\r\n-5\r\n');
  });
  it('keeps Arabic text intact', () => {
    expect(toCsv(['name'], [['ريم الشهري']])).toContain('ريم الشهري');
  });
  it('formats money as plain decimals', () => {
    expect(csvMoney(12345)).toBe('123.45');
    expect(csvMoney(500)).toBe('5.00');
  });
});
