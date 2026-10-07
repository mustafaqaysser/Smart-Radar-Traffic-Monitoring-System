import { describe, expect, it } from 'vitest';
import { buildIcs, escapeIcsText, foldIcsLine, googleCalendarUrl, icsDate } from '@/lib/domain/calendar';

const start = new Date('2026-10-08T16:30:00Z');
const end = new Date('2026-10-08T18:05:00Z');

describe('calendar hand-offs', () => {
  it('writes UTC basic-format timestamps', () => {
    expect(icsDate(start)).toBe('20261008T163000Z');
  });

  it('escapes commas, semicolons, backslashes and newlines', () => {
    expect(escapeIcsText('Al-Balad, Jeddah; gate 2\\north\nfloor')).toBe('Al-Balad\\, Jeddah\\; gate 2\\\\north\\nfloor');
  });

  it('folds long lines at 75 octets without splitting Arabic letters', () => {
    const line = `SUMMARY:${'ظلّ البلد '.repeat(12)}`;
    const folded = foldIcsLine(line);
    const physical = folded.split('\r\n');
    expect(physical.length).toBeGreaterThan(1);
    const encoder = new TextEncoder();
    for (const p of physical) expect(encoder.encode(p).length).toBeLessThanOrEqual(75);
    // Unfolding (remove CRLF + one space) restores the original line exactly.
    expect(folded.replace(/\r\n /g, '')).toBe(line);
    for (const p of physical.slice(1)) expect(p.startsWith(' ')).toBe(true);
  });

  it('builds a complete VEVENT with an alarm and CRLF line endings', () => {
    const ics = buildIcs({ uid: 'reservation-abc@zill', title: 'Zill · Al-Balad', start, end, location: 'Al-Balad, Jeddah', description: 'Party of 4 · ZL-7K3M9Q', url: 'https://zill.test/en/reserve/manage/ZL-7K3M9Q?token=x', sequence: 3, alarmMinutes: 120, stamp: start });
    expect(ics.startsWith('BEGIN:VCALENDAR\r\n')).toBe(true);
    expect(ics.endsWith('END:VCALENDAR\r\n')).toBe(true);
    expect(ics).toContain('DTSTART:20261008T163000Z');
    expect(ics).toContain('DTEND:20261008T180500Z');
    expect(ics).toContain('SEQUENCE:3');
    expect(ics).toContain('LOCATION:Al-Balad\\, Jeddah');
    expect(ics).toContain('TRIGGER:-PT120M');
    expect(ics.split('\r\n').filter((l) => l.startsWith('BEGIN:')).length).toBe(3);
    expect(ics.replace(/\r\n/g, '')).not.toContain('\n');
  });

  it('builds a Google Calendar template link', () => {
    const url = new URL(googleCalendarUrl({ title: 'ظل · البلد', start, end, location: 'البلد، جدة', details: 'ZL-7K3M9Q' }));
    expect(url.hostname).toBe('calendar.google.com');
    expect(url.searchParams.get('action')).toBe('TEMPLATE');
    expect(url.searchParams.get('dates')).toBe('20261008T163000Z/20261008T180500Z');
    expect(url.searchParams.get('text')).toBe('ظل · البلد');
    expect(url.searchParams.get('location')).toBe('البلد، جدة');
  });
});
