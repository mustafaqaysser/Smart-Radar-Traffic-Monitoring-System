/**
 * Calendar hand-offs for bookings and tickets: an RFC 5545 .ics file (Apple Calendar, Outlook and most
 * others) and a Google Calendar "add event" link. Pure functions; times are always written in UTC.
 */

export interface CalendarEvent {
  /** Globally unique and stable for the booking, so a re-sent file updates the same event. */
  uid: string;
  title: string;
  start: Date;
  end: Date;
  location?: string;
  description?: string;
  url?: string;
  /** Increases every time the booking changes (calendar apps keep the highest). */
  sequence?: number;
  /** Minutes before the start for a reminder alarm. */
  alarmMinutes?: number;
  stamp?: Date;
}

/** 2026-10-07T18:30:00.000Z → 20261007T183000Z */
export function icsDate(d: Date): string {
  return d.toISOString().replace(/[-:]/g, '').replace(/\.\d{3}/, '');
}

/** Escapes TEXT values (RFC 5545 §3.3.11). */
export function escapeIcsText(value: string): string {
  return value.replace(/\\/g, '\\\\').replace(/\r?\n/g, '\\n').replace(/,/g, '\\,').replace(/;/g, '\\;');
}

/**
 * Folds a content line so no physical line exceeds 75 octets (RFC 5545 §3.1). Continuation lines start with a
 * space; multi-byte UTF-8 characters (all of Arabic) are never split.
 */
export function foldIcsLine(line: string): string {
  const encoder = new TextEncoder();
  const lines: string[] = [];
  let current = '';
  let octets = 0;
  let limit = 75;
  for (const ch of line) {
    const size = encoder.encode(ch).length;
    if (octets + size > limit) {
      lines.push(current);
      current = ch;
      octets = size;
      limit = 74; // the leading space of a continuation line counts
    } else {
      current += ch;
      octets += size;
    }
  }
  lines.push(current);
  return lines.join('\r\n ');
}

export function buildIcs(e: CalendarEvent): string {
  const lines: string[] = [
    'BEGIN:VCALENDAR',
    'VERSION:2.0',
    'PRODID:-//Zill//Bookings//EN',
    'CALSCALE:GREGORIAN',
    'METHOD:PUBLISH',
    'BEGIN:VEVENT',
    `UID:${e.uid}`,
    `SEQUENCE:${e.sequence ?? 0}`,
    `DTSTAMP:${icsDate(e.stamp ?? new Date())}`,
    `DTSTART:${icsDate(e.start)}`,
    `DTEND:${icsDate(e.end)}`,
    `SUMMARY:${escapeIcsText(e.title)}`,
  ];
  if (e.location) lines.push(`LOCATION:${escapeIcsText(e.location)}`);
  if (e.description) lines.push(`DESCRIPTION:${escapeIcsText(e.description)}`);
  if (e.url) lines.push(`URL:${e.url}`);
  if (e.alarmMinutes) {
    lines.push('BEGIN:VALARM', 'ACTION:DISPLAY', `DESCRIPTION:${escapeIcsText(e.title)}`, `TRIGGER:-PT${Math.round(e.alarmMinutes)}M`, 'END:VALARM');
  }
  lines.push('END:VEVENT', 'END:VCALENDAR');
  return `${lines.map(foldIcsLine).join('\r\n')}\r\n`;
}

export function googleCalendarUrl(e: { title: string; start: Date; end: Date; location?: string; details?: string }): string {
  const params = new URLSearchParams({ action: 'TEMPLATE', text: e.title, dates: `${icsDate(e.start)}/${icsDate(e.end)}` });
  if (e.details) params.set('details', e.details);
  if (e.location) params.set('location', e.location);
  return `https://calendar.google.com/calendar/render?${params.toString()}`;
}
