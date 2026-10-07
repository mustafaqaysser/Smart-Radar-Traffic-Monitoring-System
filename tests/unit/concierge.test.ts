import { describe, expect, it } from 'vitest';
import { CONCIERGE_MODEL, CONCIERGE_TOOLS, runConciergeTool } from '@/lib/server/concierge';

const ctx = { locale: 'en', user: null, now: new Date('2026-10-07T12:00:00Z') };

describe('concierge tools', () => {
  it('defaults to the current Opus model and streams tool inputs', () => {
    expect(CONCIERGE_MODEL).toBe('claude-opus-5-5');
    expect(CONCIERGE_TOOLS.map((t) => t.name)).toEqual(['menu_now', 'check_availability', 'create_reservation']);
    for (const tool of CONCIERGE_TOOLS) {
      expect(tool.eager_input_streaming).toBe(true);
      expect(tool.input_schema.additionalProperties).toBe(false);
    }
  });

  it('rejects malformed inputs before touching availability or bookings', async () => {
    const truncated = await runConciergeTool('check_availability', { house: 'al-balad', date: '2026-10' }, ctx);
    expect(truncated.isError).toBe(true);
    expect(JSON.parse(truncated.content)).toHaveProperty('INVALID_JSON');

    const noPhone = await runConciergeTool('create_reservation', { house: 'al-balad', date: '2026-10-08', time: '20:00', party_size: 2, name: 'A guest', email: 'guest@example.test' }, ctx);
    expect(noPhone.isError).toBe(true);
    expect(noPhone.booking).toBeUndefined();

    const badTime = await runConciergeTool('create_reservation', { house: 'al-balad', date: '2026-10-08', time: '8pm', party_size: 2, name: 'A guest', email: 'guest@example.test', phone: '0551112233' }, ctx);
    expect(badTime.isError).toBe(true);

    const unknown = await runConciergeTool('delete_everything', {}, ctx);
    expect(unknown.isError).toBe(true);
  });
});
