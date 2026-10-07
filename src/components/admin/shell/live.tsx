'use client';

import { useRouter } from 'next/navigation';
import { createContext, useContext, useEffect, useRef, useState, type ReactNode } from 'react';
import type { LivePulse } from '@/lib/admin/live';

const LiveContext = createContext<LivePulse | null>(null);

/** One EventSource per admin tab; every screen reads the latest pulse from context. */
export function LiveProvider({ initial, children }: { initial: LivePulse; children: ReactNode }) {
  const [pulse, setPulse] = useState<LivePulse>(initial);
  useEffect(() => {
    const source = new EventSource('/api/admin/live');
    source.addEventListener('pulse', (e) => {
      try {
        setPulse(JSON.parse((e as MessageEvent<string>).data) as LivePulse);
      } catch {
        // A malformed frame is ignored; the next one replaces it.
      }
    });
    return () => source.close();
  }, []);
  return <LiveContext.Provider value={pulse}>{children}</LiveContext.Provider>;
}

export function useLive(): LivePulse | null {
  return useContext(LiveContext);
}

type Topic = 'orders' | 'requests' | 'reservations' | 'notifications';

function versionOf(pulse: LivePulse | null, topic: Topic): string | null {
  if (!pulse) return null;
  if (topic === 'notifications') return `${pulse.notifications.unread}:${pulse.notifications.latest}`;
  return pulse[topic]?.version ?? null;
}

/** Re-renders the current screen from the server whenever one of its topics changes. */
export function LiveRefresh({ topics }: { topics: Topic[] }) {
  const pulse = useLive();
  const router = useRouter();
  const key = topics.map((t) => versionOf(pulse, t)).join('|');
  const previous = useRef(key);
  useEffect(() => {
    if (previous.current === key) return;
    previous.current = key;
    router.refresh();
  }, [key, router]);
  return null;
}
