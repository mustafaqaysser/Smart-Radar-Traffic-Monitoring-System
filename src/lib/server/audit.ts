import 'server-only';
import { db } from '@/lib/db/client';
import { auditLogs, notifications, type Role } from '@/lib/db/schema';
import type { LocalizedText } from '@/lib/i18n/localized';
import { createId } from '@/lib/utils/id';
import { clientIp } from '@/lib/services/rate-limit';

export async function audit(entry: {
  actor: { id: string; email: string } | null;
  action: string;
  entity: string;
  entityId?: string | null;
  summary?: string;
  diff?: Record<string, unknown>;
}): Promise<void> {
  await db.insert(auditLogs).values({
    id: createId(),
    actorId: entry.actor?.id ?? null,
    actorEmail: entry.actor?.email ?? null,
    action: entry.action,
    entity: entry.entity,
    entityId: entry.entityId ?? null,
    summary: entry.summary ?? null,
    diff: entry.diff ?? null,
    ip: await clientIp().catch(() => null),
  });
}

/** Staff notification centre entry (targets a role, optionally within a branch). */
export async function notifyStaff(n: { role: Role | null; branchId?: string | null; kind: string; title: LocalizedText; body?: LocalizedText; href?: string }) {
  await db.insert(notifications).values({
    id: createId(),
    role: n.role,
    branchId: n.branchId ?? null,
    kind: n.kind,
    title: n.title,
    body: n.body ?? null,
    href: n.href ?? null,
    readBy: [],
  });
}
