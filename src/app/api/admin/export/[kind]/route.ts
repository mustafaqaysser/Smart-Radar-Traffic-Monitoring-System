import { getLocale } from 'next-intl/server';
import { getAdminScope } from '@/lib/admin/context';
import { csvResponse, toCsv } from '@/lib/admin/csv';
import { EXPORTS } from '@/lib/admin/exports';
import { audit } from '@/lib/server/audit';
import { can } from '@/lib/auth/permissions';
import { getCurrentUser } from '@/lib/auth/session';

export const dynamic = 'force-dynamic';

/** CSV downloads for staff with the matching permission; every export is written to the audit log. */
export async function GET(request: Request, { params }: { params: Promise<{ kind: string }> }) {
  const { kind } = await params;
  const definition = Object.hasOwn(EXPORTS, kind) ? EXPORTS[kind] : undefined;
  if (!definition) return new Response('Not found', { status: 404 });
  const user = await getCurrentUser();
  if (!user) return new Response('Unauthorized', { status: 401 });
  if (!can(user.role, definition.permission)) return new Response('Forbidden', { status: 403 });
  const [scope, locale] = await Promise.all([getAdminScope(user), getLocale()]);
  const file = await definition.build(scope, new URL(request.url).searchParams, locale);
  await audit({ actor: { id: user.id, email: user.email }, action: 'export.download', entity: 'export', entityId: kind, summary: `${file.filename} (${file.rows.length} rows)` });
  return csvResponse(file.filename, toCsv(file.header, file.rows));
}
