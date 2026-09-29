import 'server-only';
import { db } from '@/lib/db/client';
import { emailOutbox } from '@/lib/db/schema';
import { createId } from '@/lib/utils/id';
import { ResendProvider } from './resend-provider';
import type { EmailMessage, EmailProvider } from './types';

function provider(): EmailProvider | null {
  const key = process.env.RESEND_API_KEY;
  return key ? new ResendProvider(key) : null;
}

const SENSITIVE_TEMPLATES = new Set(['otp', 'reset-password']);

/**
 * Sends an email through the configured provider. Without one, the message is captured in the dev outbox
 * (Admin → Outbox), rendered exactly as it would be sent — OTP codes included.
 * With a real provider, sensitive templates are stored redacted.
 */
export async function sendEmail(message: EmailMessage): Promise<{ id: string; delivered: boolean }> {
  const from = process.env.EMAIL_FROM ?? 'Zill · ظل <hello@zill.test>';
  const p = provider();
  const id = createId();
  let status: 'sent' | 'failed' | 'captured' = 'captured';
  let error: string | null = null;
  if (p) {
    try {
      await p.send(message, from);
      status = 'sent';
    } catch (e) {
      status = 'failed';
      error = e instanceof Error ? e.message : String(e);
      console.error('[email] delivery failed', error);
    }
  }
  const redact = p !== null && SENSITIVE_TEMPLATES.has(message.template);
  await db.insert(emailOutbox).values({
    id,
    to: message.to,
    subject: message.subject,
    html: redact ? '<p>Redacted: delivered through the email provider.</p>' : message.html,
    text: redact ? 'Redacted: delivered through the email provider.' : message.text,
    template: message.template,
    locale: message.locale,
    meta: redact ? null : { ...(message.meta ?? {}), attachments: message.attachments?.map((a) => a.filename).join(', ') ?? null },
    provider: p?.name ?? 'outbox',
    status,
    error,
  });
  return { id, delivered: status !== 'failed' };
}
