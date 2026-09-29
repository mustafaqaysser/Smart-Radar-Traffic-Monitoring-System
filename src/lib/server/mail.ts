import 'server-only';
import { renderEmail, type EmailTemplate } from '@/emails/render';
import { sendEmail } from '@/lib/services/email/send';
import type { EmailAttachment } from '@/lib/services/email/types';

/** Renders a template and sends it (or captures it in the dev outbox). */
export async function mail(to: string, template: EmailTemplate, options: { attachments?: EmailAttachment[]; meta?: Record<string, string | number | boolean | null> } = {}) {
  const { subject, html, text } = await renderEmail(template);
  return sendEmail({ to, subject, html, text, template: template.name, locale: template.props.locale, attachments: options.attachments, meta: options.meta });
}
