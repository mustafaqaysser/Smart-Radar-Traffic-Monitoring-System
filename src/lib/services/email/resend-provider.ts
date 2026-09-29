import { Resend } from 'resend';
import type { EmailMessage, EmailProvider } from './types';

/** Resend adapter — active when RESEND_API_KEY is set. */
export class ResendProvider implements EmailProvider {
  readonly name = 'resend';
  private client: Resend;

  constructor(apiKey: string) {
    this.client = new Resend(apiKey);
  }

  async send(message: EmailMessage, from: string) {
    const { data, error } = await this.client.emails.send({
      from,
      to: message.to,
      subject: message.subject,
      html: message.html,
      text: message.text,
      attachments: message.attachments?.map((a) => ({ filename: a.filename, content: a.content, contentType: a.contentType })),
      tags: [{ name: 'template', value: message.template.replace(/[^a-zA-Z0-9_-]/g, '_') }],
    });
    if (error) throw new Error(`Resend: ${error.message}`);
    return { id: data?.id ?? null };
  }
}
