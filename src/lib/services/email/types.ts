export interface EmailAttachment {
  filename: string;
  /** Base64-encoded content. */
  content: string;
  contentType: string;
}

export interface EmailMessage {
  to: string;
  subject: string;
  html: string;
  text: string;
  template: string;
  locale: string;
  meta?: Record<string, string | number | boolean | null>;
  attachments?: EmailAttachment[];
}

export interface EmailProvider {
  readonly name: string;
  send(message: EmailMessage, from: string): Promise<{ id: string | null }>;
}
