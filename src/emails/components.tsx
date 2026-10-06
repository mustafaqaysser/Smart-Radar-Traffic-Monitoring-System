import type { CSSProperties, ReactNode } from 'react';
import { Body, Button, Column, Container, Head, Hr, Html, Img, Preview, Row, Section, Text } from '@react-email/components';
import restaurantConfig from '@config';
import { absoluteUrl } from '@/lib/site/url';

/** Email palette: the morning phase (emails are always read in "daylight"). */
export const EC = {
  bg: '#F4EDE2',
  paper: '#FAF6EF',
  ink: '#221D27',
  muted: '#574D5F',
  line: '#DCCBB1',
  accent: '#A3402A',
  onAccent: '#FBF8F4',
  sun: '#D9982F',
  surface: '#EADFCD',
} as const;

export const emailFont = (locale: string) =>
  locale === 'ar' ? "'Geeza Pro', 'Segoe UI', Tahoma, Arial, sans-serif" : "Georgia, 'Times New Roman', serif";

export function EmailLayout({ locale, preview, children }: { locale: string; preview: string; children: ReactNode }) {
  const rtl = locale === 'ar';
  const align: CSSProperties = { direction: rtl ? 'rtl' : 'ltr', textAlign: rtl ? 'right' : 'left' };
  const tagline = restaurantConfig.tagline[rtl ? 'ar' : 'en'];
  return (
    <Html lang={locale} dir={rtl ? 'rtl' : 'ltr'}>
      <Head />
      <Preview>{preview}</Preview>
      <Body style={{ backgroundColor: EC.bg, margin: 0, padding: '32px 12px', fontFamily: emailFont(locale), color: EC.ink }}>
        <Container style={{ maxWidth: 560, backgroundColor: EC.paper, padding: '40px 36px 32px', ...align }}>
          <Img src={absoluteUrl('/brand/email-lockup.png')} width="150" height="60" alt="Zill · ظل" style={{ margin: rtl ? '0 0 32px auto' : '0 auto 32px 0' }} />
          {children}
          <Hr style={{ borderColor: EC.line, margin: '36px 0 20px' }} />
          <Text style={{ fontSize: 13, lineHeight: '20px', color: EC.muted, margin: 0, ...align }}>{tagline}</Text>
          <Text style={{ fontSize: 12, lineHeight: '18px', color: EC.muted, margin: '8px 0 0', ...align }}>
            {rtl ? 'ظل — البلد، جدّة · وادي حنيفة، الرياض' : 'Zill — Al-Balad, Jeddah · Wadi Hanifah, Riyadh'}
          </Text>
        </Container>
      </Body>
    </Html>
  );
}

export function EHeading({ children, locale }: { children: ReactNode; locale: string }) {
  return (
    <Text style={{ fontSize: locale === 'ar' ? 26 : 30, lineHeight: locale === 'ar' ? '40px' : '34px', fontWeight: 600, margin: '0 0 16px', color: EC.ink }}>{children}</Text>
  );
}

export function EText({ children, muted = false, locale }: { children: ReactNode; muted?: boolean; locale: string }) {
  return (
    <Text style={{ fontSize: locale === 'ar' ? 17 : 16, lineHeight: locale === 'ar' ? '30px' : '26px', margin: '0 0 14px', color: muted ? EC.muted : EC.ink }}>
      {children}
    </Text>
  );
}

export function EButton({ href, children }: { href: string; children: ReactNode }) {
  return (
    <Button href={href} style={{ backgroundColor: EC.accent, color: EC.onAccent, padding: '14px 22px', fontSize: 15, fontWeight: 600, textDecoration: 'none', display: 'inline-block', borderRadius: 0 }}>
      {children}
    </Button>
  );
}

export function ELink({ href, children }: { href: string; children: ReactNode }) {
  return (
    <a href={href} style={{ color: EC.accent, textDecoration: 'underline' }}>
      {children}
    </a>
  );
}

/** Label/value rows (e.g. booking details, totals). Values are isolated with <bdi> for mixed direction. */
export function ERows({ rows, locale }: { rows: { label: string; value: string; strong?: boolean }[]; locale: string }) {
  const rtl = locale === 'ar';
  return (
    <Section style={{ backgroundColor: EC.surface, padding: '16px 20px', margin: '8px 0 20px' }}>
      {rows.map((r) => (
        <Row key={r.label}>
          <Column style={{ fontSize: 14, color: EC.muted, padding: '5px 0', textAlign: rtl ? 'right' : 'left', width: '45%' }}>{r.label}</Column>
          <Column style={{ fontSize: r.strong ? 16 : 15, fontWeight: r.strong ? 700 : 500, color: EC.ink, padding: '5px 0', textAlign: rtl ? 'left' : 'right' }}>
            <bdi>{r.value}</bdi>
          </Column>
        </Row>
      ))}
    </Section>
  );
}

/** The sun dot, used as a quiet divider. */
export function ESun() {
  return (
    <Text style={{ margin: '4px 0 18px', fontSize: 0, lineHeight: 0 }}>
      <span style={{ display: 'inline-block', width: 10, height: 10, borderRadius: 999, backgroundColor: EC.sun }} />
    </Text>
  );
}
