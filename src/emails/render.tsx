import { render } from '@react-email/render';
import type { ReactElement } from 'react';
import {
  ApplicationReceivedEmail,
  EventTicketEmail,
  GiftCardEmail,
  GiftCardReceiptEmail,
  InquiryReceivedEmail,
  NewsletterConfirmEmail,
  OrderReceiptEmail,
  OrderStatusEmail,
  OtpEmail,
  ReservationCancelledEmail,
  ReservationConfirmedEmail,
  ReservationReminderEmail,
  ReservationUpdatedEmail,
  ResetPasswordEmail,
  WaitlistEmail,
  WelcomeEmail,
  type ApplicationReceivedProps,
  type EventTicketProps,
  type GiftCardProps,
  type GiftCardReceiptProps,
  type InquiryReceivedProps,
  type NewsletterConfirmProps,
  type OrderEmailProps,
  type OrderStatusProps,
  type OtpProps,
  type ReservationCancelledProps,
  type ReservationProps,
  type ResetPasswordProps,
  type WaitlistProps,
  type WelcomeProps,
} from './templates';

export type EmailTemplate =
  | { name: 'otp'; props: OtpProps }
  | { name: 'reset-password'; props: ResetPasswordProps }
  | { name: 'welcome'; props: WelcomeProps }
  | { name: 'reservation-confirmed'; props: ReservationProps }
  | { name: 'reservation-updated'; props: ReservationProps }
  | { name: 'reservation-reminder'; props: ReservationProps }
  | { name: 'reservation-cancelled'; props: ReservationCancelledProps }
  | { name: 'waitlist'; props: WaitlistProps }
  | { name: 'order-receipt'; props: OrderEmailProps }
  | { name: 'order-status'; props: OrderStatusProps }
  | { name: 'gift-card'; props: GiftCardProps }
  | { name: 'gift-card-receipt'; props: GiftCardReceiptProps }
  | { name: 'event-ticket'; props: EventTicketProps }
  | { name: 'newsletter-confirm'; props: NewsletterConfirmProps }
  | { name: 'inquiry-received'; props: InquiryReceivedProps }
  | { name: 'application-received'; props: ApplicationReceivedProps };

const ar = (locale: string) => locale === 'ar';

function build(t: EmailTemplate): { subject: string; element: ReactElement } {
  const l = t.props.locale;
  switch (t.name) {
    case 'otp':
      return { subject: ar(l) ? `رمزك في ظل: ${t.props.code}` : `Your Zill code: ${t.props.code}`, element: <OtpEmail {...t.props} /> };
    case 'reset-password':
      return { subject: ar(l) ? 'إعادة تعيين كلمة المرور' : 'Reset your password', element: <ResetPasswordEmail {...t.props} /> };
    case 'welcome':
      return { subject: ar(l) ? 'صار لك ظلٌّ عندنا' : 'You have a shade with us now', element: <WelcomeEmail {...t.props} /> };
    case 'reservation-confirmed':
      return {
        subject: ar(l) ? `ظلّك محجوز · ${t.props.dateLabel}، ${t.props.timeLabel}` : `Your shade is booked · ${t.props.dateLabel}, ${t.props.timeLabel}`,
        element: <ReservationConfirmedEmail {...t.props} />,
      };
    case 'reservation-updated':
      return { subject: ar(l) ? `عدّلنا حجزك · ${t.props.code}` : `Booking updated · ${t.props.code}`, element: <ReservationUpdatedEmail {...t.props} /> };
    case 'reservation-reminder':
      return { subject: ar(l) ? `نراك قريباً · ${t.props.timeLabel}` : `See you soon · ${t.props.timeLabel}`, element: <ReservationReminderEmail {...t.props} /> };
    case 'reservation-cancelled':
      return { subject: ar(l) ? `أُلغي الحجز ${t.props.code}` : `Booking ${t.props.code} cancelled`, element: <ReservationCancelledEmail {...t.props} /> };
    case 'waitlist':
      return { subject: ar(l) ? 'أنت على قائمة الانتظار' : 'You are on the waitlist', element: <WaitlistEmail {...t.props} /> };
    case 'order-receipt':
      return { subject: ar(l) ? `طلبك ${t.props.number}` : `Your order ${t.props.number}`, element: <OrderReceiptEmail {...t.props} /> };
    case 'order-status':
      return { subject: t.props.headline, element: <OrderStatusEmail {...t.props} /> };
    case 'gift-card':
      return { subject: ar(l) ? `${t.props.senderName} أهداك ظلّاً` : `${t.props.senderName} has sent you a Zill gift card`, element: <GiftCardEmail {...t.props} /> };
    case 'gift-card-receipt':
      return { subject: ar(l) ? 'إيصال بطاقة الهدية' : 'Your gift card receipt', element: <GiftCardReceiptEmail {...t.props} /> };
    case 'event-ticket':
      return { subject: ar(l) ? `تذكرتك · ${t.props.eventTitle}` : `Your ticket · ${t.props.eventTitle}`, element: <EventTicketEmail {...t.props} /> };
    case 'newsletter-confirm':
      return { subject: ar(l) ? 'أكّد اشتراكك في رسائل ظل' : 'Confirm your Zill letters', element: <NewsletterConfirmEmail {...t.props} /> };
    case 'inquiry-received':
      return { subject: ar(l) ? 'وصلتنا رسالتك' : 'We have your message', element: <InquiryReceivedEmail {...t.props} /> };
    case 'application-received':
      return { subject: ar(l) ? 'وصلنا طلبك للانضمام' : 'We have your application', element: <ApplicationReceivedEmail {...t.props} /> };
  }
}

export async function renderEmail(t: EmailTemplate): Promise<{ subject: string; html: string; text: string }> {
  const { subject, element } = build(t);
  const [html, text] = await Promise.all([render(element), render(element, { plainText: true })]);
  return { subject, html, text };
}
