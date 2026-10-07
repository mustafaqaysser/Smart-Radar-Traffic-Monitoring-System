/**
 * Transactional emails, written natively in Arabic and English. Callers pass pre-formatted strings (dates,
 * money, labels) so the templates stay presentational; `render.ts` maps a template name to its subject.
 */
import { Img, Section, Text } from '@react-email/components';
import { EButton, EC, EHeading, ELink, ERows, ESun, EText, EmailLayout } from './components';

type Loc = { locale: string };
const pick = (locale: string, ar: string, en: string) => (locale === 'ar' ? ar : en);

// ——————————————————————————— Accounts ———————————————————————————

export interface OtpProps extends Loc {
  code: string;
  purpose: 'sign-in' | 'email-verification' | 'forget-password' | 'change-email';
}
export function OtpEmail({ locale, code, purpose }: OtpProps) {
  const intro = {
    'sign-in': pick(locale, 'هذا رمز الدخول إلى حسابك في ظل.', 'Here is your code to sign in to Zill.'),
    'email-verification': pick(locale, 'هذا رمز تأكيد بريدك الإلكتروني.', 'Here is the code to confirm your email address.'),
    'forget-password': pick(locale, 'هذا رمز إعادة تعيين كلمة المرور.', 'Here is the code to reset your password.'),
    'change-email': pick(locale, 'هذا رمز تأكيد عنوان بريدك الجديد.', 'Here is the code to confirm your new email address.'),
  }[purpose];
  return (
    <EmailLayout locale={locale} preview={pick(locale, `رمزك: ${code}`, `Your code: ${code}`)}>
      <EHeading locale={locale}>{pick(locale, 'رمزك', 'Your code')}</EHeading>
      <EText locale={locale}>{intro}</EText>
      <Section style={{ backgroundColor: EC.surface, padding: '18px 0', margin: '8px 0 18px', textAlign: 'center' }}>
        <Text style={{ fontFamily: "'IBM Plex Mono', Menlo, monospace", fontSize: 34, letterSpacing: '10px', margin: 0, color: EC.ink, direction: 'ltr' }}>{code}</Text>
      </Section>
      <EText locale={locale} muted>
        {pick(locale, 'ينتهي الرمز خلال عشر دقائق. إن لم تطلبه، فتجاهل هذه الرسالة.', 'It expires in ten minutes. If you did not ask for it, you can ignore this email.')}
      </EText>
    </EmailLayout>
  );
}

export interface ResetPasswordProps extends Loc {
  name: string;
  url: string;
}
export function ResetPasswordEmail({ locale, name, url }: ResetPasswordProps) {
  return (
    <EmailLayout locale={locale} preview={pick(locale, 'إعادة تعيين كلمة المرور', 'Reset your password')}>
      <EHeading locale={locale}>{pick(locale, `أهلاً ${name}،`, `Hello ${name},`)}</EHeading>
      <EText locale={locale}>{pick(locale, 'طلبتَ إعادة تعيين كلمة المرور. الرابط صالحٌ لساعةٍ واحدة.', 'You asked to reset your password. The link is valid for one hour.')}</EText>
      <EButton href={url}>{pick(locale, 'اختر كلمة مرور جديدة', 'Choose a new password')}</EButton>
      <EText locale={locale} muted>
        {pick(locale, 'إن لم تطلب ذلك، فلا حاجة لأيّ إجراء.', 'If you did not ask for this, no action is needed.')}
      </EText>
    </EmailLayout>
  );
}

export interface WelcomeProps extends Loc {
  name: string;
  /** The signup bonus, e.g. '100 points'; null when the membership programme is off. */
  bonusLabel: string | null;
  menuUrl: string;
}
export function WelcomeEmail({ locale, name, bonusLabel, menuUrl }: WelcomeProps) {
  return (
    <EmailLayout locale={locale} preview={pick(locale, 'صار لك ظلٌّ عندنا', 'You have a shade with us now')}>
      <EHeading locale={locale}>{name ? pick(locale, `أهلاً ${name}`, `Hello, ${name}`) : pick(locale, 'أهلاً بك', 'Hello')}</EHeading>
      <EText locale={locale}>
        {pick(
          locale,
          'صار لك حسابٌ في ظل: احفظ عناوينك وأطباقك المفضّلة، وأخبرنا بحساسيّتك الغذائية مرّةً واحدة فنتذكّرها في كلّ زيارة.',
          'You now have an account at Zill: save your addresses and favourite dishes, and tell us about allergies once — we will remember them every visit.',
        )}
      </EText>
      {bonusLabel ? <EText locale={locale}>{pick(locale, `أضفنا إلى رصيدك ${bonusLabel} في دائرة الظلّ.`, `We have added ${bonusLabel} to your Shade Circle balance.`)}</EText> : null}
      <EButton href={menuUrl}>{pick(locale, 'ما يُقدَّم الآن', 'What is being served now')}</EButton>
    </EmailLayout>
  );
}

// ——————————————————————————— Reservations ———————————————————————————

export interface ReservationProps extends Loc {
  name: string;
  code: string;
  branchName: string;
  branchAddress: string;
  dateLabel: string;
  timeLabel: string;
  partyLabel: string;
  areaLabel: string;
  occasionLabel?: string | null;
  depositLabel?: string | null;
  /** How long before the booking online changes close, e.g. '4 hours' / '٤ ساعات'. */
  cutoffLabel: string;
  manageUrl: string;
  calendarUrl: string;
  whatsappUrl: string;
  mapUrl: string;
}

function ReservationDetails(p: ReservationProps) {
  const { locale } = p;
  const rows = [
    { label: pick(locale, 'البيت', 'House'), value: p.branchName },
    { label: pick(locale, 'التاريخ', 'Date'), value: p.dateLabel },
    { label: pick(locale, 'الوقت', 'Time'), value: p.timeLabel },
    { label: pick(locale, 'الضيوف', 'Guests'), value: p.partyLabel },
    { label: pick(locale, 'الجلسة', 'Seating'), value: p.areaLabel },
    ...(p.occasionLabel ? [{ label: pick(locale, 'المناسبة', 'Occasion'), value: p.occasionLabel }] : []),
    ...(p.depositLabel ? [{ label: pick(locale, 'العربون', 'Deposit'), value: p.depositLabel }] : []),
    { label: pick(locale, 'رقم الحجز', 'Reference'), value: p.code, strong: true },
  ];
  return <ERows locale={locale} rows={rows} />;
}

export function ReservationConfirmedEmail(p: ReservationProps) {
  const { locale } = p;
  return (
    <EmailLayout locale={locale} preview={pick(locale, `ظلّك محجوز: ${p.dateLabel}، ${p.timeLabel}`, `Your shade is booked: ${p.dateLabel}, ${p.timeLabel}`)}>
      <EHeading locale={locale}>{pick(locale, `ظلّك محجوز يا ${p.name}`, `Your shade is booked, ${p.name}`)}</EHeading>
      <ESun />
      <EText locale={locale}>
        {pick(locale, 'نحفظ لك الطاولة ونتذكّر ما أخبرتنا به. أرفقنا الموعد بملفٍّ لتقويمك.', 'We are keeping your table and we have noted what you told us. The booking is attached as a calendar file.')}
      </EText>
      <ReservationDetails {...p} />
      <EText locale={locale} muted>{p.branchAddress}</EText>
      <EButton href={p.manageUrl}>{pick(locale, 'تعديل الحجز أو إلغاؤه', 'Change or cancel')}</EButton>
      <EText locale={locale}>
        <ELink href={p.calendarUrl}>{pick(locale, 'أضِف إلى تقويم Google', 'Add to Google Calendar')}</ELink>
        {'  ·  '}
        <ELink href={p.whatsappUrl}>{pick(locale, 'شارِك عبر واتساب', 'Share on WhatsApp')}</ELink>
        {'  ·  '}
        <ELink href={p.mapUrl}>{pick(locale, 'الاتجاهات', 'Directions')}</ELink>
      </EText>
      <EText locale={locale} muted>
        {pick(
          locale,
          `يمكنك التعديل أو الإلغاء عبر الرابط ما دام بينك وبين الموعد ${p.cutoffLabel} أو أكثر. وبعد ذلك، اتصل بنا مباشرة.`,
          `You can change or cancel with the link until ${p.cutoffLabel} before. After that, please call us directly.`,
        )}
      </EText>
    </EmailLayout>
  );
}

export function ReservationUpdatedEmail(p: ReservationProps) {
  const { locale } = p;
  return (
    <EmailLayout locale={locale} preview={pick(locale, 'عدّلنا حجزك', 'Your booking has changed')}>
      <EHeading locale={locale}>{pick(locale, 'عدّلنا حجزك', 'Your booking has changed')}</EHeading>
      <EText locale={locale}>{pick(locale, 'هذه تفاصيل الحجز بعد التعديل، والملفّ المرفق يحلّ محلّ السابق في تقويمك.', 'Here are the new details; the attached calendar file replaces the previous one.')}</EText>
      <ReservationDetails {...p} />
      <EButton href={p.manageUrl}>{pick(locale, 'عرض الحجز', 'View booking')}</EButton>
    </EmailLayout>
  );
}

export function ReservationReminderEmail(p: ReservationProps) {
  const { locale } = p;
  return (
    <EmailLayout locale={locale} preview={pick(locale, `نراك غداً عند ${p.timeLabel}`, `See you tomorrow at ${p.timeLabel}`)}>
      <EHeading locale={locale}>{pick(locale, 'نراك قريباً', 'See you soon')}</EHeading>
      <EText locale={locale}>
        {pick(locale, `نذكّرك بحجزك في ${p.branchName}. الفناء يبرد بعد المغرب؛ فإن كان العشاء، فاحمل وشاحاً خفيفاً.`, `A reminder of your table at ${p.branchName}. The courtyard cools after sunset — for dinner, bring a light layer.`)}
      </EText>
      <ReservationDetails {...p} />
      <EButton href={p.manageUrl}>{pick(locale, 'تعديل أو إلغاء', 'Change or cancel')}</EButton>
      <EText locale={locale}>
        <ELink href={p.mapUrl}>{pick(locale, 'الاتجاهات', 'Directions')}</ELink>
      </EText>
    </EmailLayout>
  );
}

export interface ReservationCancelledProps extends Loc {
  name: string;
  code: string;
  branchName: string;
  dateLabel: string;
  timeLabel: string;
  rebookUrl: string;
  refundLabel?: string | null;
  /** Shown when a paid deposit is kept because the booking was cancelled inside the cut-off. */
  keptLabel?: string | null;
}
export function ReservationCancelledEmail(p: ReservationCancelledProps) {
  const { locale } = p;
  return (
    <EmailLayout locale={locale} preview={pick(locale, 'ألغينا حجزك', 'Your booking is cancelled')}>
      <EHeading locale={locale}>{pick(locale, 'ألغينا حجزك', 'Your booking is cancelled')}</EHeading>
      <EText locale={locale}>
        {pick(locale, `ألغينا الحجز ${p.code} في ${p.branchName} يوم ${p.dateLabel} عند ${p.timeLabel}.`, `We have cancelled booking ${p.code} at ${p.branchName} on ${p.dateLabel} at ${p.timeLabel}.`)}
      </EText>
      {p.refundLabel ? <EText locale={locale}>{pick(locale, `سيُعاد العربون (${p.refundLabel}) إلى وسيلة الدفع نفسها.`, `The deposit (${p.refundLabel}) will be returned to the same payment method.`)}</EText> : null}
      {p.keptLabel ? <EText locale={locale} muted>{p.keptLabel}</EText> : null}
      <EButton href={p.rebookUrl}>{pick(locale, 'احجز موعداً آخر', 'Book another time')}</EButton>
    </EmailLayout>
  );
}

export interface WaitlistProps extends Loc {
  name: string;
  branchName: string;
  dateLabel: string;
  timeLabel: string;
  partyLabel: string;
}
export function WaitlistEmail(p: WaitlistProps) {
  const { locale } = p;
  return (
    <EmailLayout locale={locale} preview={pick(locale, 'أنت على قائمة الانتظار', 'You are on the waitlist')}>
      <EHeading locale={locale}>{pick(locale, 'أنت على قائمة الانتظار', 'You are on the waitlist')}</EHeading>
      <EText locale={locale}>
        {pick(locale, 'إن خلا ظلٌّ في هذا الوقت أو قريباً منه، نكتب إليك فوراً ونحفظه لك ساعة.', 'If a table opens at or near that time, we will write to you straight away and hold it for an hour.')}
      </EText>
      <ERows
        locale={locale}
        rows={[
          { label: pick(locale, 'البيت', 'House'), value: p.branchName },
          { label: pick(locale, 'التاريخ', 'Date'), value: p.dateLabel },
          { label: pick(locale, 'الوقت المفضّل', 'Preferred time'), value: p.timeLabel },
          { label: pick(locale, 'الضيوف', 'Guests'), value: p.partyLabel },
        ]}
      />
    </EmailLayout>
  );
}

export interface WaitlistOfferProps extends Loc {
  name: string;
  branchName: string;
  dateLabel: string;
  timeLabel: string;
  partyLabel: string;
  expiresLabel: string;
  bookUrl: string;
}
export function WaitlistOfferEmail(p: WaitlistOfferProps) {
  const { locale } = p;
  return (
    <EmailLayout locale={locale} preview={pick(locale, `فرغت طاولة: ${p.dateLabel}، ${p.timeLabel}`, `A table has opened: ${p.dateLabel}, ${p.timeLabel}`)}>
      <EHeading locale={locale}>{pick(locale, `فرغت طاولةٌ لك يا ${p.name}`, `A table has opened for you, ${p.name}`)}</EHeading>
      <ESun />
      <EText locale={locale}>
        {pick(locale, `نحفظها باسمك حتى ${p.expiresLabel}. أكّد الحجز بلمسة، وإن لم يناسبك الوقت فلا حاجة لأيّ إجراء.`, `We are holding it in your name until ${p.expiresLabel}. Confirm with one tap — if the time no longer suits you, there is nothing to do.`)}
      </EText>
      <ERows
        locale={locale}
        rows={[
          { label: pick(locale, 'البيت', 'House'), value: p.branchName },
          { label: pick(locale, 'التاريخ', 'Date'), value: p.dateLabel },
          { label: pick(locale, 'الوقت', 'Time'), value: p.timeLabel, strong: true },
          { label: pick(locale, 'الضيوف', 'Guests'), value: p.partyLabel },
        ]}
      />
      <EButton href={p.bookUrl}>{pick(locale, 'أكّد الحجز', 'Confirm the booking')}</EButton>
    </EmailLayout>
  );
}

// ——————————————————————————— Orders ———————————————————————————

export interface OrderEmailProps extends Loc {
  name: string;
  number: string;
  branchName: string;
  channelLabel: string;
  whenLabel: string;
  addressLabel?: string | null;
  lines: { name: string; quantity: string; details: string; total: string }[];
  totals: { label: string; value: string; strong?: boolean }[];
  paymentLabel: string;
  trackUrl: string;
}
export function OrderReceiptEmail(p: OrderEmailProps) {
  const { locale } = p;
  const rtl = locale === 'ar';
  return (
    <EmailLayout locale={locale} preview={pick(locale, `طلبك ${p.number} وصلنا`, `Order ${p.number} received`)}>
      <EHeading locale={locale}>{pick(locale, `وصلنا طلبك يا ${p.name}`, `We have your order, ${p.name}`)}</EHeading>
      <EText locale={locale}>
        {pick(locale, `${p.channelLabel} من ${p.branchName} — ${p.whenLabel}.`, `${p.channelLabel} from ${p.branchName} — ${p.whenLabel}.`)}
      </EText>
      {p.addressLabel ? <EText locale={locale} muted>{p.addressLabel}</EText> : null}
      <Section style={{ margin: '12px 0' }}>
        {p.lines.map((l, i) => (
          <Text key={`${l.name}-${i}`} style={{ margin: '0 0 10px', fontSize: 15, lineHeight: '22px', color: EC.ink, textAlign: rtl ? 'right' : 'left' }}>
            <strong>
              <bdi>{l.quantity}</bdi> × {l.name}
            </strong>{' '}
            — <bdi>{l.total}</bdi>
            {l.details ? <span style={{ display: 'block', color: EC.muted, fontSize: 13 }}>{l.details}</span> : null}
          </Text>
        ))}
      </Section>
      <ERows locale={locale} rows={[...p.totals, { label: pick(locale, 'الدفع', 'Payment'), value: p.paymentLabel }]} />
      <EButton href={p.trackUrl}>{pick(locale, 'تتبّع الطلب', 'Track your order')}</EButton>
    </EmailLayout>
  );
}

export interface OrderStatusProps extends Loc {
  name: string;
  number: string;
  headline: string;
  message: string;
  trackUrl: string;
}
export function OrderStatusEmail(p: OrderStatusProps) {
  const { locale } = p;
  return (
    <EmailLayout locale={locale} preview={p.headline}>
      <EHeading locale={locale}>{p.headline}</EHeading>
      <EText locale={locale}>{p.message}</EText>
      <EText locale={locale} muted>
        {pick(locale, `رقم الطلب: ${p.number}`, `Order ${p.number}`)}
      </EText>
      <EButton href={p.trackUrl}>{pick(locale, 'تتبّع الطلب', 'Track your order')}</EButton>
    </EmailLayout>
  );
}

// ——————————————————————————— Gift cards ———————————————————————————

export interface GiftCardProps extends Loc {
  recipientName: string;
  senderName: string;
  message?: string | null;
  amountLabel: string;
  code: string;
  designImageUrl: string;
  balanceUrl: string;
  expiresLabel: string;
}
export function GiftCardEmail(p: GiftCardProps) {
  const { locale } = p;
  return (
    <EmailLayout locale={locale} preview={pick(locale, `${p.senderName} أهداك ظلّاً`, `${p.senderName} has given you some shade`)}>
      <EHeading locale={locale}>{pick(locale, `${p.recipientName}، هذا ظلٌّ لك`, `${p.recipientName}, this shade is for you`)}</EHeading>
      <EText locale={locale}>{pick(locale, `أرسل إليك ${p.senderName} بطاقة هديةٍ من ظل.`, `${p.senderName} has sent you a Zill gift card.`)}</EText>
      <Img src={p.designImageUrl} width="488" height="300" alt={pick(locale, `بطاقة هدية بقيمة ${p.amountLabel}`, `Gift card worth ${p.amountLabel}`)} style={{ width: '100%', height: 'auto', margin: '8px 0 20px' }} />
      {p.message ? (
        <Text style={{ fontSize: 17, lineHeight: '28px', fontStyle: locale === 'ar' ? 'normal' : 'italic', borderInlineStart: `3px solid ${EC.sun}`, padding: '0 14px', margin: '0 0 20px' }}>{p.message}</Text>
      ) : null}
      <ERows
        locale={locale}
        rows={[
          { label: pick(locale, 'القيمة', 'Value'), value: p.amountLabel, strong: true },
          { label: pick(locale, 'الرمز', 'Code'), value: p.code },
          { label: pick(locale, 'صالحةٌ حتى', 'Valid until'), value: p.expiresLabel },
        ]}
      />
      <EText locale={locale}>
        {pick(locale, 'استخدمها في البيتين أو عند الطلب عبر الموقع؛ ويبقى الرصيد لما بعد ذلك إن لم تُنفقه كلّه.', 'Use it at either house or when ordering online; any balance you do not spend stays on the card.')}
      </EText>
      <EButton href={p.balanceUrl}>{pick(locale, 'تحقّق من الرصيد', 'Check the balance')}</EButton>
    </EmailLayout>
  );
}

export interface GiftCardReceiptProps extends Loc {
  purchaserName: string;
  recipientName: string;
  amountLabel: string;
  deliveryLabel: string;
  code: string;
}
export function GiftCardReceiptEmail(p: GiftCardReceiptProps) {
  const { locale } = p;
  return (
    <EmailLayout locale={locale} preview={pick(locale, 'بطاقتك في طريقها', 'Your gift card is on its way')}>
      <EHeading locale={locale}>{pick(locale, `شكراً يا ${p.purchaserName}`, `Thank you, ${p.purchaserName}`)}</EHeading>
      <EText locale={locale}>{pick(locale, `ستصل بطاقة ${p.recipientName} ${p.deliveryLabel}.`, `${p.recipientName}'s card will arrive ${p.deliveryLabel}.`)}</EText>
      <ERows
        locale={locale}
        rows={[
          { label: pick(locale, 'القيمة', 'Value'), value: p.amountLabel, strong: true },
          { label: pick(locale, 'الرمز', 'Code'), value: p.code },
        ]}
      />
    </EmailLayout>
  );
}

// ——————————————————————————— Events ———————————————————————————

export interface EventTicketProps extends Loc {
  name: string;
  eventTitle: string;
  dateLabel: string;
  timeLabel: string;
  branchName: string;
  ticketLabel: string;
  quantityLabel: string;
  totalLabel: string;
  code: string;
  qrUrl: string;
  mapUrl: string;
}
export function EventTicketEmail(p: EventTicketProps) {
  const { locale } = p;
  return (
    <EmailLayout locale={locale} preview={pick(locale, `تذكرتك: ${p.eventTitle}`, `Your ticket: ${p.eventTitle}`)}>
      <EHeading locale={locale}>{p.eventTitle}</EHeading>
      <EText locale={locale}>{pick(locale, `مقعدك محجوز يا ${p.name}. اعرض الرمز عند الباب.`, `Your seat is booked, ${p.name}. Show this code at the door.`)}</EText>
      <Section style={{ textAlign: 'center', backgroundColor: EC.surface, padding: '24px 0', margin: '8px 0 16px' }}>
        <Img src={p.qrUrl} width="180" height="180" alt={pick(locale, `رمز التذكرة ${p.code}`, `Ticket code ${p.code}`)} style={{ margin: '0 auto' }} />
        <Text style={{ fontFamily: "'IBM Plex Mono', Menlo, monospace", fontSize: 18, letterSpacing: '4px', margin: '12px 0 0', direction: 'ltr' }}>{p.code}</Text>
      </Section>
      <ERows
        locale={locale}
        rows={[
          { label: pick(locale, 'البيت', 'House'), value: p.branchName },
          { label: pick(locale, 'التاريخ', 'Date'), value: p.dateLabel },
          { label: pick(locale, 'الوقت', 'Time'), value: p.timeLabel },
          { label: pick(locale, 'التذكرة', 'Ticket'), value: `${p.ticketLabel} × ${p.quantityLabel}` },
          { label: pick(locale, 'المجموع', 'Total'), value: p.totalLabel, strong: true },
        ]}
      />
      <EText locale={locale}>
        <ELink href={p.mapUrl}>{pick(locale, 'الاتجاهات', 'Directions')}</ELink>
      </EText>
    </EmailLayout>
  );
}

// ——————————————————————————— Newsletter, inquiries, careers ———————————————————————————

export interface NewsletterConfirmProps extends Loc {
  confirmUrl: string;
}
export function NewsletterConfirmEmail({ locale, confirmUrl }: NewsletterConfirmProps) {
  return (
    <EmailLayout locale={locale} preview={pick(locale, 'أكّد اشتراكك', 'Confirm your subscription')}>
      <EHeading locale={locale}>{pick(locale, 'رسالةٌ واحدة كلّ موسم', 'One letter a season')}</EHeading>
      <EText locale={locale}>
        {pick(locale, 'نكتب حين يتغيّر شيء يستحقّ: قائمةٌ جديدة، أمسيةٌ في الفناء، أول رُطَب الموسم. أكّد اشتراكك لنبدأ.', 'We write when something changes that deserves it: a new menu, an evening in the courtyard, the first rutab of the season. Confirm to begin.')}
      </EText>
      <EButton href={confirmUrl}>{pick(locale, 'أكّد الاشتراك', 'Confirm subscription')}</EButton>
      <EText locale={locale} muted>{pick(locale, 'إن لم تشترك، فتجاهل هذه الرسالة ولن نكتب إليك.', 'If you did not sign up, ignore this email and we will not write again.')}</EText>
    </EmailLayout>
  );
}

export interface InquiryReceivedProps extends Loc {
  name: string;
  kindLabel: string;
  summary: string;
}
export function InquiryReceivedEmail(p: InquiryReceivedProps) {
  const { locale } = p;
  return (
    <EmailLayout locale={locale} preview={pick(locale, 'وصلتنا رسالتك', 'We have your message')}>
      <EHeading locale={locale}>{pick(locale, `شكراً يا ${p.name}`, `Thank you, ${p.name}`)}</EHeading>
      <EText locale={locale}>{pick(locale, `وصلنا طلبك (${p.kindLabel}). يردّ عليك أحد فريقنا خلال يوم عمل.`, `We have your ${p.kindLabel.toLowerCase()} request. Someone from our team will reply within one working day.`)}</EText>
      <EText locale={locale} muted>{p.summary}</EText>
    </EmailLayout>
  );
}

export interface ApplicationReceivedProps extends Loc {
  name: string;
  role: string;
}
export function ApplicationReceivedEmail(p: ApplicationReceivedProps) {
  const { locale } = p;
  return (
    <EmailLayout locale={locale} preview={pick(locale, 'وصلنا طلبك للانضمام', 'We have your application')}>
      <EHeading locale={locale}>{pick(locale, `شكراً يا ${p.name}`, `Thank you, ${p.name}`)}</EHeading>
      <EText locale={locale}>{pick(locale, `وصلنا طلبك لوظيفة «${p.role}» وسيرتك الذاتية. نقرأ كلّ طلب بأنفسنا، ونكتب إليك خلال أسبوعين.`, `We have your application for ${p.role} and your CV. We read every application ourselves and will write within two weeks.`)}</EText>
    </EmailLayout>
  );
}
