import type { SeedFaq } from './types';

/** Questions guests actually ask, answered by the host. Written natively in each language. */
export const faqs: SeedFaq[] = [
  // ── Visiting ────────────────────────────────────────────────────────────
  {
    category: 'visiting',
    question: { ar: 'متى تفتحون؟', en: 'When are you open?' },
    answer: {
      ar: 'لكلّ ساعةٍ قائمتها. ظلّ الصباح من السابعة حتى الحادية عشرة والنصف، ويوم الجمعة من الثامنة حتى الثانية عشرة والنصف. الظلّ القصير من الثانية عشرة والنصف حتى الرابعة، والظلّ الطويل من الرابعة حتى السادسة والنصف. ويفتح فناء الليل من السابعة حتى الثانية عشرة والنصف بعد منتصف الليل، ويمتدّ ليلتَي الخميس والجمعة حتى الواحدة والنصف. المواعيد واحدةٌ في البيتين.',
      en: 'Each hour has its own menu. Morning shade runs from 07:00 to 11:30, and on Fridays from 08:00 to 12:30. The short shade is 12:30 to 16:00; the long shade, 16:00 to 18:30. The night courtyard opens at 19:00 and closes at 00:30, or 01:30 on Thursdays and Fridays. Both houses keep the same hours.',
    },
  },
  {
    category: 'visiting',
    question: { ar: 'هل تستقبلون الضيوف من غير حجز؟', en: 'Can I come without a booking?' },
    answer: {
      ar: 'نعم، ما دام في الفناء مقعدٌ فارغ. الصباح أيسر الأوقات، والعصر كذلك. أمّا عشاء الخميس والجمعة فنقترح أن تحجز، فالفناء يمتلئ قبل أن يبرد الهواء.',
      en: 'Yes, whenever the courtyard has a free table. Mornings are the easiest time, and afternoons are close behind. For dinner on a Thursday or Friday, book: the courtyard fills before the air has cooled.',
    },
  },
  {
    category: 'visiting',
    question: { ar: 'هل الأطفال مرحَّبٌ بهم؟', en: 'Are children welcome?' },
    answer: {
      ar: 'في كلّ ساعة. لهم قائمة «ظلٌّ صغير» بأطباقٍ على قدر أيديهم، وعندنا كراسٍ عالية في البيتين، وأقلامٌ وورقٌ لمن يحبّ أن يرسم ظلّ الليمونة. ومرّةً في الشهر نخبز معهم صباحاً.',
      en: 'At every hour. They have their own menu, Small shade, with plates sized for small hands. There are high chairs in both houses, and pencils and paper for anyone who wants to draw the shadow of the lime tree. Once a month, we bake with them in the morning.',
    },
  },

  // ── Reservations ────────────────────────────────────────────────────────
  {
    category: 'reservations',
    question: { ar: 'قبل كم يومٍ يمكنني الحجز؟', en: 'How far ahead can I book?' },
    answer: {
      ar: 'تُفتح الحجوزات قبل ستين يوماً، يوماً بعد يوم. فإذا أردت ليلةً بعينها، فاحجزها حين يُفتح يومها.',
      en: 'Reservations open 60 days ahead, one day at a time. If you have a particular evening in mind, book it on the day it opens.',
    },
  },
  {
    category: 'reservations',
    question: { ar: 'نحن أكثر من ثمانية، كيف نحجز؟', en: 'We are more than eight. How do we book?' },
    answer: {
      ar: 'يقبل الحجز الإلكتروني حتى ثمانية ضيوف. فإن كنتم تسعةً فأكثر، فاكتبوا إلينا عبر صفحة المناسبات الخاصة، ونرتّب لكم الغرفة أو السطح أو المجلس بحسب العدد والساعة، ونردّ خلال يوم عمل.',
      en: 'Online booking takes up to eight guests. For nine or more, write to us through the private dining page and we will arrange a room, the roof or the majlis to suit your number and the hour. We reply within one working day.',
    },
  },
  {
    category: 'reservations',
    question: { ar: 'لماذا تطلبون عربوناً؟', en: 'Why do you ask for a deposit?' },
    answer: {
      ar: 'للطاولات التي تتّسع لستّة ضيوفٍ فأكثر نأخذ عربوناً قدره ١٠٠ ريال عن كلّ ضيف، ويُخصم كاملاً من الفاتورة. ويُردّ إليك إن ألغيت قبل موعدك بأربع ساعاتٍ على الأقل.',
      en: 'For tables of six or more we take a deposit of SAR 100 per guest, deducted in full from the bill. It is returned if you cancel at least four hours before your booking.',
    },
  },
  {
    category: 'reservations',
    question: { ar: 'كيف أعدّل حجزي أو ألغيه؟', en: 'How do I change or cancel my booking?' },
    answer: {
      ar: 'من الرابط الذي في رسالة التأكيد، حتى أربع ساعاتٍ قبل الموعد. بعد ذلك اتّصل بالبيت مباشرة، وسنفعل ما نستطيع.',
      en: 'Through the link in your confirmation email, up to four hours before your time. After that, call the house directly and we will do what we can.',
    },
  },

  // ── Menu ────────────────────────────────────────────────────────────────
  {
    category: 'menu',
    question: { ar: 'هل كلّ ما تقدّمونه حلال؟', en: 'Is everything halal?' },
    answer: {
      ar: 'نعم، كلّه. لا لحم خنزير في مطبخنا، ولا كحول في الكؤوس ولا في القدور. وعندنا بدلاً منها قهوةٌ خولانية، وشايٌ بالنعناع، وليموناضة بورد الطائف.',
      en: 'Yes, all of it. There is no pork in our kitchen and no alcohol, neither in the glass nor in the pot. Instead there is Khawlani coffee, mint tea and a lemonade made with Taif roses.',
    },
  },
  {
    category: 'menu',
    question: { ar: 'عندي حساسية من بعض الأطعمة، فماذا أفعل؟', en: 'I have an allergy. What should I do?' },
    answer: {
      ar: 'اذكرها عند الحجز، ثم ذكّر بها من يخدم طاولتك. كلّ طبقٍ في القائمة مُعلَّمٌ بمسبّبات الحساسية، غير أنّ مطبخنا واحد، فيه سمسمٌ ومكسّراتٌ وقمحٌ وحليب، فلا نضمن خلوّ أيّ طبقٍ منها تماماً. وتتكيّف قائمة «المزولة» مع حساسيتك إن أخبرتنا قبل ٤٨ ساعة.',
      en: 'Tell us when you book, then remind whoever looks after your table. Every dish on the menu is marked with its allergens, but ours is one kitchen, with sesame, nuts, wheat and milk in it, so we cannot promise any dish is entirely free of them. The Sundial adapts to allergies with 48 hours’ notice.',
    },
  },
  {
    category: 'menu',
    question: { ar: 'ما «المزولة»؟', en: 'What is The Sundial?' },
    answer: {
      ar: 'قائمة تذوّقٍ من سبعة أطباق تمشي مع النهار، من تمرة الفجر إلى شاي منتصف الليل. ثمنها ٤٨٥ ريالاً للضيف، ولا تُقدَّم إلّا بالحجز، من الأربعاء إلى السبت مساءً. خصّص لها قرابة ساعتين ونصف.',
      en: 'A tasting menu of seven courses that follows the day, from a date at dawn to mint tea at midnight. It costs SAR 485 per guest and is served by reservation only, Wednesday to Saturday evenings. Allow about two and a half hours.',
    },
  },

  // ── Ordering ────────────────────────────────────────────────────────────
  {
    category: 'ordering',
    question: { ar: 'هل توصّلون الطلبات؟', en: 'Do you deliver?' },
    answer: {
      ar: 'نعم، من البيتين كليهما، ويمكنك أن تستلم طلبك بنفسك من الباب. يوصّل كلّ بيتٍ إلى الأحياء القريبة منه، وتظهر رسوم التوصيل والحدّ الأدنى للطلب عند الدفع بحسب حيّك. ويمكنك جدولة الطلب حتى ثلاثة أيامٍ مقدّماً.',
      en: 'Yes, from both houses, and you can collect from the door if you prefer. Each house delivers to the neighbourhoods around it; the delivery fee and minimum order for your area appear at checkout. You can schedule an order up to three days ahead.',
    },
  },
  {
    category: 'ordering',
    question: { ar: 'كم يستغرق الطلب؟', en: 'How long will my order take?' },
    answer: {
      ar: 'يظهر لك الوقت المتوقّع قبل الدفع، ثم يتحدّث وأنت تتابع الطلب. وإذا ازدحم المطبخ أطلنا الوقت المعلن، ولم نستعجل القدر.',
      en: 'You see an estimate before you pay, and it updates while you follow the order. When the kitchen is busy, we lengthen the estimate rather than hurry the pot.',
    },
  },
  {
    category: 'ordering',
    question: { ar: 'كيف أطلب من الطاولة؟', en: 'How does ordering at the table work?' },
    answer: {
      ar: 'على كلّ طاولةٍ رمزٌ تمسحه بهاتفك، فتفتح القائمة باسم طاولتك. اطلب متى شئت، وادفع حين تنتهي، أو اترك الأمر لنا كما جرت العادة. وتُضاف رسوم خدمةٍ قدرها عشرة في المئة على طلبات الطاولة وحدها.',
      en: 'Each table has a code you scan with your phone; the menu opens already knowing where you sit. Order whenever you like and pay when you are done, or leave it to us the old way. A 10% service charge applies to table orders only.',
    },
  },

  // ── Events ──────────────────────────────────────────────────────────────
  {
    category: 'events',
    question: { ar: 'كيف أحجز مقعداً في إحدى الأمسيات أو الدروس؟', en: 'How do I book an event or a class?' },
    answer: {
      ar: 'من صفحة المناسبات. التذكرة باسم صاحبها، وتشمل كلّ ما يُقدَّم في الأمسية. وإن كان عندك قيدٌ في الطعام، فأخبرنا قبل ٤٨ ساعة لنعدّ لك صحنك.',
      en: 'From the events page. Each ticket carries a guest’s name and covers everything served on the night. If you have a dietary need, tell us 48 hours ahead so we can prepare your plate.',
    },
  },
  {
    category: 'events',
    question: { ar: 'هل يمكنني إلغاء التذكرة أو التنازل عنها؟', en: 'Can I cancel or pass on a ticket?' },
    answer: {
      ar: 'يمكنك أن تهب مقعدك لغيرك متى شئت، فقط اكتب إلينا باسمه. ونردّ ثمن التذكرة كاملاً إن ألغيت قبل الموعد باثنتين وسبعين ساعة؛ أمّا بعدها فقد بدأنا نطبخ لك.',
      en: 'You can give your seat to someone else at any time; just send us their name. Tickets cancelled 72 hours or more before the event are refunded in full. After that, we have already started cooking for you.',
    },
  },

  // ── Gift cards ──────────────────────────────────────────────────────────
  {
    category: 'gift-cards',
    question: { ar: 'كيف تعمل بطاقات الهدايا؟', en: 'How do gift cards work?' },
    answer: {
      ar: 'تختار قيمةً بين ١٠٠ و٥٬٠٠٠ ريال، وتصميماً من ساعات النهار الأربع، وتكتب سطراً لمن تهديه. تصل البطاقة إليه بالبريد الإلكتروني، وتصلح في البيتين وفي طلبات التوصيل والاستلام، وتبقى صالحةً أربعةً وعشرين شهراً.',
      en: 'Choose an amount between SAR 100 and SAR 5,000, a design from one of four hours of the day, and a line for the person receiving it. The card arrives by email, works in both houses and on delivery and pickup orders, and stays valid for 24 months.',
    },
  },
  {
    category: 'gift-cards',
    question: { ar: 'هل يمكن استخدام البطاقة على دفعات؟', en: 'Can a gift card be used more than once?' },
    answer: {
      ar: 'نعم. يُخصم من البطاقة ما أنفقته، ويبقى الرصيد لزيارةٍ أخرى، وتراه في أيّ وقتٍ برقم البطاقة. لكنّها لا تُستبدل نقداً.',
      en: 'Yes. Each visit takes only what you spend, and the balance waits for the next one; you can check it at any time with the card number. It cannot be exchanged for cash.',
    },
  },

  // ── Loyalty ─────────────────────────────────────────────────────────────
  {
    category: 'loyalty',
    question: { ar: 'كيف تعمل «دائرة الظلّ»؟', en: 'How does The Shade Circle work?' },
    answer: {
      ar: 'تكسب نقطةً عن كلّ ريالٍ تنفقه في البيتين أو في طلباتك، ومئة نقطةٍ ترحيباً حين تنضمّ. كلّ عشرين نقطةً تساوي ريالاً، ويمكنك أن تدفع بالنقاط حتى نصف قيمة الطلب.',
      en: 'You earn one point for every riyal spent in either house or on your orders, plus 100 points when you join. Every 20 points are worth SAR 1, and points can pay for up to half of any order.',
    },
  },
  {
    category: 'loyalty',
    question: { ar: 'ما مستويات الدائرة؟', en: 'What are the tiers?' },
    answer: {
      ar: 'ثلاثة ظلال. «ظلّ الصباح» من أوّل زيارة، و«الظلّ الطويل» عند ١٬٥٠٠ نقطة، و«فناء الليل» عند ٥٬٠٠٠ نقطة. كلّما طال ظلّك كسبت نقاطاً أكثر عن الريال نفسه، وانفتحت لك مكافآتٌ لا تُتاح في المستوى الأول.',
      en: 'Three shades. Morning Shade from your first visit, Long Shade at 1,500 points and Night Courtyard at 5,000. The longer your shade, the more points each riyal earns, and the more rewards open up that the first tier does not see.',
    },
  },

  // ── Accessibility ───────────────────────────────────────────────────────
  {
    category: 'accessibility',
    question: { ar: 'هل يسهل الوصول إلى البيتين بالكرسي المتحرّك؟', en: 'Are the houses accessible by wheelchair?' },
    answer: {
      ar: 'في البلد مدخلٌ بلا درجاتٍ من الزقاق، والفناء والليوان ودورة مياهٍ واحدة مهيّأة، أمّا السطح فدرجٌ لا غير. وفي وادي حنيفة الطابق الأرضي كلّه مستوٍ، ومعه دورتا مياه، ولدينا منحدرٌ متنقّل للمجلس الخاص. اذكر حاجتك عند الحجز، فنختار لك طاولةً يسهل بلوغها.',
      en: 'In Al-Balad there is a step-free entrance from the lane, and the courtyard, the liwan and one restroom are accessible; the roof is reached by stairs only. In Wadi Hanifah the whole ground floor is level, with two accessible restrooms and a portable ramp for the private majlis. Mention it when you book and we will choose a table that is easy to reach.',
    },
  },
];
