import type { SeedJob } from './types';

/** Open positions. Descriptions are clean HTML: <p>, <h3>, <ul>, <li>. */
export const jobs: SeedJob[] = [
  {
    slug: 'dawn-baker',
    title: { ar: 'خبّاز الفجر', en: 'Dawn baker' },
    branch: 'al-balad',
    employment: 'full_time',
    summary: {
      ar: 'تعمل إلى جانب أبي صالح من الرابعة والنصف فجراً، حين يُوقد التنّور، حتى يخرج آخر خبز الصباح.',
      en: 'Work beside Abu Saleh from 04:30, when the tannour is lit, until the last of the morning bread is out.',
    },
    description: {
      ar: '<p>يخرج أوّل خبزنا من التنّور عند السادسة إلا ربعاً، ونبحث عمّن يساعده على الوصول. ستعمل إلى جانب أبي صالح، الذي أوقد هذا التنّور كلّ صباحٍ منذ ٢٠٢١، وسيعلّمك ما يستطيع، وهو كثير.</p><h3>ماذا ستفعل</h3><ul><li>تعجن التميس والصاج والكعك وتشكّلها، للفناء ولمطبخ التوصيل معاً</li><li>تقوم على التنّور من الرابعة والنصف حتى الحادية عشرة والنصف، ستّة صباحاتٍ في الأسبوع</li><li>تُعدّ عجين الغد قبل أن تنصرف</li><li>تترك المخبز عند الظهر نظيفاً كما وجدته عند الفجر</li></ul><h3>ما نرجو أن تحمله معك</h3><ul><li>سنتان على الأقل في مخبز، من أيّ مدرسةٍ كان</li><li>ألفةٌ مع الحرّ والساعات المبكرة والتكرار</li><li>صبرٌ طويل؛ فالعجين لا يستعجل، ولا أبو صالح</li></ul><h3>ما نقدّمه لك</h3><ul><li>نوبةٌ تنتهي قبل الغداء</li><li>مواصلاتٌ إلى البيت في نوبة الفجر</li><li>الوجبات، والتأمين الطبي، وثلاثون يوماً من الإجازة السنوية</li></ul>',
      en: '<p>The first bread leaves our tannour at quarter to six, and we are looking for someone to help it get there. You will work beside Abu Saleh, who has lit this oven every morning since 2021 and will teach you what he can, which is a great deal.</p><h3>The work</h3><ul><li>Mixing and shaping tamees, saj and ka’ak for the courtyard and the delivery kitchen</li><li>Tending the tannour from 04:30 to 11:30, six mornings a week</li><li>Setting tomorrow’s dough before you leave</li><li>Leaving the bakery as clean at noon as you found it at dawn</li></ul><h3>What we hope you bring</h3><ul><li>At least two years in a bakery, in any tradition</li><li>Ease with heat, early hours and repetition</li><li>Patience: the dough is in no hurry, and neither is Abu Saleh</li></ul><h3>What we offer</h3><ul><li>A shift that ends before lunch</li><li>Transport to the house for the dawn shift</li><li>Meals, medical insurance and thirty days of annual leave</li></ul>',
    },
  },
  {
    slug: 'chef-de-partie-embers',
    title: { ar: 'رئيس قسم الجمر', en: 'Chef de partie, embers' },
    branch: 'wadi-hanifah',
    employment: 'full_time',
    summary: {
      ar: 'تتولّى قسم الجمر في مطبخ وادي حنيفة، مع راكان الحارثي على خطّ التقديم.',
      en: 'Run the embers section of the Wadi Hanifah kitchen, with Rakan Al-Harthi on the pass.',
    },
    description: {
      ar: '<p>يطبخ مطبخنا في الرياض على حطب السمر والحجر الحامي، ويقرأ النار كما يقرأ الساعة. ستتولّى قسم الجمر: ريش الغنم والكفتة، وحجارة المضبي في الظهيرة، وسمك العشاء حين يأتي من البحر.</p><h3>ماذا ستفعل</h3><ul><li>تُعدّ قسمك وتقوده في نوبتَي الغداء والعشاء</li><li>توقد الجمر وتحفظه على حرارةٍ واحدة طوال الخدمة</li><li>تدرّب طاهياً مساعداً أو اثنين، وتسلّمهما ما تعرف</li><li>تشارك راكان في كتابة أطباق الموسم الجديدة</li></ul><h3>ما نرجو أن تحمله معك</h3><ul><li>ثلاث سنواتٍ على الأقل في مطبخٍ جادّ، منها سنةٌ على الجمر أو الحطب</li><li>معرفةٌ بمطبخ نجد والحجاز، أو رغبةٌ صادقة في تعلّمه</li><li>هدوءٌ في الضغط، وصوتٌ واضح على خطّ التقديم</li></ul><h3>ما نقدّمه لك</h3><ul><li>يوما راحةٍ متتاليان في الأسبوع</li><li>رحلةٌ كلّ موسم إلى مزارعنا ومورّدينا</li><li>الوجبات، والتأمين الطبي، وثلاثون يوماً من الإجازة السنوية</li></ul>',
      en: '<p>Our Riyadh kitchen cooks over samr wood and hot stones, and reads the fire the way it reads the clock. You will run the embers: lamb chops and kofta, the stones for madhbi chicken at noon, and the evening’s fish when it comes in from the coast.</p><h3>The work</h3><ul><li>Setting up and running your section through lunch and dinner</li><li>Building the embers and holding them at one heat through service</li><li>Training one or two commis and handing on what you know</li><li>Writing the new season’s dishes with Rakan</li></ul><h3>What we hope you bring</h3><ul><li>At least three years in a serious kitchen, one of them over embers or wood</li><li>Knowledge of Najdi and Hijazi cooking, or a genuine wish to learn it</li><li>Calm under pressure and a clear voice on the pass</li></ul><h3>What we offer</h3><ul><li>Two consecutive days off each week</li><li>A trip each season to our farms and suppliers</li><li>Meals, medical insurance and thirty days of annual leave</li></ul>',
    },
  },
  {
    slug: 'host',
    title: { ar: 'مضيف الفناء', en: 'Courtyard host' },
    branch: null,
    employment: 'part_time',
    summary: {
      ar: 'أوّل من يلقاه الضيف حين يدخل، وآخر من يودّعه عند الباب؛ في البلد أو في وادي حنيفة.',
      en: 'The first person a guest meets and the last one they see at the door, in Al-Balad or Wadi Hanifah.',
    },
    description: {
      ar: '<p>المضيف عندنا يلاحظ قبل أن يُسأل: ضيفٌ يتّقي الشمس، وطفلٌ يحتاج كرسياً عالياً، وطاولةٌ آن لها أن تنتقل إلى الظلّ. نبحث عن مضيفٍ يعمل أربع نوباتٍ في الأسبوع، عصراً أو مساءً.</p><h3>ماذا ستفعل</h3><ul><li>تستقبل الضيوف وتُجلسهم، وتدير قائمة الانتظار في ليالي الازدحام</li><li>تردّ على الهاتف وعلى رسائل الحجز بالعربية والإنجليزية</li><li>تعرف الضيوف الدائمين، وما يحبّون، وأين يحبّون أن يجلسوا</li><li>تتابع مع ريم بخش ترتيب الصالة قبل كلّ خدمة</li></ul><h3>ما نرجو أن تحمله معك</h3><ul><li>عربيةٌ وإنجليزيةٌ سليمتان في الحديث والكتابة</li><li>ذاكرةٌ للأسماء والوجوه</li><li>لطفٌ لا يتكلّف، حتى في العاشرة من ليلة الخميس</li></ul><h3>ما نقدّمه لك</h3><ul><li>جدولٌ يُعلن قبل أسبوعين</li><li>وجبةٌ مع الفريق قبل كلّ خدمة</li><li>أولويةٌ في الوظائف الكاملة حين تُفتح</li></ul>',
      en: '<p>A host here notices before being asked: a guest squinting into the sun, a child who needs a high chair, a table that should move into the shade. We are looking for a host for four shifts a week, afternoons or evenings.</p><h3>The work</h3><ul><li>Greeting and seating guests, and running the waiting list on busy nights</li><li>Answering the phone and booking messages in Arabic and English</li><li>Knowing the regulars, what they like and where they like to sit</li><li>Setting the floor with Reem Bakhsh before each service</li></ul><h3>What we hope you bring</h3><ul><li>Good spoken and written Arabic and English</li><li>A memory for names and faces</li><li>Warmth that does not look like effort, even at ten on a Thursday night</li></ul><h3>What we offer</h3><ul><li>A rota published two weeks ahead</li><li>A team meal before every service</li><li>First consideration for full-time roles when they open</li></ul>',
    },
  },
  {
    slug: 'khawlani-barista',
    title: { ar: 'باريستا بار الخولاني', en: 'Barista, the Khawlani bar' },
    branch: 'wadi-hanifah',
    employment: 'full_time',
    summary: {
      ar: 'تقطّر البنّ الخولاني وتُعدّ القهوة السعودية على بار وادي حنيفة، وتتعلّم مع يوسف المالكي من المدرّج إلى الفنجان.',
      en: 'Pour Khawlani coffee and make gahwa at the Wadi Hanifah bar, learning from terrace to cup with Yousef Al-Maliki.',
    },
    description: {
      ar: '<p>بنّنا يأتي من مدرّجات جازان، ونشتريه من المزارعين أنفسهم، ونحمّصه على وجهين: أشقرَ شاحباً للقهوة السعودية بالهيل، وأبعدَ قليلاً للتقطير. نبحث عمّن يقف خلف البار في العصر والمساء، ويعرف أنّ دقيقةً زائدة تغيّر الفنجان.</p><h3>ماذا ستفعل</h3><ul><li>تقطّر الخولاني على البار، وتشرح لكلّ ضيفٍ من أين جاء فنجانه</li><li>تُعدّ الدلال للفناء والمجلس، وتحفظ القهوة على حرارتها</li><li>تساعد في درس «ساعة الخولاني» مرّتين في الشهر</li><li>تتذوّق كلّ حمصةٍ جديدة مع يوسف قبل أن تصل إلى البار</li></ul><h3>ما نرجو أن تحمله معك</h3><ul><li>سنةٌ على الأقل خلف بار قهوةٍ مختصّة</li><li>لسانٌ يميّز، وصبرٌ على الميزان والساعة</li><li>رغبةٌ في أن تحدّث الضيف، لا أن تحاضره</li></ul><h3>ما نقدّمه لك</h3><ul><li>رحلةٌ في موسم القطاف إلى المزارع في جبال جازان</li><li>تدريبٌ على التحميص مع يوسف</li><li>الوجبات، والتأمين الطبي، وثلاثون يوماً من الإجازة السنوية</li></ul>',
      en: '<p>Our coffee comes from the terraces of Jazan. We buy it from the farmers themselves and roast it two ways: pale and blond for gahwa with cardamom, a little further for the pour-over. We are looking for someone to stand behind the bar in the afternoons and evenings who knows that one extra minute changes the cup.</p><h3>The work</h3><ul><li>Pouring Khawlani at the bar, and telling each guest where their cup comes from</li><li>Making dallahs for the courtyard and the majlis, and keeping them at temperature</li><li>Helping with The Khawlani Hour class twice a month</li><li>Cupping every new roast with Yousef before it reaches the bar</li></ul><h3>What we hope you bring</h3><ul><li>At least a year behind a specialty coffee bar</li><li>A discerning palate and patience with scales and timers</li><li>A wish to talk with guests, not lecture them</li></ul><h3>What we offer</h3><ul><li>A harvest trip to the farms in the Jazan mountains</li><li>Roasting training with Yousef</li><li>Meals, medical insurance and thirty days of annual leave</li></ul>',
    },
  },
];
