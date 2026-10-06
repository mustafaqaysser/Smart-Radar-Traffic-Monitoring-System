import type { SeedEvent } from './types';

/** Dawn bread, coffee at the bar, music in the long shade, and the long tables. Prices per ticket in halalas. */

const dawnBread: Pick<SeedEvent, 'kind' | 'title' | 'summary' | 'body' | 'branch' | 'startTime' | 'durationMinutes' | 'capacity' | 'image' | 'tickets'> = {
  kind: 'class',
  title: { ar: 'خبز الفجر مع أبي صالح', en: 'Dawn bread with Abu Saleh' },
  summary: {
    ar: 'ساعتان ونصف عند التنّور، من أوّل عجنةٍ حتى الفطور في الفناء.',
    en: 'Two and a half hours at the tannour, from the first dough to breakfast in the courtyard.',
  },
  body: {
    ar: '<p>حين تصل في السادسة والنصف يكون التنّور قد اشتعل منذ قرابة ساعتين، وقد خرج أوّل الخبز إلى الفناء. والآن جاء دورك.</p><p>سيريك أبو صالح كيف يُمدّ التميس على المخدّة ثم يُلصق بجدار التنّور، وكيف يُقلب الصاج بأطراف الأصابع، ولماذا يُفتل الكعك على هذا النحو دون غيره. ستخرج والطحين على كمّيك وحرارة التنّور على وجهك. وينتهي الدرس في التاسعة بفطورٍ في الفناء: خبزك أنت، وفولٌ بالكمّون، وسمنٌ من الخرج، وعسل سدرٍ من الباحة. أمّا ما خبزته بيدك فتأخذه معك إلى البيت، ملفوفاً في قماشٍ كما يُلفّ خبزنا.</p>',
    en: '<p>By the time you arrive at half past six, the tannour has been burning for nearly two hours and the first bread has already gone out to the courtyard. Now it is your turn.</p><p>Abu Saleh will show you how tamees is stretched over the cushion and pressed onto the wall of the oven, how saj is turned with the fingertips, and why ka’ak is twisted the way it is and no other. Expect flour on your sleeves and heat on your face. The class ends at nine with breakfast in the courtyard: your own bread, foul with cumin, ghee from Al-Kharj and sidr honey from Al-Baha. What you bake goes home with you, wrapped in cloth like ours.</p>',
  },
  branch: 'al-balad',
  startTime: '06:30',
  durationMinutes: 150,
  capacity: 10,
  image: 'craft-bread-oven',
  tickets: [{ name: { ar: 'مقعدٌ عند التنّور', en: 'A place at the tannour' }, price: 22000 }],
};

export const events: SeedEvent[] = [
  {
    slug: 'long-shade-oud-al-balad',
    kind: 'gathering',
    title: { ar: 'عصرٌ وعودٌ في الظلّ الطويل', en: 'A long shade afternoon, with oud' },
    summary: {
      ar: 'قهوةٌ وحلو وعازف عود في الفناء، والظلّ يطول على الأرض.',
      en: 'Coffee, sweets and an oud player in the courtyard while the shade lengthens across the floor.',
    },
    body: {
      ar: '<p>في الرابعة والنصف يبلغ ظلّ الليمونة منتصف الفناء، وعندها يبدأ العود. ساعتان يجلس فيهما العازف تحت عريشة العنب، يعزف من قديم الأغاني الحجازية، ويرتجل على المقامات، ويتبع ما يطلبه المكان وما يطلبه الهواء.</p><p>تشمل التذكرة دلّة قهوةٍ أو فنجان خولاني مقطّراً، وصينيةً من حلو العصر من مطبخ نورة حمدان: لقيماتٌ تُقلى عند الطلب، وكليجا، وكعكة التمر بالطحينة. العزف بلا مكبّرات صوت، ولهذا لا نتجاوز ثلاثين ضيفاً، ونرجو ألّا يرنّ هاتفٌ في المقطع الأخير. والأطفال مرحّبٌ بهم، ما داموا يحبّون العود ويحتملون السكوت قليلاً.</p>',
      en: '<p>At half past four the shadow of the lime tree reaches the middle of the courtyard, and that is when the oud begins. For two hours the player sits under the vine and plays old Hijazi songs, improvisations on the maqam, and whatever the courtyard seems to ask for.</p><p>Your ticket covers a dallah of gahwa or a Khawlani pour-over, and a tray of afternoon sweets from Noura Hamdan’s kitchen: luqaimat fried to order, kleija and date cake with tahini. The music is unamplified, so we keep the courtyard to thirty guests and ask that no phone rings in the last song. Children are welcome, as long as they like the oud and can bear a little quiet.</p>',
    },
    branch: 'al-balad',
    daysFromNow: 2,
    startTime: '16:30',
    durationMinutes: 120,
    capacity: 30,
    image: 'place-arch-shadow',
    tickets: [{ name: { ar: 'مقعدٌ في الفناء', en: 'A seat in the courtyard' }, price: 12000 }],
  },
  { ...dawnBread, slug: 'dawn-bread-october', daysFromNow: 4 },
  {
    slug: 'sundial-night-wadi-hanifah',
    kind: 'tasting',
    title: { ar: 'ليلة المزولة في وادي حنيفة', en: 'A Sundial night in Wadi Hanifah' },
    summary: {
      ar: 'قائمة التذوّق بأطباقها السبعة، من الفجر إلى منتصف الليل، على مائدةٍ طويلة تحت سقف جذوع النخل.',
      en: 'The seven-course tasting menu, from dawn to midnight, at one long table under the palm-trunk ceiling.',
    },
    body: {
      ar: '<p>تمشي «المزولة» مع نهارٍ واحد في سبعة أطباق. تبدأ عند الفجر بتمرٍ وقهوةٍ وقشطة، وتنتهي عند منتصف الليل بكأس شاي ونعناع وتمرةٍ أخرى. وبينهما خبز التنّور بالسمن والعسل، وسمكٌ نيّء بالليمون والفلفل الأخضر، وأرزٌ بالزعفران مع لبنٍ مدخّن، ولحمٌ باللومي من على الجمر، ثم وردٌ وفستقٌ وقشطة.</p><p>الليلة يطبخها راكان الحارثي في الرياض، على مائدةٍ واحدة طويلة تحت سقف جذوع النخل، ويخبو الضوء في فتحات الجدار شيئاً فشيئاً حتى تُكمل الفوانيس ما بدأته الشمس. ومن شاء أضاف مرافقة القهوة والشاي، يختار فيها يوسف المالكي فنجاناً لكلّ طبق. نكيّف الأطباق مع الحساسية إن أخبرتنا قبل ٤٨ ساعة.</p>',
      en: '<p>The Sundial follows a single day across seven courses. It begins at dawn with a date, coffee and cream, and ends at midnight with a glass of mint tea and one more date. In between come tannour bread with ghee and honey, raw fish with lime and green chilli, saffron rice with smoked yoghurt, lamb with dried lime from the embers, then rose, pistachio and cream.</p><p>Tonight Rakan Al-Harthi cooks it in Riyadh, at one long table under the palm-trunk ceiling, while the light in the wall vents fades and the lanterns finish what the sun began. The second ticket adds a cup for each course, chosen by Yousef Al-Maliki. Allergies can be accommodated with 48 hours’ notice.</p>',
    },
    branch: 'wadi-hanifah',
    daysFromNow: 6,
    startTime: '19:30',
    durationMinutes: 150,
    capacity: 24,
    image: 'dish-sundial-maghrib',
    tickets: [
      { name: { ar: 'المزولة', en: 'The Sundial' }, price: 48500 },
      {
        name: { ar: 'المزولة مع مرافقة القهوة والشاي', en: 'The Sundial with coffee and tea pairing' },
        description: {
          ar: 'سبعة فناجين وكؤوس: خولاني مقطّر، وقهوةٌ سعودية، وكرك، وشاي بالنعناع، وما بينها.',
          en: 'Seven cups and glasses: Khawlani pour-over, gahwa, karak, mint tea and what lies between.',
        },
        price: 56000,
      },
    ],
  },
  {
    slug: 'khawlani-hour-wadi-hanifah',
    kind: 'class',
    title: { ar: 'ساعة الخولاني', en: 'The Khawlani Hour' },
    summary: {
      ar: 'ساعةٌ ونصف على البار مع يوسف المالكي: ثلاثة مدرّجات، وتحميصتان، وغلّايةٌ واحدة.',
      en: 'Ninety minutes at the bar with Yousef Al-Maliki: three terraces, two roasts, one kettle.',
    },
    body: {
      ar: '<p>ينبت البنّ الخولاني على مدرّجاتٍ حجرية ضيّقة في جبال جازان، يزورها الضباب أكثر العصاري ويغادرها قبل المغيب. وهناك زرعته عائلة يوسف المالكي جيلاً بعد جيل. في هذا الدرس يأتي يوسف بثلاث دفعاتٍ من ثلاثة مدرّجات، ويقدّم كلّاً منها مرّتين: مقطّرةً بتحميصٍ خفيف، ثم قهوةً سعودية بالهيل.</p><p>ستتعلّم الوزن والطحن والصبّ، وتتذوّق ما تصنعه مئتا مترٍ من الارتفاع في فنجانٍ واحد. اثنا عشر مقعداً على البار، والكليجا إلى جانب الفنجان. والتذكرة الثانية تضيف كيساً من البنّ الذي أحببته أكثر من غيره، لتعيد الدرس في بيتك صباح الغد.</p>',
      en: '<p>Khawlani coffee grows on narrow stone terraces in the mountains of Jazan, where the mist arrives most afternoons and leaves before sunset. Yousef Al-Maliki’s family has grown it there for generations. In this class he brings three lots from three terraces and serves each one twice: first as a light-roast pour-over, then as gahwa with cardamom.</p><p>You will learn to weigh, grind and pour, and taste what two hundred metres of altitude do to a single cup. There are twelve seats at the bar, with kleija beside each cup. The second ticket adds a bag of whichever beans you liked best, so you can repeat the lesson at home the next morning.</p>',
    },
    branch: 'wadi-hanifah',
    daysFromNow: 9,
    startTime: '16:00',
    durationMinutes: 90,
    capacity: 12,
    image: 'craft-pour-over',
    tickets: [
      { name: { ar: 'مقعدٌ على البار', en: 'A seat at the bar' }, price: 18000 },
      {
        name: { ar: 'مقعدٌ وكيسٌ من البنّ', en: 'A seat and a bag of beans' },
        description: { ar: '٢٥٠ غراماً من الدفعة التي تختارها.', en: '250 g of the lot you choose.' },
        price: 23000,
      },
    ],
  },
  {
    slug: 'chefs-table-al-balad',
    kind: 'chefs_table',
    title: { ar: 'طاولة الشيف في البلد', en: 'Chef’s Table, Al-Balad' },
    summary: {
      ar: 'ثمانية مقاعد أمام المطبخ، وقائمةٌ تكتبها لميس قطب في الأسبوع نفسه.',
      en: 'Eight seats facing the kitchen, and a menu Lamees Qutub writes that same week.',
    },
    body: {
      ar: '<p>ثمانية مقاعد إلى منضدةٍ من الحجر المنقبي، تواجه خطّ التقديم. تطبخ لكم لميس قطب بنفسها، من قائمةٍ تكتبها في الأسبوع ذاته بحسب ما أرسله سوق الفجر والمزارع: سمكٌ من قوارب الصباح، وآخر رمّان الطائف، وأوّل التمر الجديد.</p><p>نحو عشرة أطباقٍ صغيرة تأتي واحداً بعد واحد، ومعها حكاياتٌ لا يدور كلّها حول الطعام. القهوة والشاي المرافقان للأطباق مشمولان في التذكرة، وتنتهي الأمسية في الفناء بدلّة قهوةٍ تحت الليمونة. ولأنّ القائمة تُكتب لكم أنتم الثمانية، نرجو أن تخبرونا بأيّ حساسيةٍ قبل ٤٨ ساعة، فلا نكتشفها على الطاولة.</p>',
      en: '<p>Eight seats at a counter of coral stone, facing the pass. Lamees Qutub cooks for you herself, from a menu she writes that week around whatever the dawn market and the farms have sent: fish from the morning boats, the last of the Taif pomegranates, the first of the new dates.</p><p>Around ten small courses arrive one at a time, with stories that are only partly about the food. The coffee and tea pairing is included, and the evening ends in the courtyard with a dallah under the lime tree. Because the menu is written for the eight of you, please tell us about any allergies 48 hours ahead, so we do not discover them at the table.</p>',
    },
    branch: 'al-balad',
    daysFromNow: 12,
    startTime: '19:30',
    durationMinutes: 180,
    capacity: 8,
    image: 'craft-plating',
    tickets: [{ name: { ar: 'مقعدٌ على طاولة الشيف', en: 'A seat at the Chef’s Table' }, price: 65000 }],
  },
  {
    slug: 'bread-morning-children',
    kind: 'class',
    title: { ar: 'صباح الخبز للصغار', en: 'A bread morning for children' },
    summary: {
      ar: 'يشكّل الأطفال من الخامسة إلى الحادية عشرة خبزهم بأيديهم مع فريق المخبز، ثم يأكلونه في الفناء.',
      en: 'Children aged five to eleven shape their own bread with the bakery team, then eat it in the courtyard.',
    },
    body: {
      ar: '<p>مرّةً في الشهر يفتح المخبز بابه لأصغر الخبّازين في البيت. يأخذ كلّ طفلٍ من الخامسة إلى الحادية عشرة مريلةً وكرةً من العجين ومكاناً إلى المائدة الطويلة، ويريه فريق المخبز كيف يفرد ويشكّل ويرشّ السمسم، وكيف يحكم إن كان رغيفه أقرب إلى القمر أم إلى الجمل.</p><p>وبينما يدخل الخبز التنّور، والتنّور بعيدٌ آمنٌ خلف الفريق، يصنع الأطفال صحون اللبنة وأكواب الفاكهة. ثم يجلس الجميع إلى الفطور في الفناء. ويبقى الأهل على مقربةٍ مع قهوتهم وفطورهم، وتلك هي التذكرة الثانية. ويعود كلّ طفلٍ إلى بيته برغيفه، وبشهادةٍ صغيرة يوقّعها أبو صالح.</p>',
      en: '<p>Once a month the bakery opens its door to the smallest bakers in the house. Each child gets an apron, a ball of dough and a place at the long table, and the bakery team shows them how to roll, shape and scatter sesame, and how to decide whether a loaf looks more like a moon or a camel.</p><p>While the bread goes into the tannour, which stays safely behind the team, the children make labneh plates and fruit cups. Then everyone eats in the courtyard. Parents stay close by with their own coffee and breakfast; that is the second ticket. Every child goes home with a loaf and a small certificate signed by Abu Saleh.</p>',
    },
    branch: 'al-balad',
    daysFromNow: 16,
    startTime: '09:00',
    durationMinutes: 90,
    capacity: 24,
    image: 'craft-kneading',
    tickets: [
      {
        name: { ar: 'خبّازٌ صغير', en: 'Young baker' },
        description: { ar: 'من الخامسة إلى الحادية عشرة؛ تشمل المريلة والفطور.', en: 'Ages five to eleven; apron and breakfast included.' },
        price: 9000,
        capacity: 14,
      },
      {
        name: { ar: 'مرافقٌ بالغ', en: 'Accompanying adult' },
        description: { ar: 'فطورٌ وقهوةٌ في الفناء.', en: 'Breakfast and coffee in the courtyard.' },
        price: 6000,
        capacity: 10,
      },
    ],
  },
  {
    slug: 'date-harvest-supper',
    kind: 'gathering',
    title: { ar: 'عشاء الصِّرام', en: 'The date-harvest supper' },
    summary: {
      ar: 'مائدةٌ واحدة طويلة على حافة النخيل، وقائمةٌ تدور حول تمر عنيزة الجديد.',
      en: 'One long table at the edge of the palm grove, and a menu built around the new dates from Unaizah.',
    },
    body: {
      ar: '<p>في نجدٍ يسمّون جنيَ التمر «الصِّرام»، وهو لأسابيع قليلة يقرّر ما يأكله الناس. وصلت هذا العام أولى الصناديق من مزرعة العائلة في عنيزة مبكّرة، فمددنا مائدةً طويلة في ساحة النخيل بجوار بيت وادي حنيفة، وطبخنا حولها.</p><p>ستجد رطباً مع القهوة، وكتف خروفٍ مدهوناً بدبس التمر على جمر السمر، وأرزاً حساوياً أحمر، وسلطةً من التمر والأعشاب والرمّان، وكعكة التمر بكراميل الطحينة من فرن نورة. وتجلس معنا الليلة العائلة التي زرعت هذا التمر. وأحضر معك سترةً خفيفة؛ فالوادي يبرد سريعاً بعد العشاء.</p>',
      en: '<p>In Najd the date harvest is called the siram, and for a few weeks it decides what everyone eats. This year the first boxes from our family farm in Unaizah arrived early, so we are laying one long table in the palm yard beside the Wadi Hanifah house and cooking around them.</p><p>Expect fresh rutab with gahwa, lamb shoulder glazed with date molasses over samr embers, red Hassawi rice, a salad of dates, herbs and pomegranate, and Noura’s date cake with tahini caramel. The family who grew the dates will be at the table with us. Bring a light jacket: the valley cools quickly once the sun has gone.</p>',
    },
    branch: 'wadi-hanifah',
    daysFromNow: 20,
    startTime: '19:00',
    durationMinutes: 180,
    capacity: 40,
    image: 'place-palm-grove',
    tickets: [
      { name: { ar: 'مقعدٌ إلى المائدة', en: 'A seat at the table' }, price: 38000 },
      {
        name: { ar: 'مقعدٌ لطفل', en: 'A child’s seat' },
        description: { ar: 'دون الثانية عشرة، بصحنٍ على قدره.', en: 'Under twelve, with a plate to match.' },
        price: 15000,
        capacity: 10,
      },
    ],
  },
  {
    slug: 'long-shade-oud-wadi-hanifah',
    kind: 'gathering',
    title: { ar: 'عصرٌ وعودٌ بين النخيل', en: 'A long shade afternoon, with oud, among the palms' },
    summary: {
      ar: 'عزفٌ على العود في فناء الطين، ومثلّثات الضوء تمشي على الأرض.',
      en: 'Oud in the mud-brick courtyard while triangles of light walk across the floor.',
    },
    body: {
      ar: '<p>في عصر الرياض تُسقط فتحات الجدار مثلّثاتٍ من الضوء على أرض الفناء، تميل ثم تطول ثم تصعد الجدار المقابل حتى تنطفئ. في هذا الوقت بالذات يجلس عازف العود تحت سقف جذوع النخل، فيعزف من الألحان النجدية القديمة، ومن السامري حين يحين له أن يُعزف، ويترك بين المقطوعات صمتاً يسمع فيه الضيوف سعف النخيل.</p><p>تشمل التذكرة دلّة قهوةٍ وتمر عنيزة، وصينيةً من حلو العصر: لقيماتٌ بدبس التمر، وكليجا، وكعكة التمر بالطحينة. ثلاثون مقعداً بين الفناء والليوان، والعزف بلا مكبّرات صوت، كما كان يُعزف في البيوت.</p>',
      en: '<p>On a Riyadh afternoon the vents in the walls drop triangles of light onto the courtyard floor. They lean, lengthen, then climb the opposite wall until they go out. That is exactly when the oud player sits down under the palm-trunk ceiling and plays old Najdi melodies, a samri when the moment asks for one, and leaves enough silence between pieces for the guests to hear the palm fronds.</p><p>Your ticket includes a dallah of gahwa with Unaizah dates and a tray of afternoon sweets: luqaimat with date syrup, kleija and date cake with tahini. Thirty seats between the courtyard and the liwan, and no amplification.</p>',
    },
    branch: 'wadi-hanifah',
    daysFromNow: 23,
    startTime: '16:30',
    durationMinutes: 120,
    capacity: 30,
    image: 'place-palm-shadow-1',
    tickets: [{ name: { ar: 'مقعدٌ في الفناء', en: 'A seat in the courtyard' }, price: 12000 }],
  },
  {
    slug: 'sundial-night-al-balad',
    kind: 'tasting',
    title: { ar: 'ليلة المزولة في البلد', en: 'A Sundial night in Al-Balad' },
    summary: {
      ar: 'سبعة أطباق تمشي مع النهار، تطبخها لميس قطب في الفناء الذي بدأ فيه كلّ شيء.',
      en: 'Seven courses that follow the day, cooked by Lamees Qutub in the courtyard where it all began.',
    },
    body: {
      ar: '<p>وُلدت «المزولة» في هذا الفناء، حين لاحظت لميس قطب أنّ ظلّ الليمونة يعبر الأرض في يومٍ واحد كما تعبر الأطباق المائدة في ليلةٍ واحدة. سبعة أطباق: تمرٌ وقهوةٌ وقشطة للفجر، وخبز التنّور للصباح، وسمكٌ نيّء من سوق الفجر للظهيرة، وأرزٌ بالزعفران للظلّ الطويل، ولحمٌ باللومي للمغرب، ووردٌ وفستقٌ لليل، وشاي بالنعناع وتمرةٌ واحدة لمنتصفه.</p><p>تُقدَّم الليلة تحت عريشة العنب، ونسمة البحر تدخل من الزقاق بعد العشاء. ولمن يشاء مرافقة القهوة والشاي. نكيّف الأطباق مع الحساسية إن أخبرتنا قبل ٤٨ ساعة.</p>',
      en: '<p>The Sundial was born in this courtyard, when Lamees Qutub noticed that the shadow of the lime tree crosses the floor in a day the way courses cross a table in an evening. Seven of them: dates, coffee and cream for dawn; tannour bread for morning; raw fish from the dawn market for noon; saffron rice for the long shade; lamb with dried lime for sunset; rose and pistachio for night; and mint tea with a single date for midnight.</p><p>Tonight it is served under the vine, with the sea breeze finding its way in from the lane after dark. A coffee and tea pairing is available. Allergies can be accommodated with 48 hours’ notice.</p>',
    },
    branch: 'al-balad',
    daysFromNow: 27,
    startTime: '19:30',
    durationMinutes: 150,
    capacity: 24,
    image: 'dish-sundial-dawn',
    tickets: [
      { name: { ar: 'المزولة', en: 'The Sundial' }, price: 48500 },
      {
        name: { ar: 'المزولة مع مرافقة القهوة والشاي', en: 'The Sundial with coffee and tea pairing' },
        description: {
          ar: 'فنجانٌ أو كأسٌ لكلّ طبق، يختاره يوسف المالكي.',
          en: 'A cup or glass for each course, chosen by Yousef Al-Maliki.',
        },
        price: 56000,
      },
    ],
  },
  { ...dawnBread, slug: 'dawn-bread-november', daysFromNow: 32 },
  {
    slug: 'chefs-table-wadi-hanifah',
    kind: 'chefs_table',
    title: { ar: 'طاولة الشيف في وادي حنيفة', en: 'Chef’s Table, Wadi Hanifah' },
    summary: {
      ar: 'ثمانية مقاعد حول الجمر، وراكان الحارثي يطبخ لكم ما جاء به الأسبوع.',
      en: 'Eight seats around the embers, with Rakan Al-Harthi cooking whatever the week has brought.',
    },
    body: {
      ar: '<p>في مطبخ وادي حنيفة ثمانية مقاعد تحيط بموقد الجمر، قريبةٌ بما يكفي لتسمع صوت الشحم على حطب السمر. يطبخ راكان الحارثي قائمةً لا تتكرّر: جريشٌ من قمح حائل، ولبنٌ من ألبان الخرج، وما وصل من البحر ذلك الصباح، وخضارٌ من مزارع الوادي.</p><p>نحو عشرة أطباقٍ صغيرة، يقدّم راكان كلّاً منها بنفسه، ويجيب عن كلّ سؤال، إلّا سؤالاً واحداً عن خلطة جدّته للبهارات. القهوة والشاي المرافقان مشمولان، وتُختم الأمسية في المجلس بدلّة قهوةٍ وتمرٍ من عنيزة. ونرجو أن تخبرنا بأيّ حساسيةٍ قبل ٤٨ ساعة.</p>',
      en: '<p>In the Wadi Hanifah kitchen, eight seats circle the ember hearth, close enough to hear fat meet samr wood. Rakan Al-Harthi cooks a menu that is never repeated: jareesh of Hail wheat, laban from the dairy near Al-Kharj, whatever came up from the coast that morning, and vegetables from the farms along the valley.</p><p>Around ten small courses, each brought to you by Rakan himself, who will answer any question except the one about his grandmother’s spice mix. The coffee and tea pairing is included, and the evening ends in the majlis with a dallah and dates. Please tell us about any allergies 48 hours ahead.</p>',
    },
    branch: 'wadi-hanifah',
    daysFromNow: 41,
    startTime: '19:30',
    durationMinutes: 180,
    capacity: 8,
    image: 'craft-embers',
    tickets: [{ name: { ar: 'مقعدٌ على طاولة الشيف', en: 'A seat at the Chef’s Table' }, price: 65000 }],
  },
  {
    slug: 'khawlani-hour-al-balad',
    kind: 'class',
    title: { ar: 'ساعة الخولاني في البلد', en: 'The Khawlani Hour, Al-Balad' },
    summary: {
      ar: 'يأتي يوسف المالكي إلى جدّة بقطاف الموسم الجديد، ويقطّره لكم في الليوان.',
      en: 'Yousef Al-Maliki brings the new harvest to Jeddah and pours it for you in the liwan.',
    },
    body: {
      ar: '<p>كلّ موسم قطاف يعود يوسف المالكي من جبال جازان بأكياسٍ صغيرة لا تكفي البار، فيجعلها درساً. في هذه الجلسة يقطّر في الليوان ثلاث دفعاتٍ من القطاف الجديد، كلّ واحدةٍ من مدرّجٍ مختلف، ويقارنها بقهوةٍ سعودية من البنّ نفسه، محمّصاً أشقر ومطبوخاً بالهيل.</p><p>ستتعلّم كيف تزن وتطحن وتصبّ، ولماذا يبرد الفنجان الجيّد ولا يفقد طعمه، بل يكشف منه أكثر. وتُقدَّم إلى جانب القهوة كليجا من فرن نورة حمدان. اثنا عشر مقعداً، والليوان بارد حتى في عصر جدّة. والتذكرة الثانية تضيف كيساً من البنّ الذي تختاره.</p>',
      en: '<p>Every harvest Yousef Al-Maliki comes back from the Jazan mountains with a few small sacks, never enough for the bar, so he turns them into a class. In this session he pours three lots from the new harvest in the liwan, each from a different terrace, and sets each against gahwa made from the same beans, roasted pale and brewed with cardamom.</p><p>You will learn to weigh, grind and pour, and why a good cup does not lose its taste as it cools but shows you more of it. Kleija from Noura’s oven comes on the side. Twelve seats, and the liwan stays cool even on a Jeddah afternoon. The second ticket adds a bag of the beans you choose.</p>',
    },
    branch: 'al-balad',
    daysFromNow: 58,
    startTime: '16:00',
    durationMinutes: 90,
    capacity: 12,
    image: 'dish-khawlani-pour-over',
    tickets: [
      { name: { ar: 'مقعدٌ في الليوان', en: 'A seat in the liwan' }, price: 18000 },
      {
        name: { ar: 'مقعدٌ وكيسٌ من البنّ', en: 'A seat and a bag of beans' },
        description: { ar: '٢٥٠ غراماً من الدفعة التي تختارها.', en: '250 g of the lot you choose.' },
        price: 23000,
      },
    ],
  },
];
