import type { SeedJournalPost } from './types';

/** The journal: four long reads and two recipes. Clean HTML, no links. */
export const journalPosts: SeedJournalPost[] = [
  {
    slug: 'the-day-the-shade-disappears',
    kind: 'article',
    title: { ar: 'اليوم الذي يختفي فيه الظلّ', en: 'The day the shade disappears' },
    excerpt: {
      ar: 'مرّتين في السنة تقف شمس الظهيرة فوق جدّة تماماً، فيخلو فناؤنا من الظلّ دقائق معدودة. ونحن نقيم لذلك غداءً.',
      en: 'Twice a year the noon sun stands directly over Jeddah, and for a few minutes our courtyard has no shade at all. We hold a lunch for it.',
    },
    body: {
      ar: '<p>في أكثر الأيام يكون فناء البلد مزولةً لا يحتاج أحدٌ إلى قراءتها. تُلقي الليمونة ظلّها نحو الغرب في الصباح، ثم تجمعه إلى جذعها عند الظهر، ثم ترسله نحو الشرق طوال العصر حتى يصعد جدار الحجر المنقبي ويغيب في الرواشين. بهذا الظلّ يعرف الطاقم الوقت، وتعرفه القطط أيضاً.</p><p>ومرّتين في السنة يفعل الظلّ شيئاً أغرب من ذلك: يختفي.</p><h2>شمسٌ لا تميل</h2><p>تقع جدّة على نحو إحدى وعشرين درجةً ونصف شمال خطّ الاستواء، أي داخل المدار. ولهذا تمرّ الشمس فوقها عموديّاً يومين في السنة: مرّةً وهي صاعدةٌ نحو الشمال في أواخر مايو، ومرّةً وهي عائدةٌ في منتصف يوليو. يقع ذلك في جدّة نحو الثانية عشرة وعشرين دقيقة في مايو، وقريباً من الثانية عشرة والنصف في يوليو. وفي تلك الدقائق يقف كلّ شيءٍ قائمٍ على ظلّه. ينكمش ظلّ الليمونة حتى يصير حلقةً داكنة حول جذعها، وتُسقط عريشة العنب شبكتها من الضوء على الأرض مستقيمة، ولا يترك كأس الماء على الطاولة ظلّاً، بل دائرةً صغيرة من الضوء تحت قاعدته.</p><p>ويحدث الأمر نفسه فوق مكّة في اليوم ذاته أو قريباً منه، وقد عرف الناس ذلك منذ قرون فاستدلّوا به على القبلة: في تلك اللحظة يشير ظلّك، أينما كنت والشمس طالعة، إلى الجهة المعاكسة لمكّة تماماً. أمّا نحن فغايتنا أبسط: نضع الغداء على المائدة.</p><blockquote>دقائق قليلة يخلو فيها الفناء من الظلّ كلّه، فيرفع كلّ من فيه رأسه في وقتٍ واحد.</blockquote><h2>غداءٌ بلا ظلّ</h2><p>أقمنا «غداء انعدام الظلّ» أوّل مرّةٍ عام ٢٠٢٢، ونصفه مصادفة: لاحظت لميس الأرض الخالية من الظلّ عند الظهر، فطلبت من المطبخ أن يُخرج ما كان جاهزاً. أمّا اليوم فنستعدّ له. يُفتح الفناء عند الثانية عشرة بدل الثانية عشرة والنصف، وتبقى المظلّات مطويّة، وتميل القائمة إلى البارد والحادّ: سمكٌ نيّء من سوق الفجر بالليمون والفلفل الأخضر، ولبنٌ بالخيار والنعناع اليابس، وبطّيخٌ بالجبن الأبيض المالح، وليموناضة ورد الطائف على كثيرٍ من الثلج. ويرسل أبو صالح صينيةً أخيرة من التميس، فلا غداء عندنا بلا خبز.</p><p>وفي اللحظة نفسها لا يلقي أحدٌ كلمة. يتوقّف الطاقم، فيلاحظ الضيوف أنّه توقّف، ويرفع الفناء كلّه رأسه دقيقةً أو اثنتين. ثم يبدأ ظلّ الليمونة يزحف نحو الشرق، ويمضي الغداء.</p><h2>إن أردت أن تكون هناك</h2><p>يقع الغداء القادم في أواخر مايو. والموعد تحدّده الشمس لا نحن، ونفتح حجزه قبل ستين يوماً كأيّ يومٍ آخر. أحضر قبّعة، واجلس حيث شئت؛ فكلّ مقاعد الفناء، هذه المرّة وحدها، في الشمس.</p>',
      en: '<p>Most days, the courtyard in Al-Balad is a sundial nobody needs to read. The lime tree throws its shadow west in the morning, draws it in towards the trunk at noon, and lets it stretch east through the afternoon until it climbs the coral-stone wall and disappears into the rawasheen. The staff tell the time by it. So do the cats.</p><p>Twice a year, the shadow does something stranger. It goes.</p><h2>A sun with nowhere to lean</h2><p>Jeddah sits at about twenty-one and a half degrees north, inside the tropics. That means that on two days a year, once as the sun travels north in late May and again as it comes back in mid-July, it passes directly overhead at noon. In Jeddah the moment falls at around twenty past twelve in May and nearer half past in July. For a few minutes, every upright thing stands on its own shadow. The lime tree’s shade shrinks to a dark ring around the trunk. The vine trellis drops its net of light straight down. A glass of water on the table casts no shadow at all, only a small bright circle under its base.</p><p>The same thing happens over Makkah on the same day or close to it, and for centuries people have used it to find the direction of prayer: at that minute, wherever you are and the sun is up, your shadow points directly away from Makkah. Our purpose is humbler. We put lunch on the table.</p><blockquote>For a few minutes the courtyard has no shade at all, and everyone in it looks up at the same time.</blockquote><h2>Lunch without shade</h2><p>We first held the Zero-Shade Lunch in 2022, half by accident: Lamees noticed the empty floor at noon and asked the kitchen to send out whatever was ready. Now we plan for it. The courtyard opens at noon instead of half past, the umbrellas stay furled, and the menu leans cold and sharp: raw fish from the dawn market with lime and green chilli, laban with cucumber and dried mint, watermelon with salty white cheese, and Taif rose lemonade poured over a great deal of ice. Abu Saleh sends out one last tray of tamees, because there is no lunch here without bread.</p><p>At the moment itself, nobody makes a speech. The staff stop, the guests notice they have stopped, and for a minute or two the whole courtyard looks up. Then the shadow of the lime tree begins to creep out towards the east, and lunch goes on.</p><h2>If you would like to be there</h2><p>The next Zero-Shade Lunch falls in late May. The date is set by the sun, not by us, and bookings open sixty days before, as for any other day. Bring a hat. Sit anywhere: for once, every seat in the courtyard is in the sun.</p>',
    },
    author: { ar: 'لميس قطب', en: 'Lamees Qutub' },
    readingMinutes: 3,
    cover: 'place-courtyard-sun',
    publishedDaysAgo: 80,
  },
  {
    slug: 'abu-saleh-and-the-0440-tannour',
    kind: 'article',
    title: { ar: 'أبو صالح وتنّور الرابعة وأربعين', en: 'Abu Saleh and the 04:40 tannour' },
    excerpt: {
      ar: 'كلّ صباحٍ في الرابعة وأربعين دقيقة، والزقاق بلا ضوء، يوقد كبير خبّازينا التنّور. ذهبنا مبكّرين لنراه.',
      en: 'Every morning at twenty to five, before there is any light in the lane, our head baker lights the oven. We went in early to watch.',
    },
    body: {
      ar: '<p>في الرابعة وأربعين دقيقة يكون الزقاق أمام بيت البلد مظلماً وبارداً تقريباً. لا صوت فيه إلّا قطٌّ فوق خزّان ماء، وصريرُ غطاءٍ معدنيّ يأتي من مكانٍ ما في الداخل. أبو صالح يوقد التنّور.</p><p>فعل ذلك كلّ صباحٍ منذ افتُتح البيت عام ٢٠٢١، وقبلها ثلاثين سنةً في مخبز أبيه على بُعد أزقّةٍ قليلة. لا يضبط منبّهاً، ويقول إنّه لم يحتج إليه قطّ، ولم يضبطه أحدٌ متأخّراً.</p><h2>النار أوّلاً، ثم العجين</h2><p>التنّور أسطوانةٌ من الفخّار مدفونةٌ في بناءٍ من الطوب، مفتوحةٌ من أعلاها، وقاعدتها أوسع من فمها. يوقد أبو صالح النار بحطب السمر وقليلٍ من الفحم، ثم يتركها قرابة ساعةٍ حتى يشرب الفخّار حرارته. وفي هذه الساعة يوقظ العجين الذي عجنه عصر الأمس: عجين التميس ليّنٌ رخو، وعجين الصاج أشدّ منه، وعجين الكعك فيه شيءٌ من المحلب، يُفتل حلقاتٍ ويُغمس في السمسم.</p><p>لا يزن شيئاً. يقرص ويطوي، ويضغط بإبهامه على كرة العجين ثم ينظر كم تسرع في العودة. وإذا أراد أن يعرف أجاهزٌ التنّور أم لا، مدّ ظاهر معصمه فوق فمه وعدّ إلى ثلاثة؛ فإن احتمل حتى الأربعة، فالتنّور يحتاج وقتاً آخر.</p><blockquote>«الخبز مثل الضيوف»، يقول، «إن لم تكن جاهزاً حين يصلون، فلن ينتظروك.»</blockquote><h2>عند السادسة إلا ربعاً</h2><p>يدخل أوّل التميس التنّور نحو الخامسة والنصف. يمدّ أبو صالح كلّ رغيفٍ على مخدّةٍ محشوّة بالقطن، وينقره بأطراف أصابعه، ثم يلصقه بجدار التنّور من الداخل بحركةٍ واحدة أسرع من قراءة هذه الجملة. وبعد ثلاث دقائق ينزعه بخطّافٍ طويل، منتفخاً ومنقّطاً بسواد الجمر. وعند السادسة إلا ربعاً تخرج أوّل سلّةٍ إلى الفناء، حيث يرتّب طاقم الصباح الطاولات، وتبدأ الليمونة تُلقي أوّل ظلّها.</p><p>وحين تبلغ الساعة الحادية عشرة يكون قد خبز نحو ستّمئة رغيف: تميساً للفول، وصاجاً للّبنة، وكعكاً للكرك، وأرغفةً قليلة لم يطلبها أحد، تذهب إلى الجيران في الزقاق.</p><p>ولا يأكل أبو صالح من خبزه شيئاً قبل أن يخرج آخره. يشرب فنجانين من القهوة بلا هيل، ويقف عند باب المخبز يراقب الفناء وهو يمتلئ، ثم يعود إلى عمله. سألناه مرّةً لماذا لا يجلس، فقال إنّ التنّور لا يجلس، ولم نجد ما نردّ به.</p><h2>التلميذة</h2><p>منذ عامٍ صار لأبي صالح من يعينه: خبّازةٌ شابّة بدأت بغسل الصواني، وصارت اليوم تخبز الدفعة الثانية من الصاج وحدها. لا يُكثر من مدحها. لكنّه تركها الشهر الماضي توقد النار، ووقف عند الباب بفنجان قهوته ولم يقل شيئاً؛ وهذا في ذلك المخبز أعلى ما يُقال.</p>',
      en: '<p>At twenty to five the lane outside the Al-Balad house is dark and very nearly cool. The only sounds are a cat on a water tank and, from somewhere inside, the scrape of a metal lid. Abu Saleh is lighting the tannour.</p><p>He has done this every morning since the house opened in 2021, and for thirty years before that in his father’s bakery a few lanes away. He does not set an alarm. He says he has never needed one, and nobody has ever caught him late.</p><h2>Fire first, then dough</h2><p>The tannour is a clay cylinder set into a brick housing, open at the top and wider at the base than at the mouth. Abu Saleh builds the fire with samr wood and a little charcoal, then leaves it for nearly an hour while the clay drinks in the heat. He uses the time to wake the dough he mixed the afternoon before: tamees dough, soft and slack; saj dough, firmer; ka’ak dough with a little mahlab, to be twisted into rings and rolled in sesame.</p><p>Nothing is weighed. He pinches, folds, presses a thumb into a ball of dough and watches how quickly it springs back. When he wants to know whether the oven is ready, he holds the back of his wrist over the mouth and counts to three. If he can reach four, it needs more time.</p><blockquote>“Bread is like guests,” he says. “If you are not ready when they arrive, they will not wait for you.”</blockquote><h2>Quarter to six</h2><p>The first tamees goes in at about half past five. He stretches each round over a cotton-stuffed cushion, dimples it with his fingertips and presses it onto the inside wall of the oven in a single movement that takes less time than reading this sentence. Three minutes later he lifts it off with a long hook, blistered and freckled with char. At quarter to six the first basket leaves for the courtyard, where the morning staff are setting tables and the lime tree is just beginning to throw a shadow.</p><p>By eleven he has baked around six hundred breads: tamees for the foul, saj for the labneh, ka’ak for the karak, and a few loaves nobody ordered, which go to the neighbours on the lane. He eats none of it himself until the last loaf is out. Asked once why he never sits down, he said the tannour does not sit either, and nobody had an answer to that.</p><h2>The apprentice</h2><p>For the past year Abu Saleh has had help: a young baker who started out washing trays and now bakes the second batch of saj on her own. He does not praise her much. But last month he let her light the fire, and stood at the door with his coffee and said nothing at all, which in that bakery is the highest compliment there is.</p>',
    },
    author: { ar: 'ريم بخش', en: 'Reem Bakhsh' },
    readingMinutes: 3,
    cover: 'craft-bread-oven',
    publishedDaysAgo: 150,
  },
  {
    slug: 'khawlani-coffee-from-the-jazan-terraces',
    kind: 'article',
    title: { ar: 'الخولاني: بنٌّ من مدرّجات جازان', en: 'Khawlani: coffee from the Jazan terraces' },
    excerpt: {
      ar: 'ينبت بنّنا على مدرّجاتٍ حجرية في جبال جازان، تحفظ أشجارها عائلاتٌ جيلاً بعد جيل. عاد يوسف إلى هناك في موسم القطاف.',
      en: 'Our coffee grows on stone terraces in the mountains of Jazan, kept by families for generations. Yousef went back for the harvest.',
    },
    body: {
      ar: '<p>يبدأ الطريق الصاعد من ساحل جازان بين مزارع الموز وبساتين المانجو، ثم يأخذ في الارتفاع. وفي أقلّ من ساعة يبرد الهواء عشر درجات، وتصير التلال جبالاً، والجبال درجاً: مئاتٌ من المدرّجات الحجرية الضيّقة، يحمل كلٌّ منها شريطاً من التراب لا يزيد عرضه على غرفة، وعلى كثيرٍ منها شجر البنّ، بأوراقه الداكنة اللامعة وثماره التي تحمرّ في الخريف.</p><p>من هنا جاءت عائلتي، وهنا ينبت البنّ الذي نقدّمه في البيتين. يسمّونه «الخولاني» نسبةً إلى قبائل خولان التي زرعت هذه السفوح منذ زمنٍ لم يدوّنه أحد. وقد انتقلت معرفة زراعته من الآباء إلى الأبناء: كيف يُبنى جدار المدرّج، ومتى يُقلَّم الشجر، وكيف تُجفَّف الثمرة. وقبل سنواتٍ قليلة اعتُرف بهذه المعرفة تراثاً إنسانياً غير مادّي، ففرح المزارعون، ثم عادوا إلى عملهم.</p><h2>مدرّجاً بعد مدرّج</h2><p>لا نشتري الخولاني بالكيس، بل بالمدرّج. فكلّ عائلةٍ نتعامل معها تزرع عدّة مدرّجاتٍ على ارتفاعاتٍ مختلفة، والفرق يظهر في الفنجان: المدرّجات الدنيا، الأدفأ والأقرب إلى السهل، تعطي قهوةً مستديرة فيها شيءٌ من الفاكهة المجفّفة؛ والعليا، في الضباب فوق ألفٍ وخمسمئة متر، تعطي قهوةً أشرق، أقرب إلى المشمش والشاي الأسود. ونُبقي كلّ دفعةٍ وحدها حتى تبلغ البار، فإذا سألت من أين جاء فنجانك، كان الجواب مدرّجاً لا بلداً.</p><blockquote>«الشجرة تتذكّر من قلّمها»، قال لي العمّ حسن، وهي طريقته في أن يقول إنّه يفضّل أن يقلّمها بنفسه.</blockquote><h2>على السطوح</h2><p>يبدأ القطاف في أواخر الخريف ويمتدّ إلى الشتاء. تُقطف الثمار باليد، قليلاً قليلاً كلّما نضج منها شيء، ثم تُفرش على سطوح البيوت لتجفّ في قشرها أسبوعين أو ثلاثة. ولا يضيع القشر: يُنقع مع الزنجبيل فيصير «قِشراً»، تقدّمه لك كلّ جدّةٍ في الجبل قبل أن تخلع نعليك.</p><p>وفي كلّ موسمٍ أعود إلى الجبل أسبوعين. أصعد المدرّجات مع المزارعين في الصباح الباكر قبل أن يشتدّ الحرّ، وأتذوّق معهم ثمار كلّ مدرّجٍ على حدة، ثم نتّفق على السعر جالسين على جدارٍ حجريّ، وفناجين القِشر في أيدينا.</p><p>ونحمّص البنّ في «ظل» على وجهين. للقهوة السعودية نحمّصه تحميصاً خفيفاً جدّاً، أشقر شاحباً، ثم نطبخه بالهيل وأحياناً بشعرةٍ من الزعفران، كما تُقدَّم القهوة في الجزيرة منذ قرون. ولبار التقطير نمضي به أبعد قليلاً، بقدر ما يفتح الفاكهة ولا يُضيّع الجبل.</p><p>فإن جئت إلى بار الخولاني، فاسأل عن مدرّج الأسبوع. سأخبرك بارتفاعه، واسم العائلة التي تزرعه، وكم بقيت ثماره على السطح. لست مضطرّاً إلى الإصغاء، لكنّ القهوة أطيب حين تصغي.</p>',
      en: '<p>The road up from the Jazan coast begins among banana plantations and mango orchards, then starts to climb. Within an hour the air has cooled by ten degrees, the hills have become mountains, and the mountains have been cut into steps: hundreds of narrow stone terraces, each holding a strip of soil no wider than a room, and on many of them, coffee.</p><p>This is where my family comes from, and where the coffee we pour in both houses grows. It is called Khawlani, after the Khawlan tribes who have farmed these slopes for longer than anyone wrote down. The knowledge of growing it, how to build a terrace wall, when to prune, how to dry the cherries, has passed from parents to children, and a few years ago it was recognised internationally as intangible cultural heritage. The farmers were pleased, and then went back to work.</p><h2>A terrace at a time</h2><p>We do not buy Khawlani by the sack. We buy it by the terrace. Each family we work with farms several, at different heights, and the difference shows in the cup. The lower terraces, warmer and closer to the plain, give a rounder coffee with a note of dried fruit; the highest, in the mist above fifteen hundred metres, something brighter, nearer to apricot and black tea. We keep every lot separate all the way to the bar, so that when you ask where your coffee comes from, the answer is a terrace, not a country.</p><blockquote>“A tree remembers who pruned it,” Uncle Hassan told me, which is his way of saying he would rather do it himself.</blockquote><h2>Dried on the roof</h2><p>The harvest begins in late autumn and runs into winter. Cherries are picked by hand, a few at a time as they ripen, then spread on the flat roofs of the houses to dry in their skins for two or three weeks. The husks are not wasted: steeped with ginger they become qishr, which every grandmother on the mountain will offer you before you have taken off your sandals.</p><p>Every season I go back up the mountain for two weeks. I climb the terraces with the farmers early, before the heat, taste each one on its own, and we settle the price sitting on a terrace wall, cups of qishr in hand.</p><p>At Zill we roast the beans two ways. For gahwa they are barely roasted, pale and blond, then brewed with cardamom and sometimes a thread of saffron, as coffee has been served in the Peninsula for centuries. For the pour-over bar we take them a little further, enough to open the fruit without losing the mountain.</p><p>So if you come to the Khawlani bar, ask for the terrace of the week. I will tell you its height, the name of the family who farms it and how long the cherries sat on the roof. You do not have to listen. But the coffee tastes better if you do.</p>',
    },
    author: { ar: 'يوسف المالكي', en: 'Yousef Al-Maliki' },
    readingMinutes: 3,
    cover: 'ing-coffee-beans',
    publishedDaysAgo: 12,
  },
  {
    slug: 'building-the-wadi-hanifah-house',
    kind: 'article',
    title: { ar: 'طينٌ ونخلٌ وضوء: كيف بنينا بيت وادي حنيفة', en: 'Mud, palm and light: building the Wadi Hanifah house' },
    excerpt: {
      ar: 'أردنا لبيتنا الثاني بناءً يبرّد نفسه بنفسه. فوجدنا الجواب في بيوت نجد القديمة، وفي طين الوادي ذاته.',
      en: 'For our second house we wanted a building that cools itself. We found the answer in the old houses of Najd, and in the valley’s own mud.',
    },
    body: {
      ar: '<p>حين بحثنا عن بيتٍ ثانٍ، كنّا نعرف أنّه لن يكون نسخةً من الأوّل. جدّة تبني بالبحر: حجرٌ منقبيّ، ورواشين من الخشب، وبيتٌ يتنفّس النسمة. أمّا الرياض فمناخٌ آخر وحجّةٌ أخرى؛ هواؤها جافّ، وصيفها قاسٍ، ولياليها في الشتاء أبرد ممّا يظنّ الزائر. وقد أجابت بيوت نجد القديمة عن ذلك كلّه منذ قرون، فقرّرنا أن نصغي إليها. وكان الوادي نفسه معلّمنا الأوّل: على ضفّتيه قامت بلداتٌ من الطين عاشت قروناً، وما زالت بقاياها تقف بين النخيل.</p><h2>طينٌ من الوادي</h2><p>جدران بيت وادي حنيفة من اللَّبِن، صُنع على بُعد مئات الأمتار من طينٍ حُفر على حافّة الوادي، وخُلط بالتبن والماء، وضُغط في قوالب من الخشب، ثم تُرك في الشمس ثلاثة أسابيع. ويبلغ سمك الجدار في الطابق الأرضي قرابة سبعين سنتيمتراً. ففي أغسطس، حين تبلغ حرارة الساحة خمساً وأربعين درجة، يبقى الليوان بارداً إلى ما بعد الظهر بكثير، لأنّ الجدار ما زال منشغلاً بحرارة الصباح. وفي يناير يفعل العكس، فيردّ شمس العصر إلى الداخل بعد المغيب بساعات. ويدخل الضيف من الساحة اللاهبة فيشعر بالفرق قبل أن يفهمه.</p><p>والطين يحتاج إلى رعاية. بعد أمطار الشتاء تُلبَّس الجدران الخارجية طبقةً جديدة من الطين، كما كانت تُلبَّس بيوت الوادي كلّها. وصرنا نعدّ ذلك موسماً من مواسمنا.</p><h2>نخلٌ فوق الرؤوس</h2><p>السقوف من جذوع النخل، مشقوقةً ومصفوفةً جنباً إلى جنب، وفوقها طبقةٌ من الجريد، ثم حصيرٌ منسوج، ثم سطحٌ من التراب المرصوص. جاءت الجذوع من نخلٍ كفّ عن الحمل في البساتين المجاورة، وجاء المزارع الذي باعنا إيّاها ليراها تُرفع. ومن الداخل يبدو السقف مضلّعاً دافئ اللون، يحمل الصوت بلين، فلا تعلو الغرفة الممتلئة أبداً. وفي الليل تصير الجذوع شيئاً آخر: تُلقي الفوانيس ظلالها بين الأضلاع، فيبدو السقف كأنّه يتحرّك قليلاً كلّما مرّ نادل.</p><blockquote>بُني البيت ليحجب الشمس، فإذا به يُبنى حولها.</blockquote><h2>مثلّثاتٌ من الضوء</h2><p>كان بنّاؤو نجد يفتحون في أعالي جدرانهم فتحاتٍ مثلّثة صغيرة، تُخرج الهواء الحارّ وتُدخل قليلاً من الضوء. نسخناها، ثم أمضينا سنةً نلاحظ ما تفعل. في الصباح تضع على الجدار الغربي مثلّثاتٍ شاحبة. وعند الظهر تهبط إلى الأرض صغيرةً حادّة. وفي العصر الطويل تمتدّ وتميل وتمشي على مهلٍ نحو الشرق عبر الفناء، حتى تصعد الجدار البعيد وتنطفئ. وعليها يضبط الطاقم قهوة العصر. ويقول راكان إنّه يعرف الساعة من باب المطبخ بفارق عشر دقائق، ولم يكلّف أحدٌ نفسه أن يثبت خطأه.</p>',
      en: '<p>When we went looking for a second house, we knew it would not be a copy of the first. Jeddah builds with the sea: coral stone, timber lattices, a house that breathes the breeze. Riyadh is a different climate and a different argument. The air is dry, the summer is fierce, and winter nights are colder than visitors expect. The old houses of Najd answered all of that centuries ago. We decided to listen to them. The valley itself was our first teacher: along its banks stand mud towns that lasted for centuries, and their remains still rise between the palms.</p><h2>Mud from the valley</h2><p>The walls of the Wadi Hanifah house are mud brick, made a few hundred metres away from clay dug at the edge of the valley, mixed with straw and water, pressed into wooden moulds and left in the sun for three weeks. On the ground floor the walls are nearly seventy centimetres thick. In August, when the yard outside reaches forty-five degrees, the liwan stays cool until well after noon, because the wall is still busy with the morning’s heat. In January it does the opposite, giving the afternoon sun back to the room for hours after dark.</p><p>Mud needs looking after. After the winter rains the outer walls are given a fresh coat of mud plaster, as every house in the valley once was. We have come to think of it as one of our seasons.</p><h2>Palm overhead</h2><p>The ceilings are palm trunks, split and laid side by side, with a layer of fronds, a woven mat and a roof of packed earth above them. The trunks came from palms that had stopped bearing fruit in the groves next door, and the farmer who sold them to us came to watch them go up. From below, the ceiling is ribbed and warm in colour, and it carries sound softly; a full room never quite becomes a loud one. At night the trunks become something else: the lanterns throw shadows between the ribs, and the ceiling seems to shift a little whenever a waiter passes.</p><blockquote>The house was built to keep the sun out. It ended up being built around it.</blockquote><h2>Triangles of light</h2><p>The old Najdi builders made small triangular openings high in their walls, to let hot air out and a little light in. We copied them, then spent a year noticing what they do. In the morning they lay pale triangles on the west wall. At noon the triangles drop to the floor, small and sharp. Through the long afternoon they stretch, tilt and travel slowly east across the courtyard until they climb the far wall and go out. The staff time the afternoon coffee by them. Rakan says he can tell the hour from the kitchen door to within ten minutes, and nobody has troubled to prove him wrong.</p>',
    },
    author: { ar: 'لميس قطب', en: 'Lamees Qutub' },
    readingMinutes: 3,
    cover: 'place-mudbrick',
    publishedDaysAgo: 195,
  },
  {
    slug: 'luqaimat-with-date-syrup',
    kind: 'recipe',
    title: { ar: 'لقيمات بدبس التمر', en: 'Luqaimat with date syrup' },
    excerpt: {
      ar: 'مقرمشةٌ من خارجها، طريّةٌ من داخلها، وأطيب ما تكون في دقائقها الأربع الأولى. وصفة نورة، بمقادير مطبخ البيت.',
      en: 'Crisp outside, soft within, and at their best for about four minutes. Noura’s recipe, scaled for a home kitchen.',
    },
    body: {
      ar: '<p>اللقيمات حلوى الظلّ الطويل: صغيرةٌ مقرمشة دبقة، تنتهي في دقائق. تُصنع في كلّ بيتٍ من بيوت الجزيرة في رمضان، وفي بيتنا كلّ عصر. ولا تقليها نورة إلّا عند الطلب، لأنّ أطيب ما فيها يدوم أربع دقائق بعد خروجها من الزيت.</p><p>وفي البيت القاعدة نفسها: اجمع أهلك حول الطاولة قبل أن تنزل الدفعة الأولى إلى الزيت.</p>',
      en: '<p>Luqaimat are the sweet of the long shade: small, crisp, sticky and gone in minutes. In the Peninsula they are made in every house during Ramadan, and in ours every afternoon. Noura fries them only to order, because they are at their best for about four minutes after leaving the oil.</p><p>At home the rule is the same: have everyone at the table before the first batch goes in.</p>',
    },
    author: { ar: 'نورة حمدان', en: 'Noura Hamdan' },
    readingMinutes: 4,
    cover: 'dish-luqaimat',
    publishedDaysAgo: 45,
    recipe: {
      serves: 6,
      minutes: 90,
      ingredients: [
        { ar: '٢٥٠ غراماً من الطحين الأبيض', en: '250 g plain flour' },
        { ar: 'ملعقةٌ كبيرة من نشا الذرة', en: '1 tbsp cornflour' },
        { ar: 'ملعقةٌ صغيرة من الخميرة الفورية', en: '1 tsp instant yeast' },
        { ar: 'ملعقةٌ صغيرة من السكّر', en: '1 tsp sugar' },
        { ar: 'رشّة ملح، ورشّة هيلٍ مطحون', en: 'A pinch of salt and a pinch of ground cardamom' },
        { ar: 'شعيراتٌ من الزعفران منقوعةٌ في ملعقتين كبيرتين من الماء الدافئ', en: 'A few threads of saffron, steeped in 2 tbsp warm water' },
        { ar: 'نحو ٣٠٠ مل من الماء الدافئ', en: 'About 300 ml warm water' },
        { ar: 'زيتٌ نباتيّ للقلي', en: 'Neutral oil, for frying' },
        { ar: '٦ ملاعق كبيرة من دبس التمر، وملعقةٌ كبيرة من السمسم المحمّص', en: '6 tbsp date syrup and 1 tbsp toasted sesame, to finish' },
      ],
      steps: [
        {
          ar: 'اخلط في وعاءٍ كبير الطحين والنشا والخميرة والسكّر والملح والهيل. أضف ماء الزعفران، ثم الماء الدافئ شيئاً فشيئاً، واضرب بيدك أو بملعقةٍ خشبية حتى تحصل على عجينةٍ كثيفة ملساء لزجة، أرخى من عجين الخبز وأكثف من خليط الفطائر.',
          en: 'In a large bowl, whisk the flour, cornflour, yeast, sugar, salt and cardamom. Add the saffron water, then the warm water a little at a time, beating with your hand or a wooden spoon until you have a thick, smooth, sticky batter: looser than bread dough, thicker than pancake batter.',
        },
        {
          ar: 'غطِّ الوعاء واتركه في مكانٍ دافئ نحو ساعة، حتى يتضاعف حجم العجينة ويمتلئ سطحها بالفقاعات. في مطبخٍ جدّاويّ يكفيها أربعون دقيقة، وفي شتاء الرياض تحتاج أكثر.',
          en: 'Cover and leave somewhere warm for about an hour, until the batter has doubled and its surface is full of bubbles. In a Jeddah kitchen this takes forty minutes; in a Riyadh winter, rather longer.',
        },
        {
          ar: 'سخّن الزيت في قدرٍ عميقة حتى ١٧٠ درجة. جرّبه بقطرةٍ من العجين: ينبغي أن تغوص ثم تطفو في الحال وحولها فقاعاتٌ صغيرة.',
          en: 'Heat the oil in a deep pan to 170°C. Test it with a drop of batter: it should sink, then rise at once, ringed with small bubbles.',
        },
        {
          ar: 'بلّل يدك، واقبض على قليلٍ من العجين، واعصره من الفرجة بين الإبهام والسبّابة، ثم التقط كلّ كرةٍ بملعقةٍ صغيرة مبلّلة وأسقطها في الزيت. اقلِ على دفعات، ولا تزحم القدر.',
          en: 'Wet your hand, take up a fistful of batter and squeeze it out through the gap between thumb and forefinger. Scoop off each ball with a wet teaspoon and drop it into the oil. Fry in batches, without crowding the pan.',
        },
        {
          ar: 'قلّبها باستمرار، وستنقلب وحدها حين يثقل أحد جانبيها. بعد أربع دقائق أو خمس تصير ذهبيةً داكنة، وتسمع لها صوتاً أجوف إذا نقرتها. ارفعها وصفِّها من الزيت.',
          en: 'Keep turning them; once one side is heavy, they will roll over by themselves. After four or five minutes they should be deep gold and sound hollow when tapped. Lift them out and drain.',
        },
        {
          ar: 'دفّئ دبس التمر قليلاً، واسكبه عليها وقلّبها، ثم رشّ السمسم وقدّمها في الحال. لا تجعلها تنتظر؛ فهي أيضاً لن تنتظرك.',
          en: 'Warm the date syrup a little, pour it over and toss, scatter with sesame and serve at once. Do not make them wait; they will not wait for you either.',
        },
      ],
    },
  },
  {
    slug: 'taif-rose-lemonade',
    kind: 'recipe',
    title: { ar: 'ليموناضة ورد الطائف', en: 'Taif rose lemonade' },
    excerpt: {
      ar: 'الليمون يتكلّم أوّلاً، والورد يجيب. مبرّد العصر في البيتين، كما يُعدّ على البار.',
      en: 'The lemon speaks first; the rose answers. The afternoon cooler from both houses, as it is made at the bar.',
    },
    body: {
      ar: '<p>نصنع ليموناضة الورد في البيتين من وردٍ يأتينا من معملٍ عائليّ في الهدا فوق الطائف، حيث تُقطف البتلات في الربيع قبل الشروق، وفيها بقيّةٌ من برد الليل. والورد صوتٌ قويّ، فنستعمله همساً: يتكلّم الليمون أوّلاً، ثم يجيب الورد.</p>',
      en: '<p>The rose lemonade in both houses is made with roses from a family distillery in Al-Hada, above Taif, where the petals are picked in spring before sunrise, while they still hold the cool of the night. Rose is a strong voice, so we use it as a whisper: the lemon speaks first, and the rose answers.</p>',
    },
    author: { ar: 'يوسف المالكي', en: 'Yousef Al-Maliki' },
    readingMinutes: 4,
    cover: 'dish-taif-rose-lemonade',
    publishedDaysAgo: 110,
    recipe: {
      serves: 4,
      minutes: 25,
      ingredients: [
        { ar: '٦ حبّات ليمون، أو ما يكفي لنحو ٢٠٠ مل من العصير', en: '6 lemons, or enough for about 200 ml of juice' },
        { ar: '١٢٠ غراماً من السكّر', en: '120 g sugar' },
        { ar: '١٢٠ مل من الماء للقطر', en: '120 ml water, for the syrup' },
        { ar: 'حفنةٌ من بتلات ورد الطائف المجفّفة الصالحة للأكل', en: 'A handful of dried, food-grade Taif rose petals' },
        { ar: 'ملعقتان كبيرتان من ماء ورد الطائف', en: '2 tbsp Taif rose water' },
        { ar: 'ثماني ورقاتٍ من النعناع الطازج', en: '8 fresh mint leaves' },
        { ar: '٧٥٠ مل من الماء البارد، عاديّاً أو فوّاراً', en: '750 ml cold water, still or sparkling' },
        { ar: 'ثلجٌ وفير', en: 'Plenty of ice' },
      ],
      steps: [
        {
          ar: 'ضع السكّر والماء على نارٍ هادئة حتى يذوب السكّر ويصفو القطر. ارفعه عن النار، وأضف نصف بتلات الورد، وغطِّه عشر دقائق، ثم صفِّه واتركه يبرد.',
          en: 'Warm the sugar and water over a low heat until the sugar dissolves and the syrup is clear. Take it off the heat, add half the rose petals, cover for ten minutes, then strain and leave to cool.',
        },
        {
          ar: 'اعصر الليمون وصفِّ العصير من البذور. يجب أن يكون معك نحو ٢٠٠ مل.',
          en: 'Squeeze the lemons and strain out the pips. You want about 200 ml of juice.',
        },
        {
          ar: 'اخلط في إبريقٍ عصير الليمون وقطر الورد، ثم أضف ماء الورد قطرةً بعد قطرة، وتذوّق في كلّ مرّة. ينبغي أن يأتي الورد بعد الليمون، لا قبله.',
          en: 'In a jug, combine the lemon juice and the rose syrup, then add the rose water a little at a time, tasting as you go. The rose should arrive after the lemon, not before it.',
        },
        {
          ar: 'افرك أوراق النعناع بين أصابعك فركاً خفيفاً، وأضفها إلى الإبريق مع الماء البارد، وحرّك.',
          en: 'Bruise the mint leaves lightly between your fingers, add them to the jug with the cold water and stir.',
        },
        {
          ar: 'املأ الكؤوس بالثلج، واسكب الليموناضة، وانثر فوقها ما بقي من البتلات. قدّمها في الحال، فالماء الفوّار يفقد فقاعاته، والورد يخفت إن انتظر.',
          en: 'Fill the glasses with ice, pour, and scatter over the remaining petals. Serve straight away: sparkling water loses its bubbles, and rose fades if kept waiting.',
        },
      ],
    },
  },
];
