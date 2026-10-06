import type { SeedTeamMember } from './types';

/** The people of the houses. Photographed only by their hands and backs. */
export const team: SeedTeamMember[] = [
  {
    name: { ar: 'لميس قطب', en: 'Lamees Qutub' },
    role: { ar: 'الشيف المؤسِّسة', en: 'Chef and founder' },
    bio: {
      ar: 'نشأت لميس في مطبخٍ جدّاويّ كان الغداء فيه يُضبط على أذان الظهر، والعشاء على نسمة البحر. طبخت في مطابخ غيرها اثنتي عشرة سنة، ثم فتحت «ظل» عام ٢٠٢١ في بيتٍ من الحجر المنقبي في البلد. ما زالت تكتب كلّ قائمةٍ بخطّ يدها، فلا تُدخل فيها شيئاً قبل موسمه، ولا تُبقيه بعده. وإذا سألتها عن طبقها المفضّل، سألتك: في أيّ ساعة؟',
      en: 'Lamees grew up in a Jeddah kitchen where lunch was timed by the noon call to prayer and dinner by the sea breeze. She cooked in other people’s restaurants for twelve years, then opened Zill in a coral-stone house in Al-Balad in 2021. She still writes every menu by hand. Nothing goes on before its season. Ask for her favourite dish and she will ask you: at what hour?',
    },
    image: 'craft-plating',
    branch: null,
  },
  {
    name: { ar: 'راكان الحارثي', en: 'Rakan Al-Harthi' },
    role: { ar: 'رئيس الطهاة، وادي حنيفة', en: 'Chef de cuisine, Wadi Hanifah' },
    bio: {
      ar: 'جاء راكان من مطبخ فندقٍ كبير في الرياض، كان يطبخ فيه لستّمئة ضيف ويتمنّى أن يطبخ لستّين. افتتح بيت وادي حنيفة عام ٢٠٢٤ بجريشٍ من قمح حائل، وبقاعدةٍ ورثها عن جدّته في الخرج: القِدر تخبرك متى تنضج، لا الساعة. يقف على الجمر بنفسه ليالي الخميس، ويعرف كلّ نخلةٍ تطلّ على الفناء باسمها تقريباً.',
      en: 'Rakan came from a large hotel kitchen in Riyadh, where he cooked for six hundred and wished he were cooking for sixty. He opened the Wadi Hanifah house in 2024 with a jareesh of Hail wheat and a rule from his grandmother in Al-Kharj: the pot tells you when it is ready, not the clock. On Thursday nights he works the embers himself.',
    },
    image: 'craft-chef-back',
    branch: 'wadi-hanifah',
  },
  {
    name: { ar: 'أبو صالح بامطرف', en: 'Saleh “Abu Saleh” Bamatraf' },
    role: { ar: 'كبير الخبّازين', en: 'Head baker' },
    bio: {
      ar: 'في الرابعة وأربعين دقيقة، والزقاق ما زال مظلماً، يوقد أبو صالح التنّور. فعلها كلّ صباحٍ منذ افتُتح بيت البلد، وقبل ذلك ثلاثين سنةً في مخبز أبيه. يزن العجين بكفّيه، ويقيس الحرارة بظاهر معصمه، ولا يثق بميزان الحرارة. ويخرج أوّل الخبز من التنّور عند السادسة إلا ربعاً. يناديه المطبخ كلّه «أبو صالح»، ولا أحد يذكر من بدأ.',
      en: 'At 04:40, while the lane is still dark, Abu Saleh lights the tannour. He has done it every morning since the Al-Balad house opened, and for thirty years before that in his father’s bakery. He weighs dough with his palms and judges heat with the back of his wrist. The first bread leaves the oven at quarter to six. Nobody remembers who first called him Abu Saleh.',
    },
    image: 'craft-bread-oven',
    branch: 'al-balad',
  },
  {
    name: { ar: 'نورة حمدان', en: 'Noura Hamdan' },
    role: { ar: 'رئيسة قسم الحلويات', en: 'Pastry chef' },
    bio: {
      ar: 'درست نورة الحلويات الكلاسيكية في الخارج، ثم عادت لتجد كليجا جدّتها أطيب من كلّ ما تعلّمته. حلوياتها تبدأ من الجزيرة: تمر عنيزة، وعسل السدر من الباحة، وورد الطائف من معملٍ عائليّ في الهدا. لا تقلي اللقيمات إلّا عند الطلب، لأنّ أطيب ما فيها يدوم أربع دقائق، ولا ترضى أن تقدّم الدقيقة الخامسة.',
      en: 'Noura trained in classical pastry abroad and came home to find her grandmother’s kleija still better than anything she had learned. Her sweets begin in the Peninsula: Unaizah dates, sidr honey from Al-Baha, roses from a family distillery in Al-Hada. She fries luqaimat only to order, because they are at their best for about four minutes, and she will not serve the fifth.',
    },
    image: 'craft-kneading',
    branch: 'al-balad',
  },
  {
    name: { ar: 'يوسف المالكي', en: 'Yousef Al-Maliki' },
    role: { ar: 'مسؤول القهوة', en: 'Coffee lead' },
    bio: {
      ar: 'زرعت عائلة يوسف البنّ في مدرّجات جبال جازان منذ زمنٍ لم يدوّنه أحد. يزور المزارعين في كلّ موسم قطاف، ويشتري الخولاني بالمدرّج لا بالكيس، ويحمّصه على وجهين: أشقرَ شاحباً للقهوة السعودية بالهيل، وأبعدَ قليلاً لبار التقطير. اسأله عن ارتفاع المدرّج الذي جاء منه فنجانك، وسيخبرك. وقد يخبرك وإن لم تسأل، ومعه اسم المزارع.',
      en: 'Yousef’s family has grown coffee on the terraces above Jazan for longer than anyone wrote down. He visits the farmers every harvest, buys Khawlani by the terrace rather than the sack, and roasts it two ways: pale and blond for gahwa with cardamom, a little further for the pour-over bar. Ask him the altitude of your cup and he will tell you. Sometimes he tells you anyway.',
    },
    image: 'craft-pour-over',
    branch: null,
  },
  {
    name: { ar: 'ريم بخش', en: 'Reem Bakhsh' },
    role: { ar: 'المديرة العامة', en: 'General manager' },
    bio: {
      ar: 'تدير ريم البيتين من مكتبٍ صغير بجوار باب المطبخ في البلد، وهناك بالضبط تحبّ أن تكون. تعرف أيّ الضيوف يفضّل ركن الليوان، ومن يشرب قهوته بلا هيل، ومتى يحتاج الفناء مروحةً أخرى. وفي الرابعة من كلّ عصر، حين يبدأ الظلّ الطويل، تمشي بين الطاولات، ولا بدّ أن تنقل واحدةً منها على الأقل.',
      en: 'Reem runs both houses from a small desk beside the Al-Balad kitchen door, which is exactly where she wants to be. She knows which regulars like the corner of the liwan, who takes gahwa without cardamom, and when the courtyard needs another fan. Every afternoon at four, as the long shade begins, she walks the floor and moves at least one table.',
    },
    image: 'craft-server-hands',
    branch: null,
  },
];
