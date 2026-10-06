import type { SeedLoyaltyReward } from './types';

/** Rewards of The Shade Circle. Values in halalas; tier ids match restaurant.config.ts. */
export const loyaltyRewards: SeedLoyaltyReward[] = [
  {
    name: { ar: 'دلّة للطاولة', en: 'A dallah for the table' },
    description: {
      ar: 'دلّة قهوةٍ سعودية بالهيل، وصحنٌ من تمر عنيزة، لمن معك على الطاولة كلّهم.',
      en: 'A dallah of Saudi coffee with cardamom and a plate of Unaizah dates, for everyone at your table.',
    },
    pointsCost: 500,
    value: 2500,
    minTier: null,
  },
  {
    name: { ar: 'خبز أبي صالح إلى البيت', en: 'Abu Saleh’s bread, to take home' },
    description: {
      ar: 'سلّةٌ من خبز الصباح كما خرج من التنّور: تميس وصاج وكعك، ومعها سمنٌ من الخرج وعسل سدرٍ من الباحة. تُستلم من البيت قبل الظهر.',
      en: 'A basket of the morning’s bread as it left the tannour: tamees, saj and ka’ak, with ghee from Al-Kharj and sidr honey from Al-Baha. Collect it from the house before noon.',
    },
    pointsCost: 1200,
    value: 6000,
    minTier: null,
  },
  {
    name: { ar: 'ساعة الخولاني لاثنين', en: 'The Khawlani hour for two' },
    description: {
      ar: 'ثلاثة فناجين من ثلاثة مدرّجات في جبال جازان، يقطّرها لكما يوسف على البار، ومعها كليجا من فرن نورة.',
      en: 'Three cups from three terraces in the Jazan mountains, poured for you both at the bar, with kleija from Noura’s oven.',
    },
    pointsCost: 2000,
    value: 12000,
    minTier: 'long-shade',
  },
  {
    name: { ar: 'مقعدٌ على طاولة الشيف', en: 'A seat at the Chef’s Table' },
    description: {
      ar: 'مقعدٌ واحد من ثمانية مقاعد أمام المطبخ، في أمسيةٍ تختارها من أمسيات طاولة الشيف القادمة.',
      en: 'One of eight seats facing the kitchen, on a Chef’s Table evening of your choosing.',
    },
    pointsCost: 10000,
    value: 65000,
    minTier: 'night',
  },
];
