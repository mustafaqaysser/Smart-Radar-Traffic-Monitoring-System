'use client';

import { useLocale, useTranslations } from 'next-intl';
import type { DietProfile } from '@/lib/menu/filter';
import { profileLine, type ProfileLine } from '@/lib/menu/profile-line';
import type { Allergen, DietaryTag } from '@/lib/menu/tags';

/** Builds the line a dish shows against the guest's dietary profile (null without a profile). */
export function useProfileLine(profile: DietProfile | null): (item: { dietary: DietaryTag[]; allergens: Allergen[]; spice: number }) => ProfileLine {
  const t = useTranslations('menu.profile');
  const tc = useTranslations('common');
  const locale = useLocale();
  return (item) => profileLine(item, profile, (k, v) => t(k, v), (k, v) => tc(k, v), locale);
}
