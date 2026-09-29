import 'server-only';
import { cache } from 'react';
import restaurantConfig from '@config';
import type { FeatureFlag, OrderChannel } from '@/lib/config/define';
import { db } from '@/lib/db/client';
import { settings as settingsTable } from '@/lib/db/schema';
import type { LocalizedText } from '@/lib/i18n/localized';

export interface SocialLink {
  id: string;
  label: string;
  url: string;
}

export interface SiteSettings {
  features: Record<FeatureFlag, boolean>;
  maintenance: { enabled: boolean; message: LocalizedText };
  contact: { email: string; press: string; careers: string; events: string; privacy: string };
  tax: { rate: number; pricesIncludeTax: boolean; registrationNumber: string };
  serviceCharge: { rate: number; channels: OrderChannel[] };
  tips: { enabled: boolean; presets: number[] };
  notifications: { orderEmail: string | null; reservationEmail: string | null; inquiryEmail: string | null };
  seo: { title: LocalizedText; description: LocalizedText };
  social: SocialLink[];
}

export function defaultSettings(): SiteSettings {
  return {
    features: { ...restaurantConfig.features },
    maintenance: {
      enabled: false,
      message: {
        ar: 'نُعيد ترتيب الفناء. نعود خلال ساعات قليلة، والبيتان مفتوحان كالعادة.',
        en: 'We are rearranging the courtyard. Back within a few hours — both houses are open as usual.',
      },
    },
    contact: { ...restaurantConfig.contact },
    tax: { rate: restaurantConfig.tax.rate, pricesIncludeTax: restaurantConfig.tax.pricesIncludeTax, registrationNumber: restaurantConfig.tax.registrationNumber },
    serviceCharge: { rate: restaurantConfig.serviceCharge.rate, channels: [...restaurantConfig.serviceCharge.channels] },
    tips: { enabled: restaurantConfig.tips.enabled, presets: [...restaurantConfig.tips.presets] },
    notifications: { orderEmail: null, reservationEmail: null, inquiryEmail: null },
    seo: { title: { ...restaurantConfig.tagline }, description: { ...restaurantConfig.description } },
    social: [],
  };
}

type SettingKey = keyof SiteSettings;

/** Settings from the database merged over the defaults in restaurant.config.ts (one query per request). */
export const getSettings = cache(async (): Promise<SiteSettings> => {
  const base = defaultSettings();
  const rows = await db.select().from(settingsTable);
  const merged = base as unknown as Record<string, unknown>;
  for (const row of rows) {
    const key = row.key as SettingKey;
    if (!(key in base)) continue;
    const current = merged[key];
    merged[key] = current && typeof current === 'object' && !Array.isArray(current) && row.value && typeof row.value === 'object' && !Array.isArray(row.value)
      ? { ...(current as object), ...(row.value as object) }
      : row.value;
  }
  return merged as unknown as SiteSettings;
});

export async function saveSetting<K extends SettingKey>(key: K, value: SiteSettings[K]): Promise<void> {
  await db
    .insert(settingsTable)
    .values({ key, value })
    .onConflictDoUpdate({ target: settingsTable.key, set: { value, updatedAt: new Date() } });
}

/** A feature is on when its flag is on and anything it depends on is configured. */
export async function featureEnabled(flag: FeatureFlag): Promise<boolean> {
  const s = await getSettings();
  if (!s.features[flag]) return false;
  if (flag === 'aiConcierge') return Boolean(process.env.ANTHROPIC_API_KEY);
  return true;
}
