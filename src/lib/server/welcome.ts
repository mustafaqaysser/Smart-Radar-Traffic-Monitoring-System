import 'server-only';
import { eq, sql } from 'drizzle-orm';
import { getTranslations } from 'next-intl/server';
import restaurantConfig from '@config';
import { db } from '@/lib/db/client';
import { loyaltyTransactions, users } from '@/lib/db/schema';
import { plural } from '@/lib/i18n/plural';
import { mail } from '@/lib/server/mail';
import { featureEnabled } from '@/lib/server/settings';
import { absoluteUrl } from '@/lib/site/url';
import { createId } from '@/lib/utils/id';

/**
 * A new account (password or email code): the language it signed up in, the membership welcome bonus, and a
 * welcome email. Runs once, from the auth layer's user-created hook.
 */
export async function welcomeMember(user: { id: string; email: string; name: string }, locale: 'ar' | 'en'): Promise<void> {
  const bonus = (await featureEnabled('loyalty')) ? restaurantConfig.loyalty.signupBonus : 0;
  await db
    .update(users)
    .set({ locale, ...(bonus > 0 ? { loyaltyPoints: sql`${users.loyaltyPoints} + ${bonus}`, lifetimePoints: sql`${users.lifetimePoints} + ${bonus}` } : {}) })
    .where(eq(users.id, user.id));
  if (bonus > 0) await db.insert(loyaltyTransactions).values({ id: createId(), userId: user.id, kind: 'bonus', points: bonus, note: 'Welcome' });
  const tu = await getTranslations({ locale, namespace: 'common.units' });
  await mail(user.email, { name: 'welcome', props: { locale, name: user.name.trim(), bonusLabel: bonus > 0 ? tu('points', plural(bonus, locale)) : null, menuUrl: absoluteUrl(`/${locale}/menu`) } });
}
