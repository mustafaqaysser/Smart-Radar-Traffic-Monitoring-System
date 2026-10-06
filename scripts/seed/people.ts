/**
 * Staff (one account per role) and fictional guests. All emails use the .test domain.
 */
import { hashPassword } from 'better-auth/crypto';
import * as s from '../../src/lib/db/schema';
import { FAMILY_NAMES, FIRST_NAMES } from './names';
import { ids } from './world';
import type { SeedContext } from './types';

export const DEMO_PASSWORD = 'zill-demo-2026';

export const STAFF: { role: s.Role; email: string; name: string; branch: string | null }[] = [
  { role: 'owner', email: 'owner@zill.test', name: 'Lamees Qutub', branch: null },
  { role: 'manager', email: 'manager@zill.test', name: 'Reem Bakhsh', branch: null },
  { role: 'host', email: 'host@zill.test', name: 'Dana Al-Ghamdi', branch: 'al-balad' },
  { role: 'kitchen', email: 'kitchen@zill.test', name: 'Majid Hawsawi', branch: 'al-balad' },
  { role: 'waiter', email: 'waiter@zill.test', name: 'Omar Fallatah', branch: 'al-balad' },
  { role: 'editor', email: 'editor@zill.test', name: 'Sara Al-Qahtani', branch: null },
];

export interface SeededCustomer {
  id: string;
  name: string;
  email: string;
  phone: string;
  locale: 'ar' | 'en';
  branch: string;
}

async function insertUserWithPassword(ctx: SeedContext, user: typeof s.users.$inferInsert, passwordHash: string) {
  await ctx.db.insert(s.users).values(user);
  await ctx.db.insert(s.accounts).values({ id: `ac_${user.id}`, accountId: user.id as string, providerId: 'credential', userId: user.id as string, password: passwordHash });
}

export async function seedPeople(ctx: SeedContext): Promise<{ customers: SeededCustomer[]; demoCustomer: SeededCustomer }> {
  const { random, now } = ctx;
  const hash = await hashPassword(DEMO_PASSWORD);

  for (const staff of STAFF) {
    await insertUserWithPassword(
      ctx,
      {
        id: `us_${staff.role}`,
        name: staff.name,
        email: staff.email,
        emailVerified: true,
        role: staff.role,
        locale: staff.role === 'kitchen' || staff.role === 'waiter' ? 'ar' : 'en',
        branchId: staff.branch ? ids.branch(staff.branch) : null,
        createdAt: new Date(now.getTime() - 400 * 864e5),
      },
      hash,
    );
  }

  // The demo guest account used by reviewers and end-to-end tests.
  const demoCustomer: SeededCustomer = { id: 'us_guest', name: 'Nora Al-Harbi', email: 'guest@zill.test', phone: '+966505550111', locale: 'ar', branch: 'al-balad' };
  await insertUserWithPassword(
    ctx,
    {
      id: demoCustomer.id,
      name: demoCustomer.name,
      email: demoCustomer.email,
      emailVerified: true,
      role: 'customer',
      phone: demoCustomer.phone,
      locale: 'ar',
      dietary: { diets: [], avoidAllergens: ['nuts'], maxSpice: 2 },
      marketingEmail: true,
      tags: ['regular'],
      createdAt: new Date(now.getTime() - 200 * 864e5),
    },
    hash,
  );
  await ctx.db.insert(s.addresses).values([
    { id: 'ad_guest_home', userId: demoCustomer.id, label: 'البيت', zoneId: 'dz_al-balad_1', area: 'الحمراء', street: 'شارع الأمير فهد، مبنى ١٢', building: '12', floor: '3', notes: 'البوابة الخلفية', isDefault: true },
    { id: 'ad_guest_work', userId: demoCustomer.id, label: 'Office', zoneId: 'dz_al-balad_0', area: 'Al-Baghdadiyah', street: 'Gulf Road, Building 4', building: '4', floor: '1', isDefault: false },
  ]);
  await ctx.db.insert(s.favorites).values([
    { userId: demoCustomer.id, itemId: ids.item('tamees-foul') },
    { userId: demoCustomer.id, itemId: ids.item('kunafa-nabulsi') },
    { userId: demoCustomer.id, itemId: ids.item('khawlani-pour-over') },
  ]);

  const customers: SeededCustomer[] = [demoCustomer];
  const used = new Set<string>();
  for (let i = 0; i < 180; i++) {
    const [firstAr, firstEn] = random.pick(FIRST_NAMES);
    const [familyAr, familyEn] = random.pick(FAMILY_NAMES);
    const locale: 'ar' | 'en' = random.chance(0.72) ? 'ar' : 'en';
    let email = `${firstEn}.${familyEn}`.toLowerCase().replace(/[^a-z.]/g, '');
    while (used.has(email)) email = `${email}${random.int(1, 9)}`;
    used.add(email);
    const c: SeededCustomer = {
      id: `us_c${String(i).padStart(3, '0')}`,
      name: locale === 'ar' ? `${firstAr} ${familyAr}` : `${firstEn} ${familyEn}`,
      email: `${email}@guest.test`,
      phone: `+9665${random.pick(['0', '3', '4', '5', '6'])}555${String(random.int(1000, 9999))}`,
      locale,
      branch: random.chance(0.55) ? 'al-balad' : 'wadi-hanifah',
    };
    customers.push(c);
    const hasAccount = random.chance(0.45);
    if (hasAccount) {
      await ctx.db.insert(s.users).values({
        id: c.id,
        name: c.name,
        email: c.email,
        emailVerified: true,
        role: 'customer',
        phone: c.phone,
        locale,
        dietary: random.chance(0.2) ? { diets: [random.pick(['vegetarian', 'gluten-free', 'dairy-free'])], avoidAllergens: random.chance(0.5) ? [random.pick(['nuts', 'sesame', 'milk', 'gluten'])] : [] } : null,
        marketingEmail: random.chance(0.4),
        tags: random.chance(0.08) ? ['vip'] : null,
        createdAt: new Date(now.getTime() - random.int(30, 500) * 864e5),
      });
    }
  }
  return { customers, demoCustomer };
}
