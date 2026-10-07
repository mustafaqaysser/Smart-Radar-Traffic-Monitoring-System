import 'server-only';
import type Anthropic from '@anthropic-ai/sdk';
import { z } from 'zod';
import restaurantConfig from '@config';
import type { CurrentUser } from '@/lib/auth/session';
import { isServing } from '@/lib/domain/menus';
import { openState, scheduleForDate } from '@/lib/domain/hours';
import { formatMoney } from '@/lib/i18n/format';
import { tr } from '@/lib/i18n/localized';
import { toDietProfile } from '@/lib/menu/filter';
import { visibleItems } from '@/lib/menu/view';
import { getBranches, getSeasonalModes, hoursFor } from '@/lib/queries/branches';
import { getMenuCatalog } from '@/lib/queries/catalog';
import type { BranchDTO, MenuCatalog } from '@/lib/queries/types';
import { AREA_CHOICES, OCCASIONS } from '@/lib/reserve/types';
import { bookFromHold, bookingPath, bookingWindow, DEPOSIT_WINDOW_MINUTES, depositFor, findSlots, holdSlot, manageUrl } from '@/lib/server/reservations';
import { featureEnabled, getSettings } from '@/lib/server/settings';
import { normalizePhone } from '@/lib/services/phone';
import { limitByIp } from '@/lib/services/rate-limit';
import { absoluteUrl } from '@/lib/site/url';
import { getServingContext } from '@/lib/site/serving';
import { formatTime, getZonedParts, toDateString } from '@/lib/time/zoned';
import { createId } from '@/lib/utils/id';

/** The model the concierge runs on (override with CONCIERGE_MODEL). */
export const CONCIERGE_MODEL = process.env.CONCIERGE_MODEL?.trim() || 'claude-opus-5-5';

/** On only when the owner turns the flag on and an Anthropic API key is configured. */
export function conciergeEnabled(): Promise<boolean> {
  return featureEnabled('aiConcierge');
}

const WEEKDAYS = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
/** 'HH:MM' at an instant in a house's time zone. */
const clock = (at: Date, timeZone: string) => {
  const p = getZonedParts(at, timeZone);
  return formatTime(p.hour * 60 + p.minute);
};
const sar = (minor: number) => `${(minor / 100).toFixed(minor % 100 ? 2 : 0)} ${restaurantConfig.currency}`;

function weekly(ranges: { weekday: number; opens: string; closes: string }[]): string {
  return WEEKDAYS.map((d, i) => {
    const today = ranges.filter((r) => r.weekday === i).map((r) => `${r.opens}–${r.closes}`);
    return `${d} ${today.length ? today.join(', ') : 'closed'}`;
  }).join('; ');
}

function menuBlock(catalog: MenuCatalog, branches: BranchDTO[], allowAlcohol: boolean): string {
  const allowed = new Set(visibleItems(catalog, allowAlcohol).map((i) => i.slug));
  const lines: string[] = [];
  for (const m of catalog.menus) {
    if (m.seasonalModeId) continue;
    const where = m.branchIds ? branches.filter((b) => m.branchIds?.includes(b.id)).map((b) => b.slug).join(', ') : 'both houses';
    const when = m.schedule.length ? m.schedule.map((w) => `${w.weekdays.map((d) => WEEKDAYS[d]).join('/')} ${w.start}–${w.end}`).join('; ') : 'all day';
    lines.push(`\n## ${tr(m.name, 'en')} / ${tr(m.name, 'ar')} (menu "${m.slug}") — ${when} — ${where}${m.isTasting && m.tastingPrice ? ` — tasting menu ${sar(m.tastingPrice)} per guest` : ''}`);
    for (const c of m.categories) {
      lines.push(`### ${tr(c.name, 'en')} / ${tr(c.name, 'ar')}`);
      for (const slug of c.items) {
        const i = catalog.items[slug];
        if (!i || !allowed.has(slug)) continue;
        lines.push(
          [
            `- ${tr(i.name, 'en')} / ${tr(i.name, 'ar')} [${slug}]`,
            `${sar(i.price)}`,
            `allergens: ${i.allergens.length ? i.allergens.join(', ') : 'none of the 14'}`,
            `diets: ${i.dietary.length ? i.dietary.join(', ') : '—'}`,
            `heat ${i.spiceLevel}/3`,
            i.calories ? `about ${i.calories} kcal` : null,
            i.orderable ? null : 'served at the house only',
          ]
            .filter(Boolean)
            .join(' | '),
        );
        lines.push(`  EN: ${tr(i.description, 'en')}`);
        lines.push(`  AR: ${tr(i.description, 'ar')}`);
      }
    }
  }
  return lines.join('\n');
}

/**
 * The part of the instructions that is the same for every guest and every minute: voice, rules, the houses
 * and the whole menu. It is sent first and marked for prompt caching, so repeated conversations reuse it.
 */
export async function stableInstructions(): Promise<string> {
  const [catalog, branches, settings] = await Promise.all([getMenuCatalog(), getBranches(), getSettings()]);
  const r = restaurantConfig.reservations;
  const houses = branches
    .map((b) =>
      [
        `- ${b.slug}: ${tr(b.name, 'en')} / ${tr(b.name, 'ar')}, ${tr(b.city, 'en')}. ${tr(b.address, 'en')}. Phone ${b.phone}${b.whatsapp ? `, WhatsApp ${b.whatsapp}` : ''}.`,
        `  Opening hours: ${weekly(b.venueHours)}. Kitchen: ${weekly(b.kitchenHours)}.`,
        b.parking ? `  Parking: ${tr(b.parking, 'en')}` : null,
        b.accessibility ? `  Access: ${tr(b.accessibility, 'en')}` : null,
        `  Takes bookings online: ${b.reservationsEnabled ? 'yes' : 'no'}. Delivery: ${b.deliveryEnabled ? 'yes' : 'no'}. Pickup: ${b.pickupEnabled ? 'yes' : 'no'}.`,
      ]
        .filter(Boolean)
        .join('\n'),
    )
    .join('\n');
  return `You are the concierge of ${restaurantConfig.name.en} (${restaurantConfig.name.ar}), a restaurant with two houses in Saudi Arabia. Its idea is "Every hour has its shade": the menu follows the sun, so what is served changes through the day.

# How you speak
- Warm, brief and precise, like a good host: usually two to five short sentences.
- Plain text only, no Markdown. When you list dishes, put each on its own line starting with "· ".
- Reply in the language of the guest's latest message: elegant Modern Standard Arabic, or English. Use the dish names in that language.
- Prices are in Saudi riyals and include VAT.
- Never invent dishes, prices, ingredients, opening hours or availability. If the answer is not in these instructions or in a tool result, say you do not know and suggest calling the house.
- You only help with this restaurant: its menu, its houses, bookings, ordering, gatherings, gift cards and private dining.

# Allergies and diets
- Each dish lists which of the 14 major allergens it contains (gluten, crustaceans, eggs, fish, peanuts, soybeans, milk, nuts = tree nuts, celery, mustard, sesame, sulphites, lupin, molluscs) and the diets it suits.
- When a guest mentions an allergy or a diet, suggest only dishes whose listed allergens exclude it, and name the dishes to avoid and why.
- The kitchens share equipment, so there is always some risk of cross-contact. Never call a dish "completely safe"; always ask the guest to tell their server about allergies.
- Heat goes from 0 (none) to 3 (hot).

# What is served now
- Use the menu_now tool for what is being served at a house right now, what is sold out today and whether the house is open. Each menu below lists its serving hours in the house's local time.

# Booking a table
- Always call check_availability before offering times, and offer only times it returns.
- To book you need: the house, date, time, party size, and the guest's name, email and mobile number. A seating area (${AREA_CHOICES.join(', ')}), an occasion (${OCCASIONS.join(', ')}) and notes are optional.
- Before calling create_reservation, repeat every detail back in one short message and ask the guest to confirm. Call create_reservation only when the guest's latest message clearly confirms (for example "yes", "confirm", "نعم", "أكّد").
- Online bookings are for parties of up to ${r.maxPartyOnline}. For larger parties do not book; suggest private dining (/private-dining) or calling the house.
- Parties of ${r.deposit.minParty} or more pay a deposit of ${r.deposit.perGuest} ${restaurantConfig.currency} per guest online; the table is held for ${DEPOSIT_WINDOW_MINUTES} minutes until it is paid. Give the guest the payment link from the tool result.
- Bookings open ${r.bookingWindowDays} days ahead. Guests can change or cancel online up to ${r.modifyCutoffHours} hours before, using the link in their confirmation email.
- Dates and times are always in the house's local time.

# Elsewhere on the site
- Ordering for delivery or pickup: /order. Gatherings and tickets: /experiences. Gift cards: /gift-cards. Private dining and catering: /private-dining. The full menu with filters: /menu.
- Links you give must start with the guest's language prefix, given below (for example /ar/menu or /en/menu).

# The houses
${houses}

# The menus${menuBlock(catalog, branches, settings.features.alcohol)}`;
}

/** The part that changes per request (time at each house, the guest), placed after the cached instructions. */
export async function requestContext(locale: string, user: CurrentUser | null, now = new Date()): Promise<string> {
  const branches = await getBranches();
  const times = branches
    .map((b) => {
      return `${b.slug}: ${toDateString(now, b.timeZone)} (${WEEKDAYS[getZonedParts(now, b.timeZone).weekday]}) ${clock(now, b.timeZone)}`;
    })
    .join('; ');
  const profile = user ? toDietProfile(user.dietary) : null;
  const guest = user
    ? `The guest is signed in as ${user.name} (email ${user.email}${user.phone ? `, mobile ${user.phone}` : ''}). Use these details for a booking unless they give others.${
        profile ? ` Their dietary profile: ${[profile.avoid.length ? `avoids ${profile.avoid.join(', ')}` : null, profile.diets.length ? `eats ${profile.diets.join(', ')}` : null, profile.maxSpice !== null ? `heat up to ${profile.maxSpice}/3` : null].filter(Boolean).join('; ')}. Apply it to every suggestion.` : ''
      }`
    : 'The guest is not signed in.';
  return `Now: ${times}.\nThe guest is using the ${locale === 'ar' ? 'Arabic' : 'English'} site: link prefix /${locale}.\n${guest}`;
}

// ————————————————————————————————————————— tools —————————————————————————————————————————

export const CONCIERGE_TOOLS: Anthropic.Beta.BetaTool[] = [
  {
    name: 'menu_now',
    description: "What a house is serving right now: whether it is open, which menus are on, and which dishes are sold out or not served there today. Call it before saying what is available now.",
    eager_input_streaming: true,
    input_schema: {
      type: 'object',
      properties: { house: { type: 'string', description: 'House slug, e.g. al-balad or wadi-hanifah' } },
      required: ['house'],
      additionalProperties: false,
    },
  },
  {
    name: 'check_availability',
    description: 'Free table times at a house for a date and party size (times are local to the house). Call it before offering any time.',
    eager_input_streaming: true,
    input_schema: {
      type: 'object',
      properties: {
        house: { type: 'string', description: 'House slug' },
        date: { type: 'string', description: 'YYYY-MM-DD in the house time zone' },
        party_size: { type: 'integer', minimum: 1, maximum: 40 },
        area: { type: 'string', enum: [...AREA_CHOICES], description: 'Seating preference; "any" when the guest has none' },
      },
      required: ['house', 'date', 'party_size'],
      additionalProperties: false,
    },
  },
  {
    name: 'create_reservation',
    description: 'Books a table. Only call after check_availability offered the time and the guest explicitly confirmed every detail in their latest message.',
    eager_input_streaming: true,
    input_schema: {
      type: 'object',
      properties: {
        house: { type: 'string' },
        date: { type: 'string', description: 'YYYY-MM-DD' },
        time: { type: 'string', description: 'HH:MM, 24-hour, house local time' },
        party_size: { type: 'integer', minimum: 1, maximum: 40 },
        area: { type: 'string', enum: [...AREA_CHOICES] },
        occasion: { type: 'string', enum: [...OCCASIONS] },
        name: { type: 'string' },
        email: { type: 'string' },
        phone: { type: 'string' },
        notes: { type: 'string', description: 'Requests for the house (optional)' },
        dietary_notes: { type: 'string', description: 'Allergies or diets for the kitchen (optional)' },
      },
      required: ['house', 'date', 'time', 'party_size', 'name', 'email', 'phone'],
      additionalProperties: false,
    },
  },
];

const houseSlug = z.string().trim().min(2).max(40);
const MenuNowInput = z.object({ house: houseSlug });
const AvailabilityInput = z.object({ house: houseSlug, date: z.string().regex(/^\d{4}-\d{2}-\d{2}$/), party_size: z.number().int().min(1).max(40), area: z.enum(AREA_CHOICES).optional() });
const ReservationInput = AvailabilityInput.extend({
  time: z.string().regex(/^([01]\d|2[0-3]):[0-5]\d$/),
  occasion: z.enum(OCCASIONS).optional(),
  name: z.string().trim().min(2).max(80),
  email: z.email().max(254),
  phone: z.string().trim().min(6).max(32),
  notes: z.string().trim().max(500).optional(),
  dietary_notes: z.string().trim().max(300).optional(),
});

/** A reservation the concierge made, for the chat's booking card. */
export interface ConciergeBooking {
  code: string;
  house: string;
  date: string;
  time: string;
  partySize: number;
  depositDue: boolean;
  href: string;
}

export type ToolOutcome = { content: string; isError: boolean; booking?: ConciergeBooking };

async function branchBySlug(slug: string): Promise<BranchDTO | null> {
  return (await getBranches()).find((b) => b.slug === slug) ?? null;
}

const fail = (message: string): ToolOutcome => ({ content: JSON.stringify({ error: message }), isError: true });

/** Runs one tool call after validating its input (inputs stream unbuffered, so nothing is trusted unchecked). */
export async function runConciergeTool(name: string, input: unknown, ctx: { locale: string; user: CurrentUser | null; now: Date }): Promise<ToolOutcome> {
  if (name === 'menu_now') {
    const parsed = MenuNowInput.safeParse(input);
    if (!parsed.success) return { content: JSON.stringify({ INVALID_JSON: JSON.stringify(input) }), isError: true };
    const branch = await branchBySlug(parsed.data.house);
    if (!branch) return fail('Unknown house. Use al-balad or wadi-hanifah.');
    const [serving, catalog, modes] = await Promise.all([getServingContext(branch), getMenuCatalog(), getSeasonalModes()]);
    const state = openState(hoursFor(branch, modes), ctx.now);
    const items = Object.values(catalog.items).filter((i) => i.isActive);
    return {
      isError: false,
      content: JSON.stringify({
        house: branch.slug,
        localTime: clock(ctx.now, branch.timeZone),
        open: state.open,
        closesAt: state.closesAt ? clock(state.closesAt, branch.timeZone) : null,
        opensNext: state.opensAt ? `${toDateString(state.opensAt, branch.timeZone)} ${clock(state.opensAt, branch.timeZone)}` : null,
        servingNow: serving.menus.filter((m) => isServing(m, ctx.now, branch.timeZone)).map((m) => m.slug),
        todaysMenus: serving.menus.map((m) => m.slug),
        soldOutToday: items.filter((i) => i.branches[branch.id]?.soldOut).map((i) => i.slug),
        notServedHere: items.filter((i) => i.branches[branch.id] && !i.branches[branch.id]?.available).map((i) => i.slug),
        orderingPaused: branch.orderingPaused,
      }),
    };
  }

  if (name === 'check_availability') {
    const parsed = AvailabilityInput.safeParse(input);
    if (!parsed.success) return { content: JSON.stringify({ INVALID_JSON: JSON.stringify(input) }), isError: true };
    const d = parsed.data;
    const branch = await branchBySlug(d.house);
    if (!branch) return fail('Unknown house. Use al-balad or wadi-hanifah.');
    if (!branch.reservationsEnabled) return fail('This house does not take bookings online; suggest calling it.');
    const r = restaurantConfig.reservations;
    if (d.party_size > r.maxPartyOnline) return fail(`Parties above ${r.maxPartyOnline} book through private dining or by phone.`);
    const window = bookingWindow(branch, ctx.now);
    if (d.date < window.first || d.date > window.last) return fail(`Bookings are open from ${window.first} to ${window.last}.`);
    const modes = await getSeasonalModes();
    const day = scheduleForDate(hoursFor(branch, modes), d.date);
    const slots = await findSlots(branch, d.date, d.party_size, d.area ?? 'any', ctx.now);
    const free = slots.filter((s) => s.available);
    return {
      isError: false,
      content: JSON.stringify({
        house: branch.slug,
        date: d.date,
        partySize: d.party_size,
        closed: day.closed || slots.length === 0,
        availableTimes: free.map((s) => s.time),
        areasByTime: Object.fromEntries(free.map((s) => [s.time, s.areas])),
        deposit: depositFor(d.party_size) > 0 ? `${sar(depositFor(d.party_size))} paid online to confirm` : null,
      }),
    };
  }

  if (name === 'create_reservation') {
    const parsed = ReservationInput.safeParse(input);
    if (!parsed.success) return { content: JSON.stringify({ INVALID_JSON: JSON.stringify(input), issues: parsed.error.issues.map((i) => i.path.join('.')) }), isError: true };
    const d = parsed.data;
    const rl = await limitByIp('concierge-book', 3, 3600);
    if (!rl.ok) return fail('Too many bookings from this connection in the last hour; ask the guest to call the house.');
    const branch = await branchBySlug(d.house);
    if (!branch || !branch.reservationsEnabled) return fail('This house does not take bookings online.');
    if (d.party_size > restaurantConfig.reservations.maxPartyOnline) return fail('Party too large to book online.');
    const phone = normalizePhone(d.phone, restaurantConfig.country);
    if (!phone) return fail('That mobile number does not look valid; ask the guest to check it.');
    const sessionKey = `concierge:${createId()}`;
    const hold = await holdSlot({ branch, date: d.date, time: d.time, partySize: d.party_size, area: d.area ?? 'any', sessionKey, now: ctx.now, minutes: 5 });
    if (!hold) return fail('That time is no longer free. Check availability again and offer other times.');
    const result = await bookFromHold({
      holdId: hold.id,
      sessionKey,
      now: ctx.now,
      details: {
        name: d.name,
        email: d.email.toLowerCase(),
        phone,
        occasion: d.occasion ?? null,
        notes: d.notes || null,
        dietaryNotes: d.dietary_notes || null,
        locale: ctx.locale,
        userId: ctx.user?.id ?? null,
        source: 'concierge',
      },
    });
    if (!result.ok) return fail('That time was just taken. Check availability again.');
    const res = result.reservation;
    const confirmation = absoluteUrl(`/${ctx.locale}${bookingPath(res)}`);
    return {
      isError: false,
      booking: { code: res.code, house: tr(branch.shortName, ctx.locale), date: res.date, time: res.time, partySize: res.partySize, depositDue: result.deposit > 0, href: `/${ctx.locale}${bookingPath(res)}` },
      content: JSON.stringify({
        booked: true,
        code: res.code,
        status: result.deposit > 0 ? 'held until the deposit is paid' : 'confirmed',
        deposit: result.deposit > 0 ? formatMoney(result.deposit, 'en') : null,
        confirmationAndPaymentLink: confirmation,
        manageLink: manageUrl(res),
        email: 'A confirmation email with a calendar invitation is on its way.',
      }),
    };
  }

  return fail(`Unknown tool ${name}.`);
}
