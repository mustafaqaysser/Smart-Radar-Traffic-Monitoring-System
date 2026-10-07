# Decisions log

One line of rationale per decision. Newest at the bottom of each section. Never rewrite past entries — supersede them.

## Step 0 — repository

| # | Decision | Rationale |
| --- | --- | --- |
| D-001 | The branch already contained an unrelated project (Smart Radar Traffic Monitoring). It was moved with `git mv` to `legacy/smart-radar-traffic-monitoring/`, untouched. | The brief needs the repo root for the platform (README, `npm install` at root); moving keeps the owner's files and history intact instead of deleting them. |
| D-002 | The legacy README advertises "Download" links to two `.zip` archives committed in the repo. They were not opened, extracted or executed, and nothing in the platform references them. | That README pattern is a common malware-lure signature on GitHub; the owner should inspect those archives before trusting them. |
| D-003 | TypeScript 6.0.x instead of the `latest` 7.0.x tag. | `typescript-eslint` (required for strict linting) supports `typescript <6.1.0`; TS 7 is the native port and breaks the lint toolchain. |
| D-004 | Drizzle ORM 0.45.x (latest stable) rather than the 1.0 beta line. | The brief asks for latest *stable*; 1.0 is still published under beta tags. |
| D-005 | Node.js 22 LTS (the runtime present in the build image) is the minimum; `.nvmrc` and `engines` pin `>=22`. | Current LTS line available here; all scripts are plain Node (no bash). |
| D-006 | Chromium (`/opt/pw-browsers`) is the only browser engine installable in this container; WebKit and Firefox projects are configured in `playwright.config.ts` and run in CI, where `npx playwright install --with-deps` is allowed. | The container forbids `playwright install`; CI covers the other two engines. |
| D-007 | Higgsfield (image generation) is connected but has 0 credits; no paid generation was attempted. All photography is sourced from free-license libraries. | The brief allows generation only if available; spending the owner's credits without a balance is impossible and unasked. |

## Phases 1–3 — brand and foundation

| # | Decision | Rationale |
| --- | --- | --- |
| D-008 | Concept **Zill · ظل** ("Every hour has its shade") over Milh and Mouneh (see `docs/CREATIVE_BRIEF.md`). | It gives one idea that can drive layout, colour, motion and the product itself (time-aware menus, the sun's shade as UI). |
| D-009 | The atmosphere follows the real sun at each branch: `src/lib/time/sun.ts` computes sun position and phase for the branch's coordinates and IANA zone; six `[data-phase]` palettes re-scope the colour tokens. | A clock-based day/evening switch would be generic; the sun makes "every hour has its shade" literal and is deterministic on the server. |
| D-010 | Fonts: Reem Kufi (Arabic display), Imbue (Latin display), Markazi Text (text in both scripts), IBM Plex Mono (instrument labels); Plex Sans / Sans Arabic in the admin. All OFL, self-hosted with `next/font/local`, subset per script. | Distinct, licensed for commercial web use, and none of the faces the brief forbids as display type. |
| D-011 | Content Security Policy with a per-request nonce is set in `src/proxy.ts` (Next 16's middleware successor); Stripe and Turnstile origins are added only when their keys exist. | Strict CSP without `unsafe-inline` scripts, and no third-party origin is allowed unless that service is configured. |
| D-012 | Locale-prefixed routes only (`/ar`, `/en`); the admin has its own root layout and takes its locale from a staff preference cookie. | Keeps public URLs canonical with hreflang, and lets staff work in either language at the same URLs. |
| D-013 | Prices are integer halalas everywhere; `formatMoney()` formats at the edge only. | No floating-point money anywhere in pricing, payments or reports. |
| D-014 | Photography from Unsplash's public CDN only (search pages sit behind a bot check that was not bypassed); Wikimedia Commons was rate-limited and not used. | Clear free commercial licence and provenance for every file, without circumventing anyone's protection. |

## Phase 4–5 — public experience, reservations, ordering, commerce

| # | Decision | Rationale |
| --- | --- | --- |
| D-015 | Guest links (manage booking, order tracking, tickets, waitlist offers, gift receipts) carry an HMAC-SHA256 token derived from `BETTER_AUTH_SECRET` and the record id (`src/lib/server/tokens.ts`); only a hash is stored for reservations. | Unguessable links that work without an account, with nothing reusable stored in the database. |
| D-016 | The booking-session cookie is issued by the slots route handler, not by the hold server action. | Setting a cookie inside a server action re-renders the RSC tree and reset the page scroll mid-flow. |
| D-017 | Holds last `reservations.holdMinutes` (config); unpaid deposits, unpaid orders and unpaid tickets release after 30 minutes; waitlist offers are held for 60 minutes. | Long enough to pay or decide on a phone, short enough not to block real availability. |
| D-018 | Reservations, orders and tickets are written inside one write transaction that re-checks availability or capacity. | Prevents double-booking and overselling under concurrent requests on libSQL/Turso. |
| D-019 | Order tracking uses Server-Sent Events that poll the database every 3 s and close after 50 s with a `retry:` hint; `EventSource` reconnects. | Live updates with no paid real-time service and no long-lived connections that serverless hosts would kill. |
| D-020 | Dietary and allergen constants live in `src/lib/menu/tags.ts`, separate from the Drizzle schema. | Client components can import them without pulling the database driver into the browser bundle. |
| D-021 | Gift card designs are generated SVG artwork (`src/lib/brand/gift-card-art.ts`) in the palettes of four sun phases, rasterised to PNG with sharp for email. | Designed cards in the brand's own language, crisp at any size, and email clients that block SVG still get an image. |
| D-022 | Event tickets carry a QR code that opens the staff check-in view for that ticket code (`/admin/events/check-in?code=`). | The door scan needs a signed-in staff member, so the code itself grants nothing to a guest who photographs it. |
| D-023 | Private dining inquiries record the requested room (`inquiries.room_id`, migration `0001_inquiry_room`) and are checked against the room's capacity and the package's minimum guests on both client and server. | The events host gets an actionable request, and guests learn a constraint before sending rather than by email. |
| D-024 | The spacing token `--spacing-block` was renamed `--spacing-stack`. | Tailwind v4 derives `inline-<key>` (inline-size) utilities from spacing keys, so `inline-block` also set a fixed width. |
| D-025 | Structured data takes the currency from `restaurant.config.ts` instead of a literal. | White-label: another restaurant changes one config value. |

## Phase 5 — accounts, dine-in QR, AI concierge

| # | Decision | Rationale |
| --- | --- | --- |
| D-026 | An account always sees what was booked while signed in; orders, reservations and tickets made earlier as a guest with the same email appear only once that email is verified (email code). | Anyone can sign up with someone else's address; verification proves ownership before past bookings, addresses or receipts are shown. |
| D-027 | Account deletion cancels upcoming changeable reservations (normal cancellation and refund rules), refuses while an order is in progress, a reservation is past its cut-off, or a ticket is still valid, then removes personal data and keeps past orders/bookings with name and contact replaced. | The guest's right to erasure without breaking the restaurant's VAT records, kitchen tickets or a table that is already being prepared. |
| D-028 | The welcome bonus and welcome email run in Better Auth's `databaseHooks.user.create.after`; staff and seeded users are written directly to the database and never trigger it. | One path for password and email-code sign-ups, and staff accounts never earn guest points. |
| D-029 | The dietary profile marks dishes ("suits you", "contains tree nuts, which you avoid", "hotter than you like") rather than hiding them; "Only what suits me" is an opt-in filter. | Guests at a shared table still see the whole menu, and nobody misses a dish because of an over-cautious filter. Allergen data comes from the dish record, never from inference. |
| D-030 | The dine-in basket is separate from the delivery/pickup cart and lives in `sessionStorage`, keyed by table code. | A phone at a table must never mix a table order with a delivery basket, and closing the tab ends the table session. |
| D-031 | `/t/CODE` (the address printed on the table tent) redirects to the language of the guest's phone via `Accept-Language`; the rest of the site keeps Arabic as the default with no automatic detection. | A visitor scanning a QR code at the table should not have to find a language switch before ordering; shareable site URLs stay stable. |
| D-032 | Table orders need no email or phone (name defaults to "Table 3" / «الطاولة 3»), pay at the table or by card, and go to the kitchen with the table label; waiter and bill requests are deduplicated for 15 minutes and pushed to waiters. | Ordering at a table should be as fast as asking a waiter; repeated taps must not flood the floor team. |
| D-033 | The concierge uses the Claude API (`@anthropic-ai/sdk`, model `claude-opus-5-5` by default, overridable with `CONCIERGE_MODEL`), adaptive thinking at medium effort, a streamed manual tool loop with three narrow tools (`menu_now`, `check_availability`, `create_reservation`), and the server-side fallback beta so a declined turn is retried on a fallback model. | A small, auditable tool surface built on the same domain functions as the website; medium effort keeps answers quick for a chat. |
| D-034 | The concierge's stable instructions (voice, allergy rules, menu with allergens, houses and hours) are one cached system block; the time, locale and signed-in guest go in a second block after the cache breakpoint. | Prompt caching makes every follow-up message cheaper and faster, and per-request context never invalidates the cache. |
| D-035 | The concierge books only after the guest explicitly confirms the details, through the same hold-and-book transaction as the website, rate-limited to 3 bookings an hour per IP (30 messages per 10 minutes). Conversations are kept in the browser only. | A model can never create a reservation the guest did not ask for, and no chat transcripts are stored. |
| D-036 | The concierge is off unless both the `aiConcierge` flag is on and `ANTHROPIC_API_KEY` is set; the page, navigation link and API route disappear (404) otherwise. | The platform runs with zero external accounts, and nothing ships a UI that cannot work. |
