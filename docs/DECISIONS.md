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
