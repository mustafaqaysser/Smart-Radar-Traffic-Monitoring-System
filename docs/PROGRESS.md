# Progress

Live status of the build. Updated at the end of every phase. Resume from here after any interruption.

## Current phase

Phase 5 — Reservations, ordering, payments, commerce, customer accounts (reservations, ordering and commerce done;
accounts, dine-in QR and the AI concierge next).

## Done

- **Step 0** — brief saved verbatim (`docs/BRIEF.md`), `CLAUDE.md`, unrelated legacy project moved to `legacy/`.
- **Phase 1 — brand.** Three directions in `docs/CREATIVE_BRIEF.md`; Zill · ظل chosen ("Every hour has its shade").
  `docs/BRAND.md`: hand-drawn SVG wordmarks (Arabic, Latin), lockup, monogram, icons, sun-phase palettes (six phases,
  AA-checked pairs), type system (Reem Kufi, Imbue, Markazi Text, IBM Plex Mono; Plex Sans/Sans Arabic in the admin),
  fluid scale, 12-column grid, arch shapes, motion tokens, patterns, voice and tone.
- **Phase 2 — media.** 100 graded photographs (one grade via `scripts/media/build.mjs`, AVIF/WebP, blur, focal points,
  bilingual alt text) and ambient video loops (MP4 + WebM + posters); `docs/CREDITS.md`; manifest `src/content/media.json`.
- **Phase 3 — foundation.** Next.js 16 App Router, TypeScript strict, Tailwind v4 tokens, next-intl (ar default, en),
  Drizzle + libSQL schema and migrations, Better Auth with roles, services behind interfaces with local fallbacks
  (payments: simulated/Stripe; email: outbox/Resend; storage: local/S3; maps; captcha; rate limits), nonce CSP in
  `src/proxy.ts`, Intl formatters with Arabic-Indic digits, Arabic search normalisation, branch-timezone time helpers,
  live-sun atmosphere (`computeAtmosphere`) driving `[data-phase]` palettes, motion system (GSAP + Lenis, reduced motion).
- **Seed** — two branches, 60+ bilingual menu items with modifiers, tables, zones, events over 60 days, journal,
  team, press (fictional), reviews, FAQ, careers, loyalty, promotions, seasonal modes, private dining, and 90 days of
  synthetic orders and reservations relative to the seed date.
- **Phase 4 — public experience.** Home (cinematic day narrative), menu of the hour (sundial scrubber, filters, 14
  allergens, Arabic-aware search, matchmaker, trailing image), dish pages, printable menus + PDFs (headless Chromium),
  story, team, press, FAQ, journal, reviews (read/leave), membership, gallery, credits, link-in-bio, locations with live
  open/closed state and keyless MapLibre map, contact, careers with CV upload, newsletter double opt-in, legal pages,
  designed 404/500/offline, maintenance view.
- **Phase 5 so far.**
  - Reservations: holds, real availability (tables, turn times, pacing), the full flow with a mobile bottom sheet,
    deposits through the payment provider, confirmation with .ics / Google Calendar / WhatsApp, tokenized manage link
    (move or cancel within the cut-off), reminders and housekeeping jobs (`/api/cron/[job]`, `npm run cron`,
    `vercel.json`), waitlist with time-limited offers, large parties routed to private dining.
  - Ordering: branch, delivery/pickup, ASAP or scheduled with throttling, modifiers, notes, persistent cart (local and
    server-side), upsell, promo codes, gift card and loyalty redemption, tip, VAT/service breakdown, zones, phone
    validation, payments (simulated/Stripe/cash), receipt email, live tracking over SSE, reorder.
  - Commerce: gift cards (four designed cards rendered as SVG/PNG, custom amount, recipient, message, scheduled
    delivery, balance check), event tickets (calendar, ticket types, capacity in a write transaction, payment, QR ticket
    by email, .ics), private dining and catering (rooms, packages, inquiry with room/package/capacity checks).

## Next

1. Accounts: sign up / sign in (password or email OTP), password reset, profile, dietary & allergen profile that
   highlights safe dishes, addresses, favourites, orders + reorder, reservations, gift cards, loyalty, preferences,
   data export, deletion.
2. Dine-in QR `/t/[code]`: table menu, order to table, call waiter, request bill, pushed live to staff.
3. AI concierge behind its flag (off unless an API key is set): menu questions, allergies, availability and booking
   through tool calls.
4. Phase 6 admin: dashboard, orders board + KDS, reservations timeline/waitlist, table requests, menu, CRM, events and
   check-in, gift cards (void/adjust), promotions, loyalty, reviews, newsletter export, content, locations + QR tents,
   seasonal modes, flags, settings, reports, audit log, notifications, dev outbox, manual cron.
5. Live style guide at `/brand` (noindex).
6. Phase 7: sitemap, robots, llms.txt, OG images (pre-rendered with Chromium for Arabic shaping), PWA, Lighthouse,
   axe, Turnstile widget when keys are set, analytics.
7. Phase 8 QA (e2e in both locales, screenshots 375/768/1280/1920 × ar/en, three engines) and Phase 9 docs + `v1.0.0`.

## Known issues

- None open. (Fixed in this phase: a Tailwind v4 name clash made every `inline-block` element take the width of the
  `--spacing-block` token — the token is now `--spacing-stack`.)
