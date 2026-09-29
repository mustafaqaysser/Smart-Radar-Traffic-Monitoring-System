# MASTER BRIEF — Invent, design and build a world-class restaurant website & platform, end to end

## Your role
You are a complete senior studio in one: creative director, brand identity designer, native Arabic and native English copywriter, creative front-end developer, full-stack engineer and QA lead.

Mission: invent a restaurant brand from zero and ship its website and operating platform. It must be finished, polished, tested, documented and ready to hand over. I am providing nothing: no name, logo, colors, fonts, photos, copy or data. You decide everything, at the highest level you are capable of.

The bar:
- The public site must be a credible Awwwards "Site of the Day" contender, at the craft level of studios like Locomotive, Immersive Garden or Obys, while staying fast and effortless to use.
- A real restaurant owner, luxury or casual, must be able to run their whole online business from the admin on day one.

## Step 0 — before anything else
- Save this entire brief verbatim to `docs/BRIEF.md`. Re-read it at the start of every phase. After any context reset it is your source of truth.
- Create `CLAUDE.md` at the repo root with project conventions and pointers to `docs/BRIEF.md` and `docs/PROGRESS.md`.
- Initialize git and make the first commit.

## Operating rules
- Work autonomously to the end. Do not stop to ask me questions or for approval between phases. When a decision is needed, make the best one, log it in `docs/DECISIONS.md` with a one-line rationale, and keep going.
- Quality over speed: take the time this needs.
- Keep a live task list and update `docs/PROGRESS.md` at the end of every phase (done / next / known issues). If the session is interrupted, resume from it.
- Commit after every meaningful step with clear messages. Tag `v1.0.0` at delivery.
- Use subagents for work that can run in parallel (media sourcing, seed content, tests). You still own and personally review every design decision and every output.
- Zero placeholders. That means no lorem ipsum, no TODOs, no "coming soon", no dead links or buttons, no fake handlers, no console errors, no `any` or `@ts-ignore` used to silence errors, and no skipped tests.
- It must run fully with zero external accounts. Every third-party service sits behind an interface with a working local fallback. Adding real keys to `.env` switches to the real service with no code changes.
- Use the current Node.js LTS, npm, and cross-platform Node scripts only (no bash-only scripts).
- If a tool, download or install fails, find an alternative. Never silently drop a requirement.

## Execution phases
- Phase 1 — Creative direction & brand system
- Phase 2 — Media sourcing & processing
- Phase 3 — Foundation: scaffold, config, i18n/RTL, data model, auth & roles, service providers, motion system, component library
- Phase 4 — Public experience
- Phase 5 — Reservations, ordering, payments, commerce, customer accounts
- Phase 6 — Admin / back-office
- Phase 7 — SEO, PWA, performance, accessibility, security
- Phase 8 — QA loop until everything passes
- Phase 9 — Documentation & handover

Priority rule: if you ever have to trade depth for breadth, never compromise the public experience, the reservation flow or the ordering flow. Secondary features may be simpler, but must still be complete, working and polished.

## Creative direction — you own it
- Write `docs/CREATIVE_BRIEF.md` with three genuinely different concept directions (different cuisine, tone and visual world). For each one, cover:
  - name, cuisine and concept
  - city (your choice; the primary audience is Arabic-speaking)
  - positioning and story
  - one "concept line" that drives every decision
  - palette, type pairing, photography direction and motion language
  - 4–6 signature moments
  Pick the strongest, explain why, and commit to it fully.
- The name must be short, memorable, meaningful and easy to pronounce in both Arabic and English, and ownable. No clichés (no Golden / Royal / Palace / Bella-type names). Web-search it to make sure it doesn't clash with a well-known existing restaurant.
- The identity:
  - Arabic wordmark, English wordmark, bilingual lockup and monogram, as hand-crafted, optimized SVG
  - favicon, app and PWA icons
  - a color system with semantic roles, where every text pair passes WCAG AA
  - a type system with display and text faces for Arabic and Latin, intentionally paired, licensed for commercial web use, self-hosted via next/font, on a fluid scale with clamp()
  - spacing scale, grid, radii and elevation
  - motion tokens: custom easings, duration scale, stagger rules
  - textures or patterns, iconography, photography direction
  - voice and tone in both languages
- Document it in `docs/BRAND.md` and build a live style guide at `/brand` (noindex) that renders every token and component.
- Brand applications the product actually uses: printable menu, QR table tents, branded bilingual emails, OG/share images.

## Design bar — the most important section
- One idea, executed relentlessly. Every layout, animation and sentence expresses the concept line.
- Editorial, typography-led art direction: oversized display type, deliberate hierarchy and line lengths, strict spacing rhythm, a 12-column grid with intentional breaks, asymmetry, overlap, layering and generous negative space.
- Cinematic imagery: full-bleed moments alternating with small framed images; mask, clip-path and curtain reveals; parallax used with restraint; subtle grain or texture if it fits the concept.
- Motion with meaning: scroll-linked storytelling (pinned chapters, scrubbed sequences, line-by-line reveals) that never hijacks or fights the user's scroll. Animate only transform, opacity and clip-path, at 60fps, and make every animation interruptible.
- 4–6 signature moments no template could produce. These are sparks only; invent better ones:
  - a first-visit preloader that is part of the story (under 2s, skipped on return visits)
  - a hero built from WebGL, video or typographic choreography
  - a menu where hovering a dish reveals its photograph trailing the cursor
  - a time-aware atmosphere that shifts between day and evening service and shows what is being served right now
  - a drag-to-explore gallery, a contextual custom cursor, magnetic buttons, seamless page transitions
  - optional ambient sound, off by default
- Every state is designed: hover, focus-visible (styled, never removed), active, disabled, loading (skeletons), empty, error, success, 404, 500 and offline.
- Mobile is designed, not shrunk. Use dedicated mobile compositions, a persistent Reserve / Order action within thumb reach, bottom sheets instead of modals, and touch gestures. The first five seconds must be unforgettable on a phone.
- Performance is part of the design. Heavy effects are lazy-loaded and degrade gracefully, and prefers-reduced-motion disables them entirely, smooth scroll included. If an effect threatens a performance budget, make it desktop-only or defer it. Never ship a slow first load.
- Forbidden, because these are instant signs of a template or AI site:
  - "Welcome to [Name]" headlines
  - the dark-overlay hero with a centered title and two buttons
  - three-icon feature grids and shadowed card grids everywhere
  - purple/blue gradients, gradient blobs, default glassmorphism
  - emoji as icons
  - Inter or Roboto as display type, or the most overused Arabic web fonts (Cairo, Tajawal) as display faces
  - the default shadcn look on the public site, and rounded corners on everything
  - lazy "luxury" clichés (gold on black, generic arabesque ornament) unless genuinely reinvented
  - vanity counters ("10,000+ happy guests")
  - phrases like "culinary journey", "unforgettable experience", "taste of perfection" and their Arabic equivalents
  - anything copied from an existing site

## Arabic, RTL & bilingual craft — non-negotiable
- Arabic is the default locale and English is equal in quality. Use locale-prefixed routes (`/ar`, `/en`), a language switch that keeps the user on the same page, hreflang everywhere, and an architecture ready for more locales.
- Write copy natively in each language, never literally translated: elegant Modern Standard Arabic, and English in the same brand voice.
- Mirror fully through CSS logical properties and per-locale `dir`. Directional icons and horizontal scroll/drag interactions mirror in RTL. Logos, media controls, phone numbers and clocks do not.
- Arabic typography: never apply letter-spacing, uppercase transforms or per-character splitting/animation to Arabic, because it breaks letter joining. Split and animate Arabic by words or lines only. Use generous Arabic line-height, no justified Arabic text, and properly subset Arabic fonts.
- Isolate mixed-direction content (`<bdi>`, and `dir="ltr"` on email, phone and URL inputs and values).
- Numerals: Arabic-Indic digits by default in Arabic, switchable to Western digits in config. Every number, price and date goes through `Intl` with the right numbering system. Every numeric input accepts Arabic-Indic digits and normalizes them.
- Arabic-aware search: strip tashkeel, and unify alef variants, yaa/alef maqsura, and taa marbuta/haa.
- All scheduling logic uses the branch's IANA timezone, never the browser's or the server's.
- The admin is fully bilingual and RTL-correct too.

## Media — you source everything
- Source all photography and video yourself from libraries that allow free commercial use (Unsplash, Pexels or an equivalent with a clear license). Download and self-host every file; no hotlinking.
- Open and visually inspect every image before using it. Reject anything that is off-concept, low-quality, watermarked, contains text, logos or brands, or is inconsistent in light and color.
- For chef and team imagery, prefer editorial, non-identifying shots (hands, silhouettes, cropped or back views). Do not present a stock model as a named real person.
- Build a Node image pipeline with sharp:
  - one unified color grade so every photo feels shot by the same photographer
  - optimized masters served through next/image (AVIF/WebP)
  - blur placeholders
  - stored focal points for art-directed crops
  - unprocessed originals kept out of git
- Video: 1–3 short ambient loops compressed with ffmpeg (use ffmpeg-static if it isn't installed). Output H.264 MP4 + WebM, ≤ 4 MB each, muted, with a poster. Serve the poster instead under reduced motion, Save-Data or slow connections.
- Keep a media manifest (source URL, author, license, bilingual alt text, focal point) and `docs/CREDITS.md`.
- If an image-generation tool or MCP server is available in this session, you may use it for key imagery (hero, signature dishes, atmosphere) for perfect art-direction consistency. Otherwise rely on sourced photography.
- Craft illustrations and patterns as SVG yourself. For icons, use your own SVG set or one consistent family restyled to the brand.

## Stack & architecture
Default stack, latest stable versions. If something is broken at install time, choose the closest stable alternative and log it.
- Next.js (App Router, Server Components, Server Actions), TypeScript strict
- Tailwind CSS v4 with brand tokens as CSS variables; shadcn/ui in the admin only, fully re-themed
- GSAP (ScrollTrigger, SplitText, Flip, all free), Lenis synced with ScrollTrigger, Motion for UI micro-interactions, and React Three Fiber only for lazy-loaded WebGL moments
- next-intl
- Drizzle ORM + libSQL (a local SQLite file in dev, Turso in production via env), with migrations and a seed script
- Better Auth (email + password, email OTP) with role-based access control
- Zod + React Hook Form; in the admin: TanStack Table, dnd-kit, Tiptap, Recharts
- React Email; Vitest; Playwright + @axe-core/playwright
- Real-time updates (orders, table requests, tracking) via Server-Sent Events or short polling, with no paid real-time service

Services behind interfaces, each with a working local fallback:
- Payments: Stripe (test mode, Payment Element with Apple Pay / Google Pay), cash on delivery / pay at venue, and a built-in simulated provider when no keys are set. Adding a regional gateway must mean adding one adapter file.
- Email: Resend when a key exists. Otherwise every email lands in a dev outbox viewable in the admin, rendered exactly as sent, with OTP codes readable there.
- File storage: S3-compatible (e.g. Cloudflare R2) when configured, local filesystem otherwise.
- Maps: a keyless, custom-styled map (MapLibre GL + a keyless tile source such as OpenFreeMap) matching the brand. Lazy-load it, add deep links to Google Maps, Apple Maps and Waze, and provide a designed static fallback.
- Bot protection: honeypot + rate limiting always; Cloudflare Turnstile when keys exist.

White-label by design: one `restaurant.config.ts`, brand tokens and feature flags (delivery, pickup, dine-in QR, reservations, events, gift cards, loyalty, journal, careers, AI concierge…) must let me re-skin and re-configure the platform for another restaurant, luxury or casual, quickly. Explain how, step by step, in `docs/REBRAND.md`.

Multi-branch from the data model up (seed two branches): per-branch hours, holiday hours, timezone, tables, menu availability and delivery zones.

## Public experience
- Home: a cinematic narrative, not a stack of sections. The sequence is arrival → concept & story → signature dishes → the room & atmosphere → the chef & craft → experiences → voices (reviews, press) → reserve → find us.
- Menu:
  - every menu the concept needs (e.g. breakfast, lunch, dinner, tasting, drinks, desserts, kids)
  - instant dietary and spice filters, and allergen exclusion covering the 14 major allergens
  - Arabic-aware search and a "what should I order?" matchmaker
  - the menu being served right now highlighted, with live sold-out states and time-based availability
  - dish detail: story, ingredients, allergens, dietary tags, spice level, approximate calories, pairings, add to order
  - SEO-friendly dish pages
  - a print-optimized menu plus a downloadable PDF in both languages, rendered with headless Chromium so Arabic shapes correctly (document how regeneration works on each deployment target)
- Order online and Reserve: see the engines below.
- Experiences & events: calendar and ticketed booking (chef's table, tasting nights, classes…).
- Private dining & catering: rooms, packages, inquiry flow.
- Gift cards: purchase and balance check.
- Story: philosophy, sourcing and suppliers, sustainability. Chef & team.
- Gallery (lightbox, drag), Journal (articles, recipes), Press & awards, Reviews (read and leave one), Membership / loyalty.
- Locations: a page per branch with live "open now / closes at", hours including holidays, map, parking, accessibility, contact, WhatsApp, click-to-call and directions. Plus Contact and FAQ.
- Careers (listings, application with CV upload), newsletter signup, and a link-in-bio page at `/links`.
- Legal: privacy, terms, cookies, accessibility statement, allergen disclaimer. Designed 404, 500 and offline pages. Maintenance mode.
- Dine-in QR at `/t/[tableCode]`: an ultra-fast menu for that table. Guests can order to the table, call the waiter and request the bill, all pushed live to staff.
- Seasonal modes scheduled from the admin, e.g. Ramadan (iftar and suhoor menus, hours, a themed visual layer), Eid, New Year.
- Optional AI concierge behind a feature flag, off by default and enabled only when an API key is set. It answers menu questions, respects allergies, and checks availability and creates reservations through tool calls.
- The beverage program is alcohol-free. A platform-level flag may enable an alcohol category for other markets; it is off by default.

## Reservations engine
- Per branch: service periods, slot interval, turn times by party size, pacing (max covers per slot), tables (capacity range, area such as indoor / terrace / private, combinable), blackout dates and holiday hours.
- Real availability computed from tables, turn times and pacing. Use short holds during booking to prevent double-booking, and transactional writes.
- Flow: branch → date → party size → time → area and occasion → details → confirm. It must be beautiful, fast and fully keyboard-accessible, with a bottom sheet on mobile.
- Optional deposits for large parties or special experiences, through the payment provider.
- Confirmation page and email with an .ics file, a Google Calendar link and a WhatsApp share, plus a secure tokenized link to modify or cancel within the policy window.
- Reminder emails via a scheduled job: a cron endpoint, Vercel cron config, and a manual trigger in the admin.
- Waitlist when full; large parties are routed to private dining.
- Statuses: pending, confirmed, seated, completed, no-show, cancelled.
- Unit-test the availability algorithm, including holidays, midnight crossover and DST edge cases.

## Ordering, checkout & payments
- Branch selection; delivery, pickup or dine-in; ASAP or scheduled within opening hours and kitchen capacity (order throttling per time window).
- Modifier groups (required/optional, min/max, price deltas), item notes, per-branch and time-window availability, sold-out.
- Persistent cart (local for guests, server-side for signed-in users), a tasteful "complete your meal" upsell, promo codes, gift card and loyalty redemption, optional tip, and a tax and service-charge breakdown.
- Delivery zones as named areas with a fee and minimum order, plus an optional radius rule from a map pin.
- Guest or account checkout, address book, and phone validation (libphonenumber) defaulting to the branch's country.
- Confirmation, email receipt, a live tracking page (placed → accepted → preparing → ready / out for delivery → completed) with ETA, and one-tap reorder.
- Per-branch busy mode / pause online ordering.
- Unit-test pricing, modifiers, promos, gift cards, taxes, fees and throttling.

## Commerce, loyalty & customer accounts
- Gift cards: custom amount, designed digital card, recipient, message, scheduled delivery, unique code, balance check, partial redemption, admin void/adjust.
- Event tickets: capacity, ticket types, checkout, QR ticket by email, admin check-in view.
- Promotions: percentage or fixed, minimum order, date range, usage limits, first-order only, per branch.
- Loyalty: points per spend, tiers, rewards redeemable at checkout, history.
- Newsletter with double opt-in and CSV export.
- Accounts:
  - sign up / sign in (password or email OTP) and profile
  - a dietary and allergen profile that automatically highlights safe dishes across the menu
  - saved addresses, favorites, order history and reorder
  - reservations (manage upcoming), gift cards, loyalty
  - communication preferences, data export and account deletion

## Admin / back-office
- Roles enforced server-side: owner, manager, host, kitchen, waiter, content editor. Seed one account per role; print the credentials and list them in the README.
- Dashboard: today at a glance (covers, reservations, orders, revenue, average order value, no-shows), charts and live activity.
- Orders: a live board with sound alerts (with an enable-sound toggle for browser autoplay rules), accept/reject with reason, prep time, and a printable 80mm kitchen ticket. Kitchen display at `/admin/kds`: full screen, timers, bump.
- Reservations: per-branch timeline and calendar, list, table assignment, statuses, notes, waitlist.
- Live table requests (waiter calls, bill requests) from QR tables.
- Menu: menus, categories, items, modifiers, allergens, dietary tags, bilingual fields, image upload with focal point, drag-and-drop ordering, instant sold-out toggle, per-branch availability.
- Customers (CRM): profiles, history, preferences and allergies, tags (VIP), notes.
- Events and tickets, gift cards, promotions, loyalty, review moderation, newsletter.
- Content: every section of every page, gallery, team, press, FAQ, journal (rich text), careers and applications, inquiries with statuses.
- Locations: branches, hours, holidays, delivery zones, and tables with generated printable branded QR table tents.
- Seasonal modes, maintenance mode and feature flags.
- Settings: restaurant info, taxes and fees, payments, notifications, SEO defaults, social links.
- Reports with CSV export: sales, top items, covers, no-show rate, channels.
- Audit log, notification center, dev email outbox.
- The admin is fast, keyboard-friendly, bilingual, RTL-correct and themed with the brand, not default shadcn.

## Content & seed data
- A complete, coherent world:
  - 45–70 menu items with bilingual names and descriptions, prices, allergens, dietary tags, modifiers and pairings
  - two branches with realistic hours, tables and delivery zones
  - team, reviews, FAQ, careers, gift card designs, seasonal content
  - press and awards from fictional publications only (never real outlets, real awards or real people)
  - events over the next 60 days and at least six journal articles
- 90 days of realistic synthetic order and reservation history so dashboards and reports look alive. All seed dates are relative to the day the seed runs, so the demo never goes stale.
- Clearly fictional addresses and phone numbers; emails on the `.test` domain.
- Everything is editable from the admin. A `DEMO_MODE` flag seeds demo data and adds a discreet "concept project" note in the footer.

## SEO, PWA, performance, accessibility, security, privacy
- SEO:
  - per-page, per-locale metadata, canonicals, hreflang, sitemap, robots, llms.txt
  - JSON-LD for Restaurant/LocalBusiness (with openingHoursSpecification), Menu/MenuSection/MenuItem, Event, FAQPage, BreadcrumbList, AggregateRating and Article
  - dynamic OG images per page and dish in both languages. Verify Arabic shaping; if the renderer can't shape Arabic correctly, pre-render them with headless Chromium.
- PWA: installable, offline menu and key info, designed offline page.
- Performance budgets on a production build, mobile emulation:
  - Lighthouse Performance ≥ 90 on home and menu
  - Accessibility, Best Practices and SEO ≥ 95 (aim for 100)
  - LCP < 2.5s, CLS < 0.1, TBT < 200ms
  - Mostly Server Components; code-split animation and WebGL; subset fonts.
- Accessibility: WCAG 2.2 AA, complete keyboard support, visible designed focus, screen-reader labels, bilingual alt text, sufficient contrast, reduced motion honored everywhere, zero serious or critical axe violations.
- Security: Zod on every input, authorization on every server action and route, rate limits, secure cookies, CSP and security headers allowing only what's needed, upload validation, sanitized rich text, no secrets on the client.
- Privacy: a consent banner only when non-essential cookies are enabled; privacy-friendly first-party analytics feeding the admin dashboard; data export and deletion.

## QA loop — do not skip
- Build, lint, typecheck, unit tests and e2e tests all pass cleanly.
- Playwright e2e in both locales:
  - reserve, then modify and cancel
  - delivery and pickup orders with modifiers + promo + gift card
  - QR table order and waiter call
  - gift card purchase and event ticket
  - sign up and reorder
  - admin flows: edit a menu item → visible on the site; advance an order → visible on tracking; manage a reservation
- Visual QA: for every key page, capture screenshots at 375, 768, 1280 and 1920px in Arabic and English. Review every screenshot as a demanding creative director: alignment, spacing rhythm, hierarchy, RTL mistakes, overflow and clipping, orphans, crops, contrast. Fix and re-shoot until it is portfolio-worthy. Check Chromium, WebKit and Firefox.
- Run Lighthouse (mobile) and axe, and iterate until the budgets pass.

## Deliverables & Definition of Done
- Setup: `npm install`, then `npm run setup` (creates `.env` from the example with generated secrets, migrates, seeds), then `npm run dev`. Production: `npm run build && npm start`.
- Docs:
  - `README.md` in English, plus a separate Arabic section inside a `dir="rtl"` container
  - `docs/BRIEF.md`, `docs/CREATIVE_BRIEF.md`, `docs/BRAND.md`, `docs/DECISIONS.md`, `docs/PROGRESS.md`, `docs/CREDITS.md`, `docs/REBRAND.md`
  - `docs/DEPLOY.md` covering Vercel + Turso + R2 + Resend + Stripe, and a Docker/VPS alternative with Dockerfile and compose
  - `docs/ADMIN_GUIDE.md` written in Arabic
  - `CLAUDE.md`, a fully commented `.env.example`, and GitHub Actions CI (lint, typecheck, tests, build)
- `docs/screenshots/`: final screenshots of every key page in both languages, mobile and desktop. If feasible, add a 60–90 second Playwright-recorded walkthrough video of the main flows.
- `docs/DELIVERY.md`: a traceability table mapping every requirement in this brief to where it is implemented and how it was verified, plus any deviations with reasons.
- Done means:
  - every page exists, is linked and is designed to the same standard
  - everything is responsive and bilingual
  - every form and flow works end to end on seed data
  - all checks pass and there are zero placeholders

Begin with Step 0 now, and do not stop until the Definition of Done is met.
