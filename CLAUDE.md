# CLAUDE.md — Zill (ظل) restaurant platform

This repository is a complete, white-label restaurant website and operating platform, built for the
fictional brand **Zill / ظل** ("Every hour has its shade" / «لكلّ ساعةٍ ظلُّها»).

## Source of truth

- `docs/BRIEF.md` — the master brief, verbatim. Re-read it at the start of every phase and after any context reset.
- `docs/PROGRESS.md` — live status (done / next / known issues). Update at the end of every phase; resume from it.
- `docs/DECISIONS.md` — every non-obvious decision with a one-line rationale. Append, never rewrite history.
- `docs/CREATIVE_BRIEF.md`, `docs/BRAND.md` — concept, identity and design tokens. Every design choice must trace back to them.
- `docs/DELIVERY.md` — requirement → implementation → verification traceability.

## Commands

| Task | Command |
| --- | --- |
| Install | `npm install` |
| First-time setup (.env with generated secrets, migrate, seed) | `npm run setup` |
| Dev server | `npm run dev` |
| Production | `npm run build && npm start` |
| Lint / typecheck / unit tests | `npm run lint` · `npm run typecheck` · `npm test` |
| End-to-end tests (Playwright) | `npm run test:e2e` |
| DB: generate migration / migrate / seed / reset | `npm run db:generate` · `npm run db:migrate` · `npm run db:seed` · `npm run db:reset` |
| Media pipeline (grade, AVIF/WebP, blur, manifest) | `npm run media:build` |
| Menu PDFs (headless Chromium) | `npm run menu:pdf` |
| OG images (headless Chromium) | `npm run og:build` |
| Screenshots (375/768/1280/1920 × ar/en) | `npm run shots` |
| Lighthouse (mobile) | `npm run lighthouse` |

All scripts are cross-platform Node scripts in `scripts/` — never add bash-only scripts.

## Stack

Next.js (App Router, RSC, Server Actions) · TypeScript strict · Tailwind CSS v4 (tokens as CSS variables) ·
next-intl · Drizzle ORM + libSQL (SQLite file locally, Turso in production) · Better Auth (email+password,
email OTP) with RBAC · Zod + React Hook Form · GSAP (ScrollTrigger, SplitText, Flip) + Lenis · Motion ·
React Three Fiber (lazy WebGL only) · shadcn/ui **in the admin only**, re-themed · TanStack Table · dnd-kit ·
Tiptap · Recharts · React Email · Vitest · Playwright + axe.

## Layout

```
restaurant.config.ts      Single white-label config: identity, locales, currency, feature flags, tiers…
src/app/[locale]/…        Public site (ar default, en). Locale-prefixed routes only.
src/app/[locale]/t/[code] Dine-in QR table experience (/t/CODE redirects to the visitor's locale).
src/app/admin/…           Back-office (own root layout; locale from staff preference cookie).
src/app/api/…             Route handlers: auth, SSE streams, cron, uploads, payments webhooks, PDF, analytics.
src/components/site       Public components (never import shadcn/ui here).
src/components/admin      Admin components (shadcn/ui based, brand-themed).
src/components/brand      Logo, monogram, patterns, icons (hand-made SVG).
src/lib/domain            Pure, unit-tested business logic (availability, pricing, hours, throttling…).
src/lib/services          Provider interfaces + adapters (payments, email, storage, maps, captcha, ai).
src/lib/db                Drizzle schema, client, queries.
src/lib/i18n              Locale config, Intl formatters, Arabic normalisation, digit handling.
src/messages/{ar,en}.json UI copy. Written natively per language — never machine-translated.
src/emails                React Email templates (bilingual, RTL-aware).
scripts/                  setup, seed, media pipeline, PDF/OG rendering, screenshots, lighthouse.
tests/unit, tests/e2e     Vitest and Playwright suites.
legacy/                   Unrelated pre-existing project kept intact (see DECISIONS.md). Do not touch.
```

## Non-negotiable conventions

- **Zero placeholders**: no lorem ipsum, TODO, "coming soon", dead links/buttons, fake handlers, console errors,
  `any`, `@ts-ignore`, or skipped tests. If something is not built, it does not ship a UI.
- **Money** is integer minor units (halalas) everywhere; format only at the edge via `formatMoney()`.
- **Time**: persist instants as UTC ISO strings; every scheduling decision uses the **branch's IANA timezone**
  via `src/lib/time` — never `new Date().getHours()`, never the server or browser zone.
- **Numbers, prices, dates** go through `Intl` helpers in `src/lib/i18n/format.ts` (Arabic-Indic digits by default
  in Arabic, switchable in `restaurant.config.ts`). Numeric inputs normalise Arabic-Indic digits via `normalizeDigits()`.
- **RTL**: CSS logical properties only (`ms-*`, `pe-*`, `start-*`, `inset-inline-*`…). Directional icons and
  horizontal drag/scroll mirror in RTL; logos, media controls, phone numbers and clocks do not.
- **Arabic typography**: never letter-spacing, uppercase transforms, per-character split/animation or justified text
  on Arabic. Split/animate Arabic by words or lines only. Generous Arabic line-height.
- **Mixed direction**: wrap foreign-direction fragments in `<bdi>`; `dir="ltr"` on email/phone/URL inputs and values.
- **Security**: every server action and route validates input with Zod and checks authorization with
  `requireRole()` / `requireUser()`; rate-limit public mutations; never expose secrets to the client.
- **Services** are always accessed through `src/lib/services/*` interfaces; each has a local fallback that works with
  zero external accounts. Adding keys to `.env` switches to the real provider with no code change.
- **Motion**: animate only `transform`, `opacity`, `clip-path`. Everything interruptible. `prefers-reduced-motion`
  disables smooth scroll and all non-essential motion. Heavy effects are lazy and desktop-first.
- **Public site** never uses the default shadcn look; admin uses shadcn/ui re-themed with brand tokens.
- Commit after every meaningful step with a clear message; keep `docs/PROGRESS.md` current.

<!-- BEGIN:nextjs-agent-rules -->

# This is NOT the Next.js you know

This version has breaking changes — APIs, conventions, and file structure may all differ from your training data. Read the relevant guide in `node_modules/next/dist/docs/` (resolved from this file's directory; in monorepos the `next` package may not be visible from the repo root) before writing any code. Heed deprecation notices.

This block is written and re-added by `next dev` — verify at `node_modules/next/dist/server/lib/generate-agent-files.js`. Removing it from a diff only re-creates the uncommitted change; committing it with your work keeps the tree clean.

<!-- END:nextjs-agent-rules -->
