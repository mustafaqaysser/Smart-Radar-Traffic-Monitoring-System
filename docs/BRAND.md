# Zill · ظل — brand book

> **Every hour has its shade.** — «لكلّ ساعةٍ ظلُّها»

The live style guide is at **`/ar/brand`** and **`/en/brand`** (noindex). It renders every token and component from
the same code that runs the site, so it can never drift from this document.

---

## 1. Idea

In a hot land, shade is the first courtesy: before coffee or dates, a host moves you out of the sun. Zill is a
courtyard restaurant that follows the shade across its floor from first coffee to midnight. Every decision — menus,
colour, motion, copy, even the reservation button ("book your shade") — comes from one principle: **each hour of the
day has its own shade, and therefore its own table.**

The website knows where the sun is. It computes the sun's real position over the selected house (Jeddah or Riyadh)
and uses it to set the palette, the direction and length of every cast shadow, and the menu being served now.

## 2. Name and marks

**Zill / ظل** — shade, shadow. One syllable; two Arabic letters. *Khafif al-zill* («خفيف الظل») is how Arabic
describes charming, easy company.

| Asset | File | Use |
| --- | --- | --- |
| Arabic wordmark | `public/brand/zill-wordmark-ar.svg` | Primary in Arabic contexts |
| Latin wordmark | `public/brand/zill-wordmark-en.svg` | Primary in English contexts |
| Horizontal lockup | `public/brand/zill-lockup-horizontal.svg` | Headers, documents, emails |
| Stacked lockup | `public/brand/zill-lockup-stacked.svg` | Square formats, table tents |
| Monogram ظ | `public/brand/zill-monogram.svg` | Favicon, app icon, stamps |
| Monogram with shade | `public/brand/zill-monogram-shade.svg` | Social avatars, PWA splash |
| React components | `src/components/brand/logo.tsx` | Live, sun-aware versions on the site |

**Construction.** Both wordmarks are drawn on one grid: 12-unit verticals, 10-unit horizontals, a 48-unit x-height and
136-unit ascenders. The tall stems are **gnomons** — the upright of a sundial — and their tops are cut at **21.5°, the
latitude of Jeddah**, the angle a gnomon must make with the ground there. The dot of ظ and the dot of the Latin *i* are
the same size and sit at the same height: **the sun**, shared by both scripts. The Arabic letter ظ is itself a sundial —
a ground stroke, an upright and a sun — which is why it is the monogram.

**Rules.** Never stretch, outline, rotate or recolour the marks outside the palette; keep clear space of one x-height
(48 units) on every side; minimum size 20 px tall (wordmarks) and 16 px (monogram). On the live site the sun dot may be
tinted with `--c-sun` and the stems may cast the real shadow of the moment; printed matter uses the flat marks.

## 3. Colour

The palette is defined once in `src/lib/brand/palette.ts` and verified by `tests/unit/palette.test.ts`: every text pair
in every phase passes **WCAG AA (4.5:1)**, UI shapes and form borders pass **3:1**.

**Warm light, cool shade.** Shadows on sunlit plaster are lit by blue sky, so the brand's shade is a cool violet-brown,
never grey. Accents are borrowed from painted Najdi doors (henna, palm) and from the sun itself (saffron).

| Swatch | Hex | Role |
| --- | --- | --- |
| Plaster 100 | `#F4EDE2` | Morning background |
| Plaster 200 | `#EADFCD` | Coral stone surfaces |
| Clay 500 | `#A5784D` | Mud — decorative only |
| Shade 600 | `#574D5F` | Secondary text |
| Shade 900 | `#221D27` | Ink |
| Saffron 500 | `#D9982F` | The sun — decorative only |
| Henna 600 | `#A3402A` | Primary action |
| Palm 700 | `#3E5A33` | Success, vegetal accents |
| Night 950 | `#121119` | Night background |

**Phases** (semantic roles `bg · raised · surface · ink · muted · link · accent · on-accent · line · field · sun · shade ·
success · warning · danger`):

| Phase | When | Background | Ink | Accent |
| --- | --- | --- | --- | --- |
| dawn | civil dawn → sun 10° up | `#EDE7E4` cool pale plaster | `#221D27` | `#A3402A` |
| morning | → 90 min before solar noon | `#F4EDE2` plaster | `#221D27` | `#A3402A` |
| noon | solar noon ± 90 min (the short shade) | `#F8F4EC` bleached limewash | `#221D27` | `#A3402A` |
| afternoon | → sunset (the long shade) | `#F0E2CC` warm stone | `#221D27` | `#9C3B25` |
| dusk | sunset → civil dusk (maghrib) | `#231B23` violet ember | `#F3E8DC` | `#EE8A62` |
| night | sun below −6° | `#121119` indigo night | `#EFE6D8` lamp-lit plaster | `#E57F5C` |

Phase changes cross-fade over `--dur-horizon` (2.2 s). Admin screens use the morning/night pair only.

## 4. Typography

All faces are SIL Open Font License, self-hosted through `next/font/local` (`src/lib/fonts.ts`), split per script with
`unicode-range` so a page only downloads the scripts it renders.

| Role | Arabic | Latin | Why |
| --- | --- | --- | --- |
| Display | **Reem Kufi** 500–700 | **Imbue** 300–600, optical size auto | Kufi verticals stand like gnomons; Imbue's contrast is literally light and shade — thick stroke, hairline |
| Text | **Markazi Text** 400–600 | **Markazi Text** 400–600 | One bilingual family designed together — calligraphic Naskh and a warm serif |
| Labels | Reem Kufi 500 | IBM Plex Mono 500, tracked capitals | The "instrument" voice |
| Instrument | Reem Kufi + Arabic-Indic digits | IBM Plex Mono | Time, coordinates, the sun's angle |
| Admin UI | IBM Plex Sans Arabic | IBM Plex Sans | Dense, legible back-office screens |

**Fluid scale** (`clamp()`, 375 → 1920 px): display-xl 60→208 px · display-lg 48→136 · display-md 36→88 ·
heading-lg 28→52 · heading-md 22→34 · heading-sm 18→24 · body-lg 19→23 · body 18 (Arabic 20) · small 15 (Arabic 17) ·
label 12 (Arabic 15).

**Arabic rules (enforced in CSS):** no letter-spacing, no uppercase, no justified text, no per-character animation —
split and animate by words or lines only. Arabic display sizes are optically reduced (×0.74–0.8) with line-height
1.3–1.4; body line-height 1.9. Mixed-direction fragments are isolated with `<bdi>`; email, phone and URL values are
`dir="ltr"`.

## 5. Space, grid, shape and depth

- **Spacing** — 4 px base (`--spacing`), plus fluid rhythm: gutter 16→32 px, page margin 16→96 px, block 40→80 px,
  section 80→192 px.
- **Grid** — 12 columns on desktop, 8 on tablet, 4 on mobile (`.site-grid`), max width 1920 px. Compositions break
  the grid deliberately: full-bleed images, a column left empty on purpose, type that overhangs into the margin.
- **Shape** — square by default. The signature shape is the **arch** (`.arch-4x5`, `.arch-3x4`), the silhouette of
  courtyard openings, used for photographs that open like windows. Pills only for toggles and chips; 2 px/6 px radii
  exist only in forms and the admin.
- **Depth** — no floating card shadows. Depth is **cast shade**: a hard, un-blurred offset shadow whose direction and
  length follow the live sun (`.cast-shade`, `--shade-x/-y/-len`). Used sparingly: the dish window, the gift card, the
  preloader, the reservation ticket.

## 6. Motion

Shade moves slowly and never jumps.

| Token | Value | Use |
| --- | --- | --- |
| `--ease-shade` | `cubic-bezier(0.16, 1, 0.3, 1)` | Default for reveals — a long deceleration, like a shadow settling |
| `--ease-sun` | `cubic-bezier(0.65, 0, 0.35, 1)` | Sweeps and loops |
| `--ease-dusk` | `cubic-bezier(0.7, 0, 0.84, 0)` | Exits |
| `--ease-breeze` | `cubic-bezier(0.34, 1.2, 0.64, 1)` | Micro-interactions only |
| Durations | 90 · 180 · 320 · 560 · 900 · 1400 · 2200 ms | instant · quick · base · calm · slow · sun · horizon |

**Stagger:** words 45 ms, lines 90 ms, items 70 ms; a sequence never staggers longer than 700 ms in total.
**Rules:** animate only `transform`, `opacity` and `clip-path`; every animation is interruptible; wipes travel along the
current shade vector; scroll-linked chapters scrub with the scroll and never pin longer than 2.5 viewports; Lenis
smooth scroll is desktop-only and off under reduced motion. Under `prefers-reduced-motion` all motion is removed and
content is shown in its final state.

## 7. Texture, pattern, iconography

- **Grain** — a still plaster grain (`public/brand/grain.png`) at 5–7 %, never animated.
- **Vents** — the triangular openings of Najdi mud walls, redrawn as a sparse triangle lattice for bands and gift cards
  (`src/components/brand/patterns.tsx`). Used as architecture, not ornament: one band per page at most.
- **Hour marks** — dial ticks with Arabic-Indic numerals for time scrubbers and the preloader.
- **Icons** — an original set drawn on a 24 px grid with 1.5 px strokes, square caps and mitred joins, matching the
  wordmark's geometry (`src/components/brand/icons.tsx`), including the 14 major allergens. Directional icons mirror in
  RTL. The admin uses Lucide restyled to the same 1.5 px stroke for breadth.
- **No emoji** anywhere as icons.

## 8. Photography

Hard, low sun; long, crisp shadows; warm highlights and cool shade; plaster, coral stone, clay, palm wood, linen,
stoneware and brass. Food at 45° or top-down with generous negative space; hands allowed, faces never. Night frames
lit by warm lamps against deep blue ambient. Every image passes through one grade (`scripts/media/build.mjs`): shadows
lifted toward violet-brown, highlights warmed, greens and blues muted, a fine grain — so sourced photographs read as
one photographer's work. Crops are art-directed from stored focal points.

## 9. Voice and tone

We speak like a host who notices things: the angle of the light, the temperature of the room, the moment the bread
comes out. Calm, precise, warm, a little wry. Short sentences. Present tense. Sensory, never breathless.

**Arabic** — Modern Standard Arabic with a classical cadence and a light hand: short clauses, concrete nouns, verbs
of time and light (يميل، يطول، يقصر، يبرد). Written natively, never translated. Dialect words only for dish names and
the things people really call them (قهوة، كرك، تميس).

**English** — spare and observational, the same host speaking another language. British spelling.

| Do | Don't |
| --- | --- |
| «احجز ظلّك» · "Book your shade" | "Reserve now for an unforgettable experience" |
| «الخبز يخرج من التنّور عند السادسة إلا ربعاً» · "Bread leaves the oven at quarter to six." | "Taste the perfection of our artisanal bread" |
| «الظلّ الطويل: قهوة وحلو حتى المغرب» · "The long shade: coffee and sweets until sunset." | "Welcome to Zill!" |
| «لا نقدّم ما لم يكتمل موسمه» · "If it isn't in season, it isn't on the menu." | "A culinary journey through Arabia" |

**Banned** (both languages): welcome-to headlines, "culinary journey" (رحلة طهي), "unforgettable experience"
(تجربة لا تُنسى), "taste of perfection" (مذاق الكمال), "exquisite", "indulge", vanity counters, exclamation marks in
headlines.

## 10. Brand applications in the product

Printable menu and PDF (`/[locale]/menu/print`, rendered with headless Chromium), QR table tents
(`/admin/locations/tables/tents`), bilingual emails (`src/emails`), OG/share images (`/og/*`, pre-rendered with
headless Chromium so Arabic shapes correctly), gift card designs (dawn · noon · long shade · night), PWA icons.
