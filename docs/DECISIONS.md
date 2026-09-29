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
