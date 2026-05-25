# Changelog

Each entry is one deployed version. Most recent first.

## 2.24.012 — 2026-05-25
- Forms now auto-save drafts to `localStorage` on every keystroke and restore them on reload. Cleared on submit. Password inputs are never stored. Enabled on risk-config create/edit and login (email only).
- Opt-in for any form: `<form data-persist="some-unique-key">`.

## 2.24.011 — 2026-05-25
- Workflow page rewritten as a **Kanban**: Background → Ready → Doing → Done lanes.
  - **Background**: always-on watchers (scraper source polls, orchestrator, telegram bot).
  - **Ready**: per-stream queue depth (XPENDING for each consumer group).
  - **Doing**: agents currently in-flight, with the last message they touched.
  - **Done**: most recent successful handler calls across all agents, newest first.
  - Each card has a colored **category** chip (ingestion / intelligence / execution / hitl / ops), status badge, agent name, and relative-time stamp.

## 2.24.010 — 2026-05-25
- New **Workflow** page (`/workflow/`) — operational live board showing what the system is *doing*, not which signals are flowing. Five columns:
  - **Polling** — one card per scraper source with status (RUNNING/IDLE/ERROR), last fetch time, items fetched, poll interval.
  - **Analyzing** — graph_ingestion / strategist / reviewer / risk_manager — shows in-flight count, current verb, last summary.
  - **Awaiting approval** — pending HITL proposals (clickable).
  - **Executing** — open positions with mode/qty/entry/age.
  - **Background** — orchestrator, executor, telegram_bot, scraper.
- Page refreshes every 3 seconds via htmx.
- Scraper now publishes per-source status to Redis (`scraper:source:<name>`).

## 2.24.009 — 2026-05-25
- Voyage embeddings rate-limit no longer DLQs the signal: `embed()` retries with backoff (1s, 3s, 8s) and raises a typed `EmbeddingRateLimited` on persistent 429. `graph_ingestion` catches it, logs a WARN, and counts the drop as `dropped_embed_rate_limit` instead of crashing.
- New `humanize()` in `error_reporter` turns noisy upstream errors (Voyage 429, Bedrock schema rejection, LLM budget breach) into short actionable messages in the dashboard.

## 2.24.008 — 2026-05-25
- Error log timestamps are rendered in the browser's local timezone (was UTC). Reusable: any `<time data-utc="...">` element is now auto-converted on load and after htmx swaps.

## 2.24.007 — 2026-05-25
- Errors table: per-row copy-to-clipboard button (timestamp, agent, severity, type, message, context, traceback).
- LLM client now strips JSON-schema keywords Bedrock-backed providers reject (`minimum`, `maximum`, `pattern`, etc.). Fixes `graph_ingestion` failing with `output_config.format.schema: For 'number' type, properties maximum, minimum are not supported`.

## 2.24.006 — 2026-05-25
- Every agent now tracks `processed_total`, `errors_total`, and reason-specific `dropped_*` counters in Redis (`agent_stats:<name>`).
- Every agent appends each handled message (or error) to a 50-entry activity tail (`agent_activity:<name>`).
- Activity page replaces the flat table with **per-agent cards** showing live counts (done / errors / in-flight / dropped reasons) and the last 8 events with timestamps.
- Strategist now reports *why* it drops signals (`dropped: below_threshold / missing_event / no_embedding`) — this is the most common reason proposals don't appear.

## 2.24.005 — 2026-05-25
- Moved Resume out of the topbar; Agents page now has **Resume all** + **Halt all** buttons and per-agent Resume/Halt buttons.
- Halt/Resume channels now accept an optional `{agent: "<name>"}` payload so a single agent can be paused without freezing the rest of the system.

## 2.24.004 — 2026-05-25
- New-version banner now pushes the topbar, sidebar, and content down instead of overlaying them.

## 2.24.003 — 2026-05-25
- Dashboard "Overview" now shows real data: system state, open positions, today's PnL, pending HITL, signals today, LLM spend, plus three live feeds (recent signals / trades / errors).
- Activity page now includes: per-agent table (heartbeat, in-flight, error count, halt status), DLQ depth per stream, LLM spend daily + monthly + cap, system RUNNING/HALTED + paper/live mode, last-hour signal count.

## 2.24.002 — 2026-05-25
- Confirmation dialogs use a Flowbite modal + toast (replaces native confirm/alert).
- Light/dark theme toggle in the topbar; choice persisted across reloads.
- Errors page: Clear-log button (POST `/errors/clear`).
- Telegram bot now ships with a persistent reply-keyboard menu on `/start` and `/help`.
- Click the version label in the topbar to open the changelog carousel (back/next).
- New `VERSION` file drives the displayed version. `CHANGELOG.md` powers the carousel.

## 2.24.001 — 2026-05-25
- Switched to numeric versioning scheme (2.YY.NNN), shown in the topbar and exposed via `/api/version`.
- Added activity page with system architecture diagram, pipeline depths, API status grid, and live recent-signals feed.
- Added email/password authentication with 4h / 72h "remember me" session cookies. Admin user bootstrapped from `DASHBOARD_ADMIN_*` env vars.
- Added new-version banner that detects deploys and offers a one-click reload.
- Risk Config rewritten as a CRUD table with create / edit / delete modals.
- Errors page got a "Clear log" button.
- Agent Health page now shows "x ago" relative time on heartbeats.
- Light / dark theme toggle in the topbar (persisted in localStorage).
- Telegram bot now has a `/start` reply, persistent button keyboard, and `/help` listing.
- Confirmations and alerts replaced with Flowbite modal + toast components.
- Click the version number in the topbar to browse this changelog.
- Pipeline robustness: Langfuse v3 tracing failure no longer halts agents; orchestrator halt cascade was the root cause and is mitigated.
