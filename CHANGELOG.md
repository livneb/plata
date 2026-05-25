# Changelog

Each entry is one deployed version. Most recent first.

## 2.24.027 — 2026-05-25
- Historian seed now accepts a **research brief** (free text, any language) and **focus assets/topics**. The brief steers what the LLM looks for; if empty, the agent surfaces the largest market-moving events in the window.
- Year inputs replaced with **date inputs** (day-level granularity). Out-of-range events are dropped at validation.
- Status panel shows the active brief, focus list, and date window.

## 2.24.026 — 2026-05-25
- Historian seed UI: added **From year** / **To year** inputs. The LLM prompt now constrains the window, and any out-of-range events the LLM produces anyway are dropped at validation time. The status panel shows the active window.
- Prompt also asks the LLM to rank by market impact (largest first).

## 2.24.025 — 2026-05-25
- New **Historian seed** page (`/historian/`) — exposes the existing `plata.agents.historian.seed()` to the UI. Configure total events (10–2000) and batch size, click **Start seed**, and the agent:
  - Asks the LLM to enumerate dramatic events from 2005-2025 (wars, crises, central-bank surprises, hacks, regulation).
  - Embeds each via Voyage and inserts as an event node in the knowledge graph.
  - Pulls **real Bybit OHLCV** for the affected symbols around the event date and attaches price-impact metrics (max move, time-to-max, drawdown).
- Live progress: status badge, written/target counters, progress bar, last event date/category, last error. Page auto-refreshes every 3s; runs in the background.

## 2.24.024 — 2026-05-25
- Graph page fix: dropped the `cose-bilkent` layout plugin (needed `cytoscape.use()` registration + a separate `cose-base` dep) and switched to Cytoscape's built-in `cose` layout. No more `No such layout 'cose-bilkent' found` console error.

## 2.24.023 — 2026-05-25
- New **Graph** page (`/graph/`). Interactive Cytoscape.js view of the live knowledge graph stored in Redis:
  - Event nodes (circles), colored by category. Entity nodes (rounded rectangles), colored by entity type.
  - Edges show the mention relations (`mentions`, etc.) with directional arrows.
  - Drag nodes, scroll to zoom, click for the raw JSON doc in a side panel.
  - Click an event node to **focus** on it — pulls its entities + one-hop neighborhood. "Clear focus" returns to the most-recent-N view.
  - Selector to load 20 / 40 / 80 / 150 events.
- Pulls only event + entity + edge keys from Redis (`event:*`, `entity:*`, `edge:*`). Embeddings are stripped before sending to the browser.

## 2.24.022 — 2026-05-25
- New **History** page (`/history/`). Unified timeline merging `signal_archive`, `audit_log` (HITL decisions), `trade_ledger`, and `error_log`. Filter by kind (signal / decision / trade / error) and window (1h / 6h / 24h / 72h / 7d). Times render in your local timezone.

## 2.24.021 — 2026-05-25
- Source cards no longer hard-code "Polling X" in the title — they're just "GDELT", "Reddit", etc. The card moves between lanes based on current state:
  - `polling` (mid-fetch) → **Doing**
  - `sleeping` (between polls) → **Sleeping**
  - `error` → **Active** (so the failure is prominent)
  - `halted` → **Sleeping** with halted badge

## 2.24.020 — 2026-05-25
- Workflow Kanban: split the old "Background" lane into **💤 Sleeping** (periodic pollers between cycles) and **⚙️ Active** (event-driven observers — orchestrator + telegram bot). Five lanes total.
- Ready lane: cards now show the age of the **oldest pending message** ("oldest 5m 12s ago"), and the "0 waiting" status is labelled **caught up** instead of the confusing "empty".
- DLQ Replay fixed: re-publishes in the correct wire format (`{"data": <json>}`), throttles to 50ms between messages so consumers drain gradually, and runs in the background so the HTTP request returns instantly.
- (commit messages now prefixed with the version, matching the in-app topbar.)

## 2.24.019 — 2026-05-25
- Fix strategist `ValidationError` on `AnalogousEvent.similarity` when KNN returns a near-identical neighbor: `1 - score` could be `1.0000001` due to float32 precision. Now clamped to [0, 1].

## 2.24.018 — 2026-05-25
- Done-lane cards now describe what each agent *did* instead of dumping the raw payload ULID. Per-agent summaries:
  - `Enriched [gdelt] <title>` — graph_ingestion
  - `Analyzed [<category>] <summary>` — strategist
  - `Risk-checked <symbol> <side>` — risk_manager
  - `Executed <symbol> <side>` — executor
  - `Reviewed trade <symbol>` — reviewer
  - `Saw heartbeat from <agent>` — orchestrator
- Card subtitle dropped (was redundant with the verb in the title).

## 2.24.017 — 2026-05-25
- Collapsed `WATCHING` / `LISTENING` into a single `ACTIVE` status — both meant the same thing operationally.
- **Background cards now show the last concrete action** each watcher performed:
  - Orchestrator logs each DLQ scan, heartbeat check, halt trigger, and dead-agent detection.
  - Telegram bot logs every inbound command + each HITL prompt push, with user ID.
- New shared helper `plata.agents.base.log_action(agent, summary, kind)` for instrumenting any event-driven background loop.

## 2.24.016 — 2026-05-25
- New **Dead Letters** page (`/dlq/`). Per-stream view of parked messages with **Replay** (re-publish to source stream, agents reprocess) and **Discard** buttons. Useful after a deploy fixes a bug — recover the parked work.
- Workflow Background lane: clearer status labels — sources now show **POLLING** (mid-fetch, pulses) or **SLEEPING** (between polls); orchestrator shows **WATCHING**; telegram_bot shows **LISTENING**. No more vague "running".

## 2.24.015 — 2026-05-25
- Fix `graph_ingestion ValidationError` when LLM returns a signed sentiment value: `sentiment_magnitude` is now clamped to [0,1] (absolute value).
- Fix `strategist ResponseError: Unknown field at offset 2 near ulid`: removed the unindexed RediSearch ulid filter; self-event exclusion is now done client-side via `exclude_ulids={...}`.
- Errors copy button now reads from a hidden `<script type="text/plain">` blob per row (preserves newlines + JSON exactly; nothing truncated regardless of size or quotes).

## 2.24.014 — 2026-05-25
- Agents page now shows today's LLM spend per agent and the daily total in the header. Data comes from existing Redis counters (`cost:daily:<date>:agent:<name>`).

## 2.24.013 — 2026-05-25
- Kanban cards now show a live-ticking elapsed time (updates every second client-side).
- Background lane: sources no longer flicker between "polling" and "idle". Steady state is now "RUNNING"; "POLLING" pulses only during the brief active fetch.

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
