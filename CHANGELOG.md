# Changelog

Each entry is one deployed version. Most recent first.

## 2.24.052 — 2026-05-25
- **Risk manager Layer-1 guards** — all config-driven via `/settings/?tab=risk`, no redeploy needed:
  - `guard_min_conviction` (default 0.6) — reject proposals below this conviction.
  - `guard_dedup_event_ulid` (default true) — reject a proposal whose triggering event already has an open trade.
  - `guard_block_opposing_side` (default true) — reject SHORT on a symbol with an open LONG (and vice versa).
  - `guard_symbol_cooldown_min` (default 15) — minimum minutes between trades on the same symbol.
  - `guard_max_per_category_day` (default 3) — cap trades opened per event category per UTC day.
- Risk manager also queries open trades from the local Postgres ledger (works in paper mode where Bybit returns no positions), in addition to Bybit's positions feed.
- Tooltips for each new key in the central help glossary.

## 2.24.051 — 2026-05-25
- `trade_sampler` only logs to /workflow/ Done when it actually sampled at least one trade. Tick-only "Sampled 3 open trade(s)" rows every 5 s no longer drown the lane.
- `trade_sampler` excluded from Done aggregation (it's a watcher, like orchestrator/telegram_bot) — instead it shows up in the Active lane with its own heartbeat hash (`agent_status:trade_sampler`).
- New AGENT_VERB entry: "Sampling live prices for open trades".

## 2.24.050 — 2026-05-25
- **Graph loads near-instantly on reload.** localStorage cache + delta fetch:
  - On open we render the cached nodes/edges immediately, then fetch only events created since `lastSyncEpoch` and merge in.
  - First-ever load still hits the backend, but the backend itself is much faster now: edges are scanned **once globally** (was once per event), so cost is O(all-edges) instead of O(events × all-edges).
- New query parameter `GET /graph/data?since=<unix_seconds>` for delta fetches.

## 2.24.049 — 2026-05-25
- Fix **dark/light theme toggle**: Tailwind via CDN defaults to `darkMode: 'media'` (system preference), which silently ignores toggling the `dark` class on `<html>`. Now explicitly setting `darkMode: 'class'` before and after the CDN loads, so the topbar 🌙/☀ button actually flips the theme.

## 2.24.048 — 2026-05-25
- New **trade_sampler** loop in `execution_vault`. For every open trade it samples Bybit's latest price at an adaptive cadence picked from the longest milestone ETA: ≤15 min → every 5 s; ≤4 h → 1 min; ≤24 h → 5 min; ≤7 d → 30 min; longer → 6 h. Samples land in Redis (`trade:samples:<ulid>`, capped 720 entries).
- New endpoint `GET /trades/<ulid>/samples` returns the recorded samples.
- Trade detail page: the predicted-trajectory chart now overlays an **Actual price (live)** series (% move vs entry, time since open). Auto-refreshes every 15 s while the trade is open; static after close.

## 2.24.047 — 2026-05-25
- New **Seeded events** panel at the bottom of `/historian/`. Lists every historian-sourced event in the graph (newest first) — date, category, region, summary, entities (chips), and whether real Bybit price-impact data was attached. Click any row to jump to `/graph/?focus=<ulid>` and see it in the knowledge graph.
- New endpoint `GET /historian/events?limit=N`.
- Graph page honors `?focus=<ulid>` on initial load.

## 2.24.046 — 2026-05-25
- Global FastAPI exception handler on the dashboard. **Any uncaught exception in any route is now logged to `/errors/`** (Postgres `error_log`) — agent=`dashboard`, with path/method/user context — not just to stdout. No more "I had to dig in Railway deploy logs to find why my click failed".

## 2.24.045 — 2026-05-25
- **Historian seed actually runs now.** Root cause: `asyncio.create_task(...)` without keeping a reference allowed the task to be garbage-collected before it executed — the status hash wrote "running" but the work never started. Fixed by stashing tasks in a module-level set (`_RUNNING_TASKS`) and removing them only on completion.
- Historian start uses `loop.create_task` + immediately writes a `phase=starting` status; the seed coroutine now prints to stdout (`[historian] …`) at every step so a hang is diagnosable without structlog.
- Historian crashes (both top-level and per-batch) now flow through `error_reporter.capture_exception` → visible on the **Errors** page (`/errors/`), not just stdout.
- Start button posts via `fetch` so HTTP failures (auth expired, etc.) produce a visible toast instead of silent navigation.

## 2.24.044 — 2026-05-25
- **Trade detail charts** (the ones you asked for twice — finally landing):
  - **Predicted trajectory** line chart from the strategist's milestones (hours from entry × signed % move). Tooltip shows confidence and rationale per milestone.
  - **Analog max-move bars** (one bar per analog past event, green/red by sign).
  - **Analog overlay** — straight-line trajectories per analog from t=0 (placeholder until the per-bar OHLCV is exposed).
  - Charts use ApexCharts (already loaded site-wide).
- Historian: date presets next to the form — **1d / 1w / 1mo / 3mo / 1y / 5y / 20y** buttons set the End date = Start date + N days.
- Historian: more diagnostic logging on Start. The seed task is now wrapped — if it crashes during init, the failure is captured in Redis (`state=failed`) and shown in the UI instead of silently hanging until the 3-min stale detector kicks in.

## 2.24.043 — 2026-05-25
- **Web Push** (VAPID): service worker at `/static/sw.js`, subscription store in Redis (`push:sub:<email>`), helper `plata.dashboard.push.send_to_user`, and a 🔔 button in the topbar that asks notification permission + registers + sends a test push.
- **PWA manifest** at `/static/manifest.json` + SVG icon — Chrome desktop shows an install prompt, mobile gets "Add to Home Screen".
- **Server-Sent Events** at `/sse` — subscribes to the Redis `dashboard:events` channel and streams updates to any browser tab. Frontend wiring of specific events lands later; the pipe is ready.
- New deps: `pywebpush`, `py-vapid` (deploy will rebuild Docker layer).
- New script: `scripts/generate_vapid.py` — generates the VAPID key pair and prints a ready-to-paste Railway CLI command. Run it on your Mac (see usage below).

## 2.24.042 — 2026-05-25
- Global **Esc closes any open modal** — confirm, settings tabs, card-detail, changelog carousel, risk-config create/edit, graph focus, anything matching the standard `fixed inset-0 bg-black/50` overlay pattern. Equivalent to clicking ✕ / Cancel.

## 2.24.041 — 2026-05-25
- Strategist now outputs **milestones** along the expected trajectory (e.g. `+30% in 2 weeks`, `+56% in 3 weeks`). New `Milestone` model + `milestones: list[Milestone]` on `TradeProposal`. JSON schema asks for 2-5 milestones with `eta_minutes`, signed `expected_pct_move`, `confidence`, `rationale`. Bedrock-incompatible keywords still stripped by the LLM client.
- Trade detail page renders the milestones as a table — ETA, expected move (green/red), confidence bar, rationale.
- Per-proposal LLM cost snapshot recorded in Redis (`proposal_cost:<ulid>`) for the trade-detail page (next commit consumes it).
- Workflow Ready lane now surfaces **pending HITL proposals as actionable cards** with inline ✅ Approve / ❌ Reject buttons. Hits the same `/proposals/<ulid>/decide` endpoint Telegram uses, so both surfaces stay consistent.

## 2.24.040 — 2026-05-25
- Workflow: **Done lane groups same-agent entries within 5 s** into a single card showing `Title (N)` and exposing all the merged entries in the detail modal.
- Workflow: **click any card → details modal** (category, status, agent, lane, when, count, last touched, error, grouped entries, raw JSON).
- Workflow: small **×** dismiss button on the Historian card to clear status without leaving the page.
- Historian: each batch publishes its own Redis key (`historian:batch:<i>`) — visible as a per-batch card in the Kanban (Doing while running, Done when finished, Active+error if it failed). Bounded to 8 most-recent batches in the lanes.
- History page: signal rows with an image (metadata `image`/`image_url`/`thumbnail` or URL ending in .jpg/.png/.gif/.webp) show a **thumbnail** next to the title.

## 2.24.039 — 2026-05-25
- New **help tooltips** across the dashboard. A small grey ?-circle next to a value shows a plain-English explanation on hover/focus. Reusable Jinja macro `{{ help_icon('key') }}` reads from a central glossary so every page uses the same wording.
- Applied to: trade detail (Stop Loss, Take Profit, Conviction, Suggested SL/TP %, Notional, Sentiment magnitude, Net PnL, Analogs), dashboard tiles (Open positions, PnL today, Pending HITL, Signals today, LLM spend today), agents (In-flight, Spend today), risk-config rows (paper_trading_mode, risk_per_trade_pct, max_open_positions, max_daily_loss_pct, auto_approve_threshold_usd, llm.daily_budget_usd_total).

## 2.24.038 — 2026-05-25
- Graph readability pass:
  - Entity nodes carry a **type icon** (👤 person · 🏢 company · 🌍 country · 💰 asset · 🏛️ org · 💹 ticker) in the label.
  - Node size scales with degree, so hub entities (USA, IRN, …) visually pop.
  - Hub repulsion is degree-weighted (`120k + 30k × edges`) — the more connections, the more space they demand.
  - `idealEdgeLength` 220 → **320**, `nodeOverlap` 60 → **120**, `componentSpacing` 120 → **200**, iterations 3500 → **4500**.
  - Edge labels hidden by default to prevent the "mentions" text pileup. Hover an edge (or a node — which highlights its edges) to see the relation label.
  - Event labels sit on a rounded translucent background; entity labels now sit **inside** the rounded rectangle, not below it.

## 2.24.037 — 2026-05-25
- Telegram polling: `Conflict: terminated by other getUpdates` no longer floods the logs with stack traces. It's now a single concise WARN line on first occurrence, with the actionable hint to remove `TELEGRAM_BOT_TOKEN` from any service other than ingestion_hub.

## 2.24.036 — 2026-05-25
- Fix historian **Reset status** button: previously it lived inside an htmx fragment that swaps every 3s, so the form's confirm-then-submit handler was racing the swap and silently no-op'ing. Now it's a plain button bound once at the page level via event delegation; it calls `POST /historian/reset` directly through `fetch` and reloads the page.

## 2.24.035 — 2026-05-25
- Graph: "Back" (Esc / ← button) **restores the saved unfocused view** instead of re-fetching + re-running the layout. Node positions and zoom/pan are remembered exactly. Cache invalidates when the event-count selector changes.
- Graph: **loading indicator** shows "Loading graph…" on first load and "Loading focused view…" when drilling in.

## 2.24.034 — 2026-05-25
- Graph layout: much stronger node repulsion (80k), mandatory `nodeOverlap` gap (60px), longer edges (220), more iterations. Hub entities (USA / IRN / EUR) won't pile on top of each other any more.
- New **⟳ Re-layout** button on the graph toolbar — re-runs the force layout if the current arrangement isn't great.

## 2.24.033 — 2026-05-25
- Workflow lanes are **collapsible**. Click the header (or the ⌃ icon) to collapse a lane down to just title + count. State persists in localStorage and survives the 3-second htmx refresh.

## 2.24.032 — 2026-05-25
- Done lane no longer shows orchestrator heartbeats / telegram-bot commands / scraper polls. Those are continuous watcher activity — they belong in the watcher's own card (Active lane), not in Done.

## 2.24.031 — 2026-05-25
- Trade ledger rows clickable → new **decision-chain** page `/trades/<ulid>` showing the strategist proposal (conviction, reasoning, analogs), the triggering event, and any HITL/risk audit log entries.
- Dashboard tiles are now smart links: **Open positions** → single trade if exactly one, else `/trades/`; PnL today → `/trades/`; Pending HITL → `/proposals/`; Signals today → `/activity/`; LLM spend today → `/agents/`.
- Removed the static Mermaid architecture diagram from `/activity/`. Architecture lives in `docs/ARCHITECTURE.md` (versioned, kept in sync per commit).
- New `docs/SPEC.md` — canonical project spec, env-var catalog, contracts, known sharp edges, roadmap. Will be kept up-to-date with each meaningful change.

## 2.24.030 — 2026-05-25
- Historian seed now writes a `last_progress_at` heartbeat at every batch + every event. If no progress for >3 minutes, the dashboard auto-flags the run as **STALE** (instead of misleading "running"). A stale run is also surfaced as an error card on the Workflow Kanban.
- New **Reset status** button on the Historian page (and a `POST /historian/reset` endpoint) to clear a stale/failed/done run so a fresh seed can start. The /start endpoint now ignores a stale "running" flag.

## 2.24.029 — 2026-05-25
- Sidebar reorganised into collapsible groups: **Dashboard**, **Operations** (Workflow / Activity / Agent Health), **Knowledge** (History / Graph / Historian), **Trading** (Pending Proposals / Trades), **Diagnostics** (Errors / Dead Letters), **Settings**. Groups auto-expand when one of their children is the active page.
- Kill switch removed from the topbar. Moved to a new **Settings** page with **Flowbite tabs**: Controls (kill switch / resume), Risk Config (CRUD table), Account (signed-in user / logout), Environment (app version + state).
- `?tab=<name>` deep-links a specific Settings tab.

## 2.24.028 — 2026-05-25
- Graph page: floating toolbar with **+ / − / fit-all (⤢) / 1:1** zoom buttons (also keyboard `+ / − / 0`).
- Graph page: **Esc** (or the new ← Back button) clears focus and returns to the last view.
- Graph page: labels truncated to ~32 chars (full text in side panel), `min-zoomed-font-size` hides labels at low zoom so the graph is readable, text has a translucent background to keep it from blending into edges. Wider node spacing.
- Workflow Kanban: a running **Historian** seed now shows up as a card with "Seeded N/M (P%)" progress in Doing; completed runs appear in Done; failures appear in Active.

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
