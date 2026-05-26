# Changelog

Each entry is one deployed version. Most recent first.

## 2.24.078 — 2026-05-26
- **Stocks are now first-class** — the strategist, universe and risk-manager all know stocks exist, not just crypto.
  - **Strategist prompt** (`agents/strategist.py`): replaced the "BTC/ETH/SOL pairs only" rule with a legal-universe map per asset class: crypto names for on-chain events, **SPY/QQQ/IWM/GLD/TLT** for macro shocks, **AAPL/MSFT/NVDA/GOOGL/META/AMZN/TSLA/AMD/AVGO** for single-name US tech earnings/regulatory news, **COIN/MSTR** for crypto-adjacent equities, **XAUUSDT/GLD** for gold, **EURUSDT/GBPUSDT** for dollar stories. Default to SPY for ambiguous macro.
  - **Universe** (`execution/universe.py`): added 16 new symbols — 11 US single names + 5 ETFs (SPY, QQQ, IWM, GLD, TLT), each with `venue="alpaca_paper"`, `instrument_type="stock" | "etf"`, sector tags (`us_megacap`, `us_index`, `us_semis`, `us_crypto_adj`, `us_commodity`, `us_bonds`) so the existing sector caps apply automatically.
  - **Risk Manager** (`agents/risk_manager.py`): `_fetch_price`, `_fetch_equity`, `_fetch_positions` now route through `venue_for(symbol)` — crypto→Bybit, stock→Alpaca. Also opens an AlpacaClient on startup. Sector-cap and max-open-positions counts use the right venue's positions.
- **Both paper modes confirmed running:** Bybit testnet + Alpaca paper. Trades for crypto symbols go to Bybit testnet; trades for stock tickers (AAPL, SPY, etc.) go to Alpaca paper.
- **What to test:** Wait for the next macro-flavoured event (Fed, CPI, geopolitics) → strategist should now propose `SPY` or `GLD` instead of forcing `BTCUSDT`. Then watch the trade detail page — venue badge should show 📈 stock and the sampler should record Alpaca prices.

## 2.24.077 — 2026-05-26
- **Settings → Environment tab now reflects DB-stored credentials.** The Bybit / Alpaca status badges were only checking `settings.bybit_api_key` / `alpaca_api_key` (env-vars), so keys saved via the 🔑 API Keys tab still showed `NOT SET`. They now show `CONFIGURED` if **either** the env-var **or** the DB row is present (matches the actual runtime lookup order in `credentials.get()`).
- Replaced the misleading footer line ("configured per Railway service") with a link to the API Keys tab.
- **What to test:** save Bybit + Alpaca keys via the API Keys tab → switch to the Environment tab → both cards flip from `NOT SET` to `CONFIGURED · MAINNET` / `CONFIGURED · PAPER`.

## 2.24.076 — 2026-05-26
- **🐛 Fix saving API keys from Settings → API Keys.** The upsert was crashing with `column "metadata_" of relation "api_credentials" does not exist`. The `ApiCredential` ORM model renames the Python attribute to `metadata_` (because `metadata` collides with SQLAlchemy's `Base.metadata`) but the actual Postgres column is still `metadata`. The `INSERT ... ON CONFLICT DO UPDATE SET` block was using the Python attribute name in its `set_` dict — `set_` takes raw column names. Now uses `"metadata"`.
- **What to test:** Settings → API Keys → paste a new value into any row → Save → row updates with new last-4 suffix, no 500.

## 2.24.075 — 2026-05-26
- **🐛 Real fix for the empty actual-price line.** The sampler was calling `fetch_ohlcv_bybit(symbol, start_ts=now, end_ts=now)` — a zero-width window — so Bybit returned **zero bars every tick** and `_latest_price` returned `None` forever. **No trade ever got a sample.** Now queries the last 5 minutes and takes the latest bar's close. The same diagnostic endpoint added in `v2.24.074` would have told us this immediately (`probe_price: null` for a crypto symbol with no auth needed).
- **What to test:** wait ~1 minute after deploy → open any open crypto trade's detail page → banner above the chart should flip to `📈 N live price sample(s) recorded for this trade.` and the orange **Actual** line should draw alongside the predicted dashed line.

## 2.24.074 — 2026-05-26
- **`/trades/<ulid>/samples` is now self-diagnostic.** When the actual-price line is empty, hit this endpoint and the `diag` block tells you exactly why: sampler heartbeat age (sampler dead?), computed cadence for this trade, trade entry/exit price, venue routing (`bybit` or `alpaca`), and a one-shot live price probe — if the probe returns `null` you also get a hint pointing at missing Bybit/Alpaca credentials.
- **What to test:** `GET /trades/<ulid>/samples` returns `{count, samples, diag:{venue, sampler_heartbeat:{age_sec, alive}, cadence_sec, probe_price, probe_hint?}}`. If `sampler_heartbeat.alive` is `false` your execution_vault deploy isn't running the sampler. If `probe_price` is null the venue credentials are missing.

## 2.24.073 — 2026-05-26
- **Trade detail chart: diagnostic banner above the milestone chart.** When no live samples have been recorded yet, the chart now shows `⏳ No live price samples yet.` with an explanation (the sampler in `execution_vault` records one every 5s–6h depending on the longest milestone ETA; needs Bybit credentials for crypto, Alpaca for stocks) and a `View raw samples →` link to `/trades/<ulid>/samples`. Once samples arrive, the banner flips to `📈 N live price sample(s) recorded for this trade.` and stays in sync with the 15-second auto-refresh.
- **Why your actual-price line was empty:** if the trade pre-dates the sampler, or `execution_vault` is missing the venue credential for that symbol, no samples land in `trade:samples:<ulid>` and the actual-price series stays empty. The banner makes that obvious instead of looking like a chart bug.
- **What to test:** open `/trades/<ulid>` for an open crypto trade → banner should show a sample count >0 after a minute. Open a trade for a symbol whose venue is unconfigured → banner should explain why nothing is being plotted.

## 2.24.072 — 2026-05-25
- Translate button: when the response is `{skipped: true}` (default English / Technical preference — no rewrite needed) the JS now shows an info toast pointing the user at **Settings → Account → Preferences** instead of looking like a silent no-op.

## 2.24.071 — 2026-05-25
- **UI-managed API credentials, encrypted in Postgres.** New `🔑 API Keys` tab on `/settings/?tab=api` lets you paste / rotate / delete keys for every external provider (OpenRouter, Voyage, Bybit key+secret, Alpaca key+secret, Telegram, Langfuse) without touching Railway env-vars.
  - Encrypted with **Fernet (AES-128-CBC + HMAC-SHA256)**; encryption key is derived from `DASHBOARD_SESSION_SECRET` (no new env var needed — but rotating the session secret invalidates all stored credentials).
  - Storage: new `api_credentials` table (auto-created on dashboard startup; no Alembic step). Only the last 4 chars of the secret are ever shown back to the browser.
  - Lookup order at runtime: in-process 60-second cache → Postgres → env-var fallback. So existing deploys keep working; new keys saved in the UI override the env-var.
  - Clients updated: `LLMClient` (OpenRouter), `embeddings._client` (Voyage), `BybitClient`, `AlpacaClient` all consult credentials first.
- New dependency: `cryptography>=42.0` (already transitively present via other libs).

## 2.24.070 — 2026-05-25
- **Country alias dedup is now aggressive.** Previously the canonicalizer only fired when the LLM already classified a node as `country`, so misclassifications (IL as `asset`, USA as `org`, ILS as `ticker`) slipped through and created duplicates. Now `canonicalize_entity()` returns `(new_type, new_id, new_name)` — if the id OR name matches a known country alias we **force `type=country`** regardless of the LLM's guess. The graph_ingestion agent uses the corrected type at write time.
- **Stricter LLM extractor prompt**: explicit rules listing the right typing for each entity class, with examples for the most common misclassifications. Currency codes (ILS/USD) must NOT become country nodes; ISO country codes (US/IL/USA/GB/EU) must NOT become asset/ticker.
- **Background dedup job** with progress: `POST /graph/dedup/start` kicks an async pass that scans every `entity:*` key, merges every alias-duplicate into its canonical sibling, rewrites every incident edge, and reports progress in `graph:dedup:status` (state / merged / planned / edges_rewritten / failed / current). `GET /graph/dedup/status` exposes it; the 🧹 Dedup button on the graph page kicks the job and shows a sticky toast that updates every 2 s until the run completes, then reloads the graph.

## 2.24.069 — 2026-05-25
- **Esc actually closes modals now.** Previous visibility check used `offsetParent`, which is always `null` for `position: fixed` elements — so the handler skipped every modal it tried to close. Switched to `getComputedStyle(o).display !== 'none'`. Works on card-detail, confirm, settings tabs, changelog carousel, graph focus, every modal.
- **Sticky red banner** at the top when any agent is halted (or the system is). Polls `/api/agents/halted` every 10s + reacts instantly to SSE `system_state` events. Click → `/agents/`.
- **Charts now render** on the trade-detail page and the dashboard tiles. Root cause: the ApexCharts CDN was loaded with `defer`, so every inline chart-init script (which runs synchronously after HTML parse) ran *before* `window.ApexCharts` was defined and silently skipped. Removed the `defer`.
- **🌐 translate button now works.** Two fixes: route accepts both `/api/translate` and `/api/translate/`, the fetch now sends `credentials: 'same-origin'` (cookie) and surfaces the server error in a toast on failure.
- **OpenRouter 402 "can only afford N tokens"** is parsed and the next attempt automatically shrinks `max_tokens` to that value (minus a small buffer) and retries — so a near-empty credit balance still produces output instead of a hard failure. If the 402 persists, the provider is flagged.
- **Activity page** now shows a **LIMIT REACHED** badge + amber border next to any external API whose error rate-tripped (OpenRouter / Voyage / Bybit / Alpaca / Telegram / Langfuse). Inline panel with the message + a "→ Add credits / increase limit" link to that provider's settings page. Auto-clears after 6h. Driven by `flag_api_limit()` in `core/error_reporter.py`.
- **Agent page spend** now shows **Today / Yesterday / Last 7d / Last 30d / All time** — both as a header strip (totals) and as 5 per-agent columns on each card. Built from existing `cost:daily:<date>` and `cost:daily:<date>:agent:<name>` Redis counters; the all-time aggregate scans every dated key under that pattern.

## 2.24.068 — 2026-05-25
- **Layer-2 self-improving risk_manager.** The Reviewer agent now maintains rolling win-rate stats in Redis keyed by `(symbol, category, conviction-bucket)` — buckets `<0.6 / 0.6-0.7 / 0.7-0.8 / 0.8-0.9 / 0.9-1.0`. Every 25 closed trades it finds the worst-performing slice and asks the LLM whether ONE small, conservative tweak to a `guard_*` config key is warranted.
- Proposed tweaks land in the Postgres `audit_log` as `action=proposed_config_tweak` with the full evidence + rationale, status=`pending`.
- New **🎚️ Tuning** tab on `/settings/?tab=tuning` lists every pending proposal with Apply / Reject buttons. Apply writes the new value with a version bump (same flow as user-driven updates), mirrors to Redis, and publishes a `CONFIG_UPDATED` channel message so all agents reload. Reject just marks the row resolved.

## 2.24.067 — 2026-05-25
- Trade-detail **predicted-vs-actual** chart upgraded to Flowbite-style **area chart** with gradient fill, smooth actual line, dashed predicted line, Inter font.
- Dashboard tile **sparklines**: PnL-today tile shows last 30 d of daily PnL; Signals-today tile shows last 24 h hourly counts. Fed by new `GET /api/dashboard/sparklines` endpoint (one query each, server-side bucketing).
- Agents page **donut chart** showing today's LLM spend share per agent (Apex donut with center label = total $).

## 2.24.066 — 2026-05-25
- **Hebrew + kid-friendly help tooltips.** Every `?` icon now ships three variants in its data attributes (`data-help-en`, `data-help-he`, `data-help-kids`); JS picks one based on two cookies (`plata_lang`, `plata_aud`). Switch them on **Settings → Account → Preferences** — every tooltip updates instantly, no reload.
- **🌐 Translate / explain-further button** on long-form text (strategist reasoning, triggering-event summary, every analog summary). One click → POST `/api/translate/` (lang + audience from cookies) → LLM rewrite cached per `(text,lang,audience)` for 30 days in Redis (`translate:<sha256>`). Click again to toggle back to the original.
- Any element with `data-translate` automatically gets the button (so adding it to new prose blocks is a one-attribute change).

## 2.24.065 — 2026-05-25
- **Alpaca (US equities + ETFs) execution adapter**, alongside the existing Bybit (crypto perps).
  - `plata/execution/alpaca_client.py` — async httpx client (no extra dep). `fetch_balance` / `fetch_positions` / `fetch_ticker` / `fetch_ohlcv` / `create_market_order` mirror the Bybit interface so consumers stay venue-agnostic. Paper account by default (`ALPACA_PAPER=true`); flip to live with `ALPACA_PAPER=false`.
  - `plata/execution/router.py:venue_for(symbol, …)` decides per-proposal: `XXXUSDT/XXXUSD/XXXBTC` → Bybit, 1-5 uppercase letters (NVDA, SPY, AAPL) → Alpaca, with explicit `proposal.venue` / `proposal.instrument_type` hints overriding.
  - `executor` initializes both clients on startup; uses `_client_for(symbol, hint_venue, hint_class)` to dispatch each order.
  - `trade_sampler._latest_price` routes through the same logic — stocks pull the Alpaca latest-trade endpoint, crypto uses Bybit 1m bars.
- New settings: `ALPACA_API_KEY`, `ALPACA_API_SECRET`, `ALPACA_PAPER` (default true).
- Settings → Environment tab shows Bybit + Alpaca status cards. Activity page API status grid adds an Alpaca row.

To enable Alpaca paper trading, on your Mac:

```bash
railway service execution_vault
railway variables --set "ALPACA_API_KEY=PK..." --set "ALPACA_API_SECRET=..." --set "ALPACA_PAPER=true"
```

(Free paper key from https://alpaca.markets — Dashboard → API Keys → "Generate Paper Key".)

## 2.24.064 — 2026-05-25
- **Per-event sub-cards on the Workflow Kanban.** Each event the historian writes is pushed to a capped Redis list (`historian:events_live`, last 30, 90 s TTL); the workflow renderer surfaces up to 8 of them as cards. Within 30 s they appear in Doing as `running`, then age into Done as `ok`. Live view of the seeder filling the graph in real time.

## 2.24.063 — 2026-05-25
- **Cancel ✕ on every actionable Kanban card.** Click → confirm modal → POST. New endpoints:
  - `POST /workflow/cancel/source/<name>` — halts one scraper source's poll loop. The poll loop now reads the Redis status hash each tick and sleeps when set to `halted`.
  - `POST /workflow/cancel/agent/<name>` — proxies to the existing per-agent halt channel.
  - `POST /workflow/cancel/historian/<batch_i>` — marks the batch as failed/cancelled.
- Visible on: source cards (Sleeping lane), agent cards (Doing / Active lanes), historian batch cards (Doing), historian aggregate card (when error/ok).

## 2.24.062 — 2026-05-25
- Country canonical form flipped: canonical id/name is now the **human-readable full name** ("Israel", "United States", "United Kingdom", "European Union", …). ISO-2 / ISO-3 codes (IL/ISR, US/USA, …) become aliases of the canonical node. Re-run the 🧹 Dedup button on the graph to apply to existing nodes.

## 2.24.061 — 2026-05-25
- 🧹 **Dedup** button on the graph toolbar. One click → previews how many alias-duplicate entity nodes will be merged (US↔USA, IL↔ISR, Iran↔IRN, etc.), confirms, then runs the merge through `POST /graph/normalize_aliases` and reloads. Cache invalidated automatically.

## 2.24.060 — 2026-05-25
- **Future-proof entity dedup**: new `plata/core/entity_aliases.py` maps country aliases (US/USA/UNITED_STATES/America → `USA`, IL/ISR/Israel → `ISR`, IR/IRN/Iran → `IRN`, +30 more) to a canonical ISO-3 id. `graph_ingestion` now canonicalises every entity before upserting — duplicate nodes stop being created.
- **One-shot history merge**: new `POST /graph/normalize_aliases` endpoint. Default `?dry_run=true` previews which alias nodes would be merged into their canonical sibling; `?dry_run=false` actually does it (unions aliases, averages sentiment_ewma, rewrites every edge that points to the alias, deletes the alias node). Safe to re-run.
- Graph events selector now supports **20 / 40 / 80 / 150 / 300 / 500 / 1000**. Backend `/graph/data?limit=…` upper bound raised to 2000.

## 2.24.059 — 2026-05-25
- **Graph is now actually readable.** Three new controls in the top bar + a row of filter chips:
  - **Layout** selector: Constellation (force-directed, default), **Hub & spoke** (concentric — hubs in center, leaves on the rim), **Tree** (breadthfirst), Grid, Circle.
  - **Min connections** slider (default 2) — hides one-shot events that don't share entities with anything else, the biggest source of noise.
  - **Type chips**: toggle Events / Countries / People / Companies / Orgs / Assets / Tickers visibility.
  - **Category chips**: toggle crypto / macro / regulatory / company / geopolitics / tech / other.
- Changing any control re-renders from the in-memory cache (no network fetch), then re-saves to localStorage.

## 2.24.058 — 2026-05-25
- `trade_sampler` now caches the latest price **per symbol per tick** — N trades sharing a symbol = ONE Bybit fetch, not N.
- "Sampled" log line distinct-deduplicates the symbol list (so 3 XAUUSDT trades show as `(1 symbol(s): XAUUSDT)` instead of `XAUUSDT, XAUUSDT, XAUUSDT`).

## 2.24.057 — 2026-05-25
- Graph: **circle/halo behind every node removed** — the emoji (flag, 👤, 🏢, 📰, etc.) is the node on its own, no background fill, no border ring.
- Graph: **icon stays a stable on-screen size when you zoom in**. Logical node width is shrunk in proportion to `sqrt(zoom)` (capped at 3.5×), so icons don't dominate the canvas at high zoom; text label scales down with them.

## 2.24.056 — 2026-05-25
- **Live updates via SSE.** Producers publish to Redis channel `dashboard:events` at four moments: new HITL proposal pending, proposal approved/rejected, trade opened, trade closed, system halted/resumed. The frontend opens `EventSource('/sse')` once per tab and reacts immediately: toasts for every event, `plata:pending_changed` / `plata:trades_changed` custom events for pages to refresh their tiles, instant recolor of the system-state badge.

## 2.24.055 — 2026-05-25
- Fix the **Changelog modal "No changelog"** bug: `CHANGELOG.md` wasn't included in the Dockerfile `COPY` line, so the deployed image had no file for `/api/changelog` to read. Adding it. After redeploy the version-chip popup shows the full history with Older / Newer paging.

## 2.24.054 — 2026-05-25
- **Historian seed is now resumable** and **auto-resumes on dashboard startup**. The seed records `next_batch` in Redis after each batch; if the container is killed mid-run (Railway deploy, OOM, whatever), the next dashboard boot detects `state=running|stale` + a non-final `next_batch` and continues from that batch — keeping the written counter, brief, window, focus, and category limits intact. No more 30/100 forever.

## 2.24.053 — 2026-05-25
- **Graph icons are now the node itself**:
  - Event nodes show 📰 inside a colored circle (category color).
  - Country entities show the **flag emoji** (🇺🇸 USA, 🇮🇷 IRN, 🇮🇱 ISR, 🇪🇺 EUR + 30 others). Plain 🌍 fallback for unknown countries.
  - Person entities try a small first-name lookup → 👨 / 👩, otherwise 👤.
  - Org 🏛️, company 🏢, asset 💰, ticker 💹.
  - Type color is now a halo border, not the background — the icon is unobstructed.
  - Text label sits underneath the icon (no more icon-in-label).
- **Double-click anywhere on the graph zooms in 1.6× centered on the click point** (Cytoscape `dbltap`, animated 220ms).
- Legend updated to reflect the icon-as-node model.

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
