# Changelog

Each entry is one deployed version. Most recent first.

<<<<<<< HEAD
## 2.24.147 — 2026-06-01
- **🐛 ROOT-CAUSE FOUND: reddit poll has been silently NameError-ing since v2.24.130.** The constant `SUBREDDITS` was deleted when subreddits became config-driven, but line 53 of `reddit.py` still referenced the uppercase global. Every poll raised `NameError: name 'SUBREDDITS' is not defined` and the runner's blanket try/except just bumped the error counter. **No items ever made it through Reddit** — explains the lifetime=0 you saw. Fixed: loop variable now reads the lowercase local `subreddits` populated from config.
- **🔬 Real per-source poll probe — actual evidence, not hand-wavy diagnosis.** New `scraper:source:<name>:probe` Redis hash captures what really happened on the last poll:
  - `http_status` (200 / 401 / 429 / 500…)
  - `final_url` (post-redirect — useful when GDELT or RSS goes elsewhere)
  - `item_count` (items the upstream API actually returned)
  - `response_size` (bytes — 0 = empty body)
  - `sample` (first 300 chars of the response — shows error messages from the API)
  - `error_type` / `error_message` (Python exception class + tail)
  - Per-source extras: `per_feed` for RSS (which feeds returned what), `subreddits` for Reddit.
- **🔎 `/news/` shows a "Last poll probe ↓" disclosure** under each Diagnosis cell. Click to expand and see HTTP code, final URL (clickable), item count, response sample, and any error. This is what should have been there from the start — concrete evidence per source.
- Wired into all four sources: gdelt, cryptopanic, reddit, rss.

=======
>>>>>>> origin/master
## 2.24.146 — 2026-06-01
- **🩺 Per-source Diagnosis column on `/news/`** replaces the static "How to verify" text. Each row now tells you concretely why it's producing zero results. Ladder of checks (first match wins):
  1. Scraper agent heartbeat older than 180s → "Scraper agent heartbeat is Nm old — ingestion_hub container is probably dead. Restart on Railway; no source will poll until then."
  2. Last poll older than `interval × 3` (and > 10 min) → "Last poll was Nh Nm ago — interval is Xs, so this should have polled multiple times. Scraper task is wedged."
  3. Reddit creds missing → tells you exactly where to set them.
  4. RSS feeds list empty → tells you exactly where to add one.
  5. Source disabled in config → tells you which checkbox to enable.
  6. Last poll errored → surfaces the exact error message.
  7. Polled 5+ times with raw=0 → "the upstream API may be returning empty for the current query/config" (for GDELT: query too narrow / rate-limit).
  8. Otherwise: ✓ Healthy.
- Color-coded: 🛑 red for actionable errors (restart needed / wedged), ⚠ amber for config gaps (missing creds, empty feeds, narrow query), ℹ gray for informational.

## 2.24.145 — 2026-06-01
- **⚡ `/workflow/` kanban loads in ~1s instead of ~7s.** `_gather` did **nine sequential awaits** (state, sources, active, doing, done, historian, batches, pending HITL, ready streams) — each waiting on the previous. Now they run with one `asyncio.gather` and the page waits on max(slowest), not the sum.
- **⚡ Per-key Redis reads now pipelined inside the hot card-builders.** `_source_cards`, `_active_cards`, `_doing_cards`, `_done_cards` were doing N+1 round-trips (SCAN → for each key: HGETALL, LRANGE). Now: collect keys with one SCAN, batch all HGETALLs in one pipeline, batch all LRANGEs in a second pipeline. Same data, ~10× fewer round-trips.

## 2.24.144 — 2026-06-01
- **🐛 Fix `/workflow/` 500 (`WRONGTYPE Operation against a key holding the wrong kind of value`).** v2.24.137 introduced per-source recent-poll rings stored as Redis LISTS at `scraper:source:<name>:log` (via `LPUSH`). Several places that `SCAN` for `scraper:source:*` were calling `HGETALL` on those list keys — the per-source hash and the per-source log list both match the pattern. Added `if k.endswith(":log"): continue` guards at four sites: `/workflow/` `_source_cards`, `/workflow/resume/sources/all`, `_health_watchdog` scraper-resume sweep, and the auto-scrapers-halted-while-RUNNING check. `/api/resume` already had the guard (added in v2.24.138).

## 2.24.143 — 2026-05-29
- **🐛 Fix `/agents/` 500 (`KeyError: 'reviewer'`).** v2.24.141 had `per_agent.setdefault(agent, {})[date_iso] = per_agent[agent].get(...)` — Python evaluates the RHS before running `setdefault` on the LHS, so the lookup fired on a missing key. Refactored to capture the dict in a local first.

## 2.24.142 — 2026-05-29
- **🤖 Settings → Models tab with free/paid/auto modes + per-agent overrides.** When OpenRouter runs out of credit, Plata can now keep working on free OpenRouter models.
  - **`paid`** (default): use the per-agent paid model (Claude/GPT/etc.). Fails if credits are exhausted.
  - **`auto`**: start on paid; on a 402/credit/insufficient-balance error, automatically fall back to the agent's free model and **pin "free" for 1h** so subsequent calls skip the paid retry. Sticky pin auto-clears after 1h, or use the "Clear pin now" button after topping up credits.
  - **`free`**: every call uses the curated free model.
  - **Per-agent override**: an input under each agent lets you pick any OpenRouter model ID. Datalist suggestions surface the curated paid + free catalogs.
- **🎯 Curated free-model defaults per agent** (`AGENT_MODELS_FREE`):
  - `graph_ingestion` → `meta-llama/llama-3.3-70b-instruct:free` (most reliable free for structured JSON)
  - `strategist` / `reviewer` / `historian` → `deepseek/deepseek-r1:free` (long context, strong reasoning)
  - `risk_manager` / `position_monitor` / `translator` → `google/gemini-2.0-flash-exp:free` (fast, cheap, short prompts)
  - `scraper` → `qwen/qwen-2.5-72b-instruct:free`
- **🔄 LLMClient resolves the model per call**, not at construction, so live config changes take effect on the next message without a restart.

## 2.24.141 — 2026-05-29
- **🗄️ LLM cost moved to Postgres (durable). Redis becomes a fast tally only.** The old setup stored every cost in `cost:daily:*` Redis keys with **36-hour / 35-day TTLs** — meaning historical spend was being silently deleted as keys expired. Now:
  - New `llm_cost` table (alembic migration `20260529_0000`) — one row per LLM call with ts, agent, model, prompt_tokens, completion_tokens, cost_usd. Indexed on `(agent, ts)` and `(ts, agent)`.
  - `LLMClient.complete()` inserts a row per call on top of the existing Redis incrbyfloat (Redis kept for sub-ms budget enforcement and live header totals).
  - `/agents/` now reads spend from Postgres via a single `GROUP BY agent, DATE(ts)` query — durable history, no TTL surprises, and ~5× faster than the previous Redis SCAN+MGET approach.
  - **One-shot backfill on dashboard startup**: copies any existing `cost:daily:*:agent:*` Redis key into the new table (one row per `(agent, date)` with noon-UTC ts), then marks each key with a `:backfilled` sentinel so re-runs are idempotent. Today's accumulated spend survives the migration; tomorrow's writes go directly to Postgres.

## 2.24.140 — 2026-05-29
- **⚡ `/agents/` loads in <1s instead of ~15s.** The route used to do 12+ Redis `SCAN`s — one global, then one per-agent for the all-time total, then another global for the all-time daily sum. Each SCAN walked thousands of `cost:daily:*` keys. Rewrote as a **single SCAN + one MGET**: one pass collects every `cost:daily:*` and `cost:daily:*:agent:*` key, MGET fetches every value in one batch, then per-agent and per-window totals are computed from the in-memory dict. Status hashes are also pipelined now. Same result, ~15× faster.

## 2.24.139 — 2026-05-29
- **🐛 `/errors/` now logs why an agent went stale.** The health watchdog had `if age > 180 and not halted:` — so any agent that died **after** being halted (the common case here: executor and risk_manager were halted, then their containers died) got silently skipped by the staleness check. Nothing reached `ErrorLog`, so `/errors/` was empty even with 5 dead agents. Removed the `not halted` clause: the watchdog now logs every stale critical agent, includes a tail noting if it was also halted before going stale, and tells the user to restart the Railway container. Same 10-minute cooldown per agent so it doesn't spam.
- **🪧 Halted-agents banner ignores stale processes.** The "⚠ 2 agents halted: executor, risk_manager" banner kept showing for dead processes because `/api/agents/halted` only checked the `halted` field, not heartbeat age. Now: stale agents (heartbeat > 2 min) are excluded from the banner — they're dead, not pausable, and surfaced separately as STALE on `/agents/`. The banner is now reserved for truly-halted-but-alive agents the user can actually resume.

## 2.24.138 — 2026-05-29
- **🐛 Resume-all wasn't clearing HALTED on dead-process agents.** The Resume button publishes `Channels.SYSTEM_RESUME` over pub/sub. Live agents clear their local `_halted` flag and report back. **Dead processes never receive the message**, so their `agent_status:<name>.halted=True` flag stayed set forever — the UI kept showing `HALTED` even after the container was restarted. Now `/api/resume` also clears the `halted` field on every `agent_status:*` hash; live agents overwrite it correctly via the existing pub/sub path, dead ones get correctly downgraded to `STALE` (the heartbeat-age signal). The Resume-all confirm dialog also calls out that STALE = dead process, not halted, and that Railway needs to restart the container.
- **🩺 STALE now wins over HALTED on the agent card.** Was: a halted-then-dead agent rendered `HALTED` and the user kept clicking Resume hoping it would help. Now: if heartbeat is older than 2 minutes, the pill is `STALE` regardless of the halted flag — honest signal that the process is unreachable.
- **🔁 Resume-all toast reports what changed.** Old: `Resume-all sent.` New: `Cleared halted flag on N agent(s), M source(s). Refreshing…` plus an auto-reload so the user sees the new state instead of having to refresh manually.

## 2.24.137 — 2026-05-29
- **🔬 Per-source poll telemetry.** "Fetched = 0" was hiding three different stories (source returned nothing / everything was a dup / everything got filtered). Each poll now records `raw / published / dup / filtered` plus the filter reasons, and the scraper writes them to the source's Redis hash + a 20-entry ring (`scraper:source:<name>:log`). The `/news/` source-schedule table now shows the per-poll and lifetime counts side by side, colour-coded (emerald = published, gray = dup, amber = filtered).
- **📊 New "Recent polls" page per source.** `/news/source/<name>/log` lists the last 20 polls of one source with per-poll raw/published/dup/filtered, the filter-reason breakdown, and a sample of titles. New "📊 Recent polls" action on each schedule row. The page also surfaces lifetime totals (polls / raw / published / dup / filtered) at the top so you can spot a source that's polling regularly but never publishing — and see why.

## 2.24.136 — 2026-05-29
- **🌐 Translate-all now covers the Reasoning textarea and the Risk-snapshot `summary` field.** The batch translator previously read/wrote only plain text via `textContent`, so the re-submit form's reasoning `<textarea>` and the JSON dump in the Risk snapshot card were skipped. Two extensions:
  1. Element handlers now switch on `tagName` — `TEXTAREA`/`INPUT` use `.value` instead of `textContent`, so the reasoning field translates and you can re-submit the translated version.
  2. New `data-translate-json="<field>"` attribute lets a `<pre>` cell containing JSON have just one string field (e.g. `summary`) translated in-place while the surrounding JSON structure is preserved. Risk-snapshot pre tags get `data-translate-json="summary"`.
- Toggle-off still reverts both kinds back to originals via stashed `data-original-text` / `data-original-json-field`.

## 2.24.135 — 2026-05-29
- **📜 "Last results" link per source.** Each row of the `/news/` source schedule now has a `📜 Last results` button that opens `/history/?hours=24&source=<name>` — a filtered view showing the actual last N signals fetched by that source (title, URL, dedup status, fetched-at). Lets you confirm at a glance that a source is really producing usable signals after clicking ▶ Run now.
- **🔍 `/history/` accepts a `source` query param.** When set, only `SignalArchive` rows from that source are shown (trade/error/decision branches suppressed) so the page becomes a pure last-fetched view for one source. A blue banner explains the filter and links back to `/news/`.
- **🐛 Re-submit form no longer pretends a rejected proposal had 0.70 conviction.** When a proposal was dropped before the strategist scored it (e.g. `below_threshold: sentiment_magnitude 0.30 < threshold 0.35`), `p.conviction` is `None` but the clone-&-edit form was pre-filling the conviction field with `0.70` — making the row's `CONV —` and the expanded view's `0.70` look inconsistent. Now the form still uses `0.70` as a placeholder (so the field is valid) but labels it `⚠ placeholder` with a tooltip explaining the original had no conviction.

## 2.24.134 — 2026-05-29
- **🐛 Fix `/news/` source schedule disappearing after a few seconds.** The v2.24.133 auto-refresh JS was rebuilding the wrapper element on every tick — the regex grabbed the inner table, then the replacement inserted a new outer wrapper inside the existing one, doubling the header and eventually emptying the table. Removed the broken auto-refresh; the schedule now stays put. Added a simple "↻ refresh" link so the user can force a re-render manually after clicking ▶ Run now.
- **🔢 Row IDs + verification hints in the source schedule.** Each row now has a visible ID (1–4) and a "How to verify" column with a concrete check. You can reference rows by ID when reporting an issue ("row 3 didn't fetch") instead of source name.

## 2.24.133 — 2026-05-29
- **🩺 `/agents/` and `/activity/history` now agree.** The Agent Health cards showed every agent as `RUNNING` even when the orchestrator was emitting `Agent X appears dead` warnings for hours — the pill was reading `agent_status:<name>.halted` but never checked the heartbeat age. Now if `last_heartbeat` is older than 120s, the card shows a `STALE` pill in red, matching the orchestrator's death detector. The two pages tell the same story.
- **📰 News page = source schedule + "Run now".** Top of `/news/` now shows a per-source row: status (idle/polling/halted/error), last poll, last fetched count, interval, **seconds until next poll**, plus **▶ Run now** and Halt/Resume buttons. The scraper loop ticks every 2s checking a `run_now` flag, so manual triggers fire within seconds instead of waiting up to a poll interval. Auto-refreshes every 5s.
- **🔁 Position-monitor no longer spams `offtrack:close` proposals.** When a BTC long sat off-track for 16h, the monitor was creating a new `adjustment_suggested` row every 30 minutes (the LLM cooldown was throttling the LLM call but not the proposal row). Now: if there's already an open `adjustment_suggested` for the same trade, the monitor skips creating another one. `/proposals/` stays clean.
- **🪣 Allowlist is advisory by default.** Added `require_keywords_enforce` toggle (default OFF) to news config. When OFF, the allowlist no longer hard-gates signals — only the blocklist enforces. Prevents the strategist from starving on a too-narrow keyword list (the previous default was tossing 286+ "too quiet" stories, of which many would have been valid macro/equity content the strategist could have judged on its own).

## 2.24.132 — 2026-05-29
- **📰 News pipeline page moved out of Settings into sidebar → Knowledge → News pipeline.** Settings is for operator knobs (risk, API keys, environment); the news editor is a knowledge-domain workflow (which feeds to ingest, what to allow through), so it lives next to History / Graph / Historian seed. New page at `/news/`. Legacy `/settings/news/save` + `/settings/news/filter_drops/reset` POSTs 307-redirect to the new URL for any open tab.

## 2.24.131 — 2026-05-29
- **🐛 News tab now actually opens.** v2.24.130 added the `📰 News` tab button + panel but the inline tab-activator JS still had a hard-coded allowlist (`['controls','risk','account','env','api','tuning']`) — clicking News did nothing because the new key wasn't in the list, and the panel stayed hidden so the page kept showing whatever default (Controls) was active. Also removed the stale `'tuning'` from the list since that tab moved to `/tuning/` in v2.24.130. Result: `/settings/?tab=news` and clicking the tab both open the editor (sources, GDELT query, subreddits, RSS feeds, Telegram channels, allow/blocklists, min-title-length).

## 2.24.130 — 2026-05-29
- **📰 News tab on /settings/?tab=news** — editable from one place: enable/disable each source (GDELT, Reddit, CryptoPanic, RSS, Telegram channels), edit GDELT's boolean query, list of subreddits, list of RSS feeds (name | url, one per line), allowlist + blocklist keywords, minimum title length. All stored in Redis hash `news_config` and re-read on every poll — no restart needed.
- **🪣 Content filter applied before publish.** Every signal is checked against the allowlist (must contain at least one of N keywords) and blocklist (drop if any match) and a min-title-length, BEFORE it reaches the LLM. Drops are counted by reason on the same tab. Stories like "woman upset with neighbor about fence" never get past `block_keywords` / `no_required_keyword` now.
- **📡 Generic RSS source.** New `RssSource` polls every 5 min, reads the feed list from `news_config.rss_feeds`. Uses the already-installed `feedparser` dep.
- **🤖 Telegram channel ingestion.** The bot can now act as a news source. Add it to a channel/group, DM it `/joininfo` (run inside the target chat to discover its ID), then paste the ID into Settings → News → Telegram channel IDs. Messages from those chats are published as `RawSignal(source=TELEGRAM)` and flow through the same content filter and graph ingestion pipeline. New `/joininfo` command + chat-message handler in `plata/hitl/telegram_bot.py`.
- **🎚️ Tuning moved out of Settings into a Trades submenu.** It was awkward as a settings tab — tuning is a per-trade decision workflow. New page at `/tuning/`, surfaced under sidebar → Trading → Tuning. Old `/settings/tuning/{id}/{action}` POSTs 307-redirect to `/tuning/{id}/{action}` for the brief window between deploy and any open browser tab.

## 2.24.129 — 2026-05-29
- **🌐 "Translate all" is now one batched LLM call** instead of N sequential round-trips. Items in a `data-translate-zone` are joined with a `<<<---PLATA_SPLIT--->>>` marker, sent in a single `/api/translate/` request, then split back into each cell. Cache hits short-circuit per text, so partially-cached zones only send the misses. Toggle-off still works (click again → revert to originals). Net effect: translating the full proposal panel (event summary + strategist reasoning + 8 analogs) goes from ~10 serial calls to 1.
- **🐛 Strategist reasoning now actually translates** as part of the zone — it was already marked `data-translate-item` but the old per-row loop made it easy to miss; the batch path always includes it.

## 2.24.128 — 2026-05-29
- **🎨 Proposal expanded view layout fix.** The v2.24.126 translate-zone refactor put `data-translate-zone` directly on the `grid grid-cols-1 lg:grid-cols-2` container, so the injected "🌐 Translate all" button became its own grid cell — taking up a full column and leaving a giant empty box. Now the zone wraps the grid instead of being it, so the button sits cleanly above the two-column layout.

## 2.24.127 — 2026-05-29
- **🐛 Fix 500 on `/trades/<ulid>`** — `TemplateSyntaxError` from a missing `{% endif %}` introduced in v2.24.124 when the Unrealized/Net PnL split landed. The outer `{% if not trade.closed_at and live.price %}` had no matching close before `</section>`, so the Jinja loader rejected the template. Added the missing endif. Trade detail loads again.

## 2.24.126 — 2026-05-29
- **🐛 CloudFront 403 / "blocked from your country" now treated as a regulatory block** (paper fallback) instead of DLQ. Bybit's testnet sits behind a CloudFront distribution that geo-blocks several countries; ccxt surfaces this as `RateLimitExceeded` (misleading) with body `"The Amazon CloudFront distribution is configured to block access from your country."`. Executor's exception detector now matches `cloudfront`, `blocked + country`, and `403 + country|geo|blocked` in addition to the existing `retCode 10024` / `PermissionDenied` / `regulatory` signatures. All route to the same `regulatory_fallback=true` paper fill + `venue:blocked:<venue>` Redis flag + 10-min `VenueRegulatoryBlock` WARN on `/errors/`.
- **🌐 One Translate button per zone.** The expanded detail panels on `/proposals/` and `/trades/<ulid>` used to show a 🌐 button next to *every* text block (event summary, strategist reasoning, each of the 8 analog summaries, milestones …). Now a **single "🌐 Translate all"** button appears at the top of the zone; clicking it translates every block in sequence. Implemented as a new `data-translate-zone` pattern in `base.html`. Legacy single `data-translate` still works for standalone blocks not inside a zone.

## 2.24.125 — 2026-05-29
- **🐛 `guard_one_per_symbol_side`** — new risk guard (default ON). Rejects any new strategist proposal whose `(symbol, side)` already has an open trade. **Stops the recurring duplicate-GLD-longs problem.** New events on already-held symbols flow through the position monitor's event loop instead, which decides `scale_up / scale_down / close`. Toggle on `/settings/?tab=risk` → Guards. Rejection reason: `already_holding:<symbol>:<side>`.
- **📲 Push notifications fire on actionable events.** New `_push_relay` background task in the dashboard subscribes to `dashboard:events` and fans out a web push (via existing pywebpush + VAPID) to every saved subscription whenever:
  - `proposal_pending` — "Plata · New proposal · SPY SHORT — awaiting your approval"
  - `adjustment_suggested` — "Plata · Position adjustment · monitor suggests close on GLD"
  - `system_state == HALTED` — "Plata · System HALTED — tap to manage"
  Works on iPhone (add Plata to home screen as a PWA, then enable from the bell dropdown) and Chrome (just enable). Pushes deep-link to the relevant page when tapped.
- **🔔 Bell is the only notification icon now.** Removed the old greyed-out push-toggle 🔔 (sm:inline only, easy to confuse with the bell). Push opt-in moved into a footer at the bottom of the bell dropdown — "📡 Push delivery · Enable on this device".
- **Graph filters rebuilt as Flowbite advanced-table style.** Single bordered card holds everything:
  - Search input (with magnifying-glass icon) live-filters the entity pin chips.
  - "Show" dropdown with checkboxes for node types (events / countries / people / ...).
  - "Categories" dropdown for event categories.
  - Sub-row with Layout / Events / Min weight selectors.
  - Heaviest-entity pin chips on a third row, populated by JS.
  - Reload + 🧹 Dedup as proper Flowbite buttons.
- **🐛 Historian `JSONDecodeError: Unterminated string`.** LLM was hitting the default `max_tokens=2048` mid-output for 10-event batches. Bumped structured-output default to 8192 and added a wrapper that converts truncations into a clear `RuntimeError` with `finish_reason` + content tail (so callers can see "this got truncated" instead of a raw decode trace).

## 2.24.124 — 2026-05-29
- **Trade detail — live unrealized PnL for OPEN positions.** The summary header used to show `—` for Exit / Notional at close / Net PnL on open trades; you'd only see PnL after they closed. Now: when the trade is still open, those three cells flip to **Current price** (mark-to-market from the symbol watch, with the price drift % next to it), **Notional now**, and a prominent **Unrealized PnL** ($ + %), all color-coded. Each carries a small pulsing 🟢 indicator that updates as the sampler ticks.
- **Per-position auto-close rules card.** New section inside the Close-now card on `/trades/<ulid>`. Set deterministic hands-off rules — the position monitor evaluates them every minute and publishes a closure the instant any fires:
  - **Loss in $ ≤ −$X** (e.g. -50.00 USD)
  - **Loss % ≤ −X%** (e.g. -4%)
  - **Trailing peak drawdown** ≤ -X% from the best unrealized PnL the position touched (Redis-tracked)
  - **Close after N days** from the rule-set time
  - **Loss within window** — close if PnL drops Y% over the last N days (walks `trade:samples:<ulid>`)
  - Saved on `trade_ledger.raw_bybit_response.auto_close_rules`; `Clear all` button wipes them. New POST `/trades/<ulid>/auto_rules`.
- **Symbol detail page — entry/exit markers on the chart + total realized.** `/positions/<SYMBOL>` chart now overlays one annotation point per open or closed trade: `↑ LONG 1.00 ($750)` / `↓ SHORT 0.50 ($375)` for entries, `× exit +5.74` for closes, color-coded by side / PnL. Side rail adds **Realized (all time)** ($, count) and a bolder **Total on this symbol** = realized + unrealized.
- **Positions page — Open / Closed split.** Three pills above the table (🟢 Open / ✓ Closed / All) with live counts; click to filter. Choice persists in `localStorage` (defaults to **Open**). Works alongside the existing Group-by / Sort-by controls.
- **🔔 Bell icon is now a clear emoji** instead of a thin SVG outline that some browsers rendered nearly invisible. Wrapped in a circular hover target so the tap area is consistent with the avatar pill next to it.

## 2.24.123 — 2026-05-29
- **Trade detail — % deltas next to the dollar figures.** On `/trades/<ulid>` the summary header now shows:
  - **Notional at close** $X · `±Y.YY%` — price drift from entry to exit (sign-agnostic; just the % the underlying moved). Green if up / red if down.
  - **Net PnL** ±$X · `(±Y.YY%)` — PnL as % of Notional invested. Sign reflects your side (long: positive = up move; short: positive = down move). This is the PnL view that matches your account balance.
  - On your XAUUSDT short: entry 4745.24 → exit 4473.04 shows `−5.74%` notional drift, but Net PnL `+5.74` is `+5.74%` because you were short.

## 2.24.122 — 2026-05-29
- **🔔 Bell + dropdown replaces ephemeral toasts.** New bell icon next to the avatar with an unread-count badge. Click → dropdown panel with two tabs:
  - **🔔 Notifications** — action-required items: `proposal_pending`, `adjustment_suggested` (from the position monitor), system-halt events. Each item is clickable and takes you to the relevant page (`/proposals/?state=pending_hitl`, `/proposals/?symbol=<sym>#detail-<ulid>`, `/agents/`).
  - **📜 Activity** — informational: `trade_opened`, `trade_closed` (with signed PnL), `proposal_resolved`, system resume.
  - Items persist in `localStorage` (last 50 each) so opening a new tab still shows what happened while you were away. Opening the panel clears the unread count. `clear` empties both feeds.
  - Toasts now only fire for **critical** events (system HALTED) on top of the bell notification — everything else goes straight to the bell.
- **👁 Eye-toggle reveal panel** in the topbar — open notional ($), open positions count, realized today, and % change today / 7d / 30d / all-time against `account_baseline_equity_usd`. Persists open/closed state.
- **📋 "Copy all settings" button** on `/settings/?tab=risk` — copies every risk_config key/value to clipboard as a markdown table so you can paste back to me for investigation.
- **🐛 SSE 500s on quiet Redis pub/sub.** `subscribe()` was using `pubsub.listen()` which blocks indefinitely; Railway's idle-timeout on the redis socket bubbled up as `redis.exceptions.TimeoutError` and tore down the SSE response every few minutes. Switched to `pubsub.get_message(timeout=30)` in a loop with explicit catches for `TimeoutError` (continue) and `CancelledError` (re-raise). Idle SSE streams now survive forever.
- **🐛 `/proposals/<ulid>/resubmit` ValidationError when source had no triggering event.** Some legacy / monitor-suggested rows have `triggering_event_ulid=None`; the schema requires `str`. Resubmit now falls back to `""`.

## 2.24.121 — 2026-05-28
- **👁 Eye-toggle reveal panel on the topbar.** New button at the right of the KPI strip; click to open a small floating panel anchored under the topbar showing what the topbar can't fit:
  - **Open notional** ($) — sum of qty × entry across every open position
  - **Open positions** count
  - **Realized today** ($ signed)
  - **% Today / 7d / 30d / All time** — color-coded against `account_baseline_equity_usd` (default $10,000; editable on `/settings/?tab=risk` → Capital → "Baseline equity (USD)")
  - Closes when you click outside, the ✕, or toggle again. Open/closed state persists in `localStorage`.
- **📋 "Copy all settings" button** on `/settings/?tab=risk`. Builds a markdown table of every risk_config key/value currently on the page (both friendly cards and advanced raw rows) and copies it to the clipboard, so you can paste it back to me when something looks off. Includes the app version and timestamp at the top of the snapshot.
- **🐛 SSE 500s on quiet Redis pub/sub.** `subscribe()` was using `pubsub.listen()` which blocks indefinitely; Railway's idle-timeout on the redis socket bubbled up as `redis.exceptions.TimeoutError` and tore down the SSE response. Switched to `pubsub.get_message(timeout=30)` in a loop with explicit catches for `TimeoutError` (continue) and `CancelledError` (re-raise). Idle SSE streams now survive forever.
- **🐛 `/proposals/<ulid>/resubmit` ValidationError when source had no triggering event.** Some legacy / monitor-suggested rows have `triggering_event_ulid=None`; the `TradeProposal` schema requires `str`, so the clone constructor blew up. Resubmit now falls back to `""` for empty values.

## 2.24.121 — 2026-05-28
- **🐛 Sidebar active-highlight was stuck on the last full-reload page.** With htmx-boost only swapping `#main-content`, the sidebar (rendered once at full page load) kept its server-rendered `bg-gray-200` highlight on whatever page you originally landed on — so navigating from /trades/ to /proposals/ would leave **Positions** highlighted on the proposals page. Fixed with a client-side `updateSidebarActive(pathname)` reconciler that runs on every boosted swap + on initial load: longest-prefix-match against every sidebar `<a>`'s href, clears the old highlight class, applies the new one. Also handles deep paths (`/trades/<ulid>` correctly highlights **Positions**).
- **Auto-approve adjustments above a conviction threshold.** New `monitor_auto_approve_conviction_threshold` in `risk_config` (default **0.6**, editable slider on `/settings/?tab=risk` → **Auto-approve above conviction**). When the position monitor's LLM returns a `close / scale_up / scale_down` decision with `conviction ≥ threshold`, it's auto-applied — bypasses the per-action HITL toggles (`monitor_auto_close_offtrack` / `_scale_up` / `_scale_down`). Set the slider to 1.0 to disable and keep every adjustment HITL.

## 2.24.120 — 2026-05-28
- **New `position_monitor` agent.** Runs in `intelligence_sandbox` alongside strategist/reviewer. **Two concurrent loops** that finally close the long-standing "nothing watches open positions" gap:
  - **Periodic loop (every 60s, configurable):** for every open trade — (1) **SL/TP auto-exit** — publishes a `TradeClosure` the moment price crosses `sl_price` or `tp_price`. This was the silent missing piece; paper-mode trades with stops set never auto-closed before. (2) **Timeout** — auto-close after `monitor_max_hold_min` (default 7d). (3) **Drift judgement** — linearly interpolates the strategist's milestone trajectory at the trade's current `hours_from_entry`, compares to actual price. Status `on_track / drifting / off_track / untracked`, written to `position:health:<ulid>` Redis hash. (4) **Off-track LLM evaluation** — when status is `off_track` and the trade hasn't been re-evaluated for `monitor_llm_cooldown_min`, the LLM (Claude Haiku 4.5) decides `hold / close / scale_down` → writes a new `adjustment_suggested` proposal row.
  - **Event loop (consumes `enriched_events:stream`):** when a new event with `sentiment_magnitude ≥ monitor_event_sentiment_min` (default 0.7) mentions a symbol you already hold, the LLM judges `hold / close / scale_up / scale_down` → `adjustment_suggested` row referencing the triggering event.
- **HITL by default for adjustments.** All adjustment-suggested rows appear on `/proposals/` with state **🔄 Adjust?** for your approval. SL/TP/timeout auto-exits are immediate (toggleable via `monitor_auto_close_sl_tp` / `monitor_auto_close_timeout`).
- **Proposals page — three new states:** 🔄 **Adjust?** (`adjustment_suggested`) / 🔁 **Adjusted** (`adjustment_executed`) / ⛔ **Skipped** (`adjustment_rejected`). The `/proposals/<ulid>/decide` endpoint now handles approval: `close` publishes a `TradeClosure`; `scale_up` emits a sized manual-override that goes straight to the executor; `scale_down` is approximated as a full close for portability.
- **Trade detail page — Health card** appears above Strategist proposal showing current status, predicted vs actual %, deviation, and a banner linking to any pending adjustment suggestion. **Positions table — Health column** with a compact ✅ / ⚠ / 🛑 / · chip + tooltip.
- **Settings → Risk — new "Position monitor" group** with 11 sliders/toggles (check interval, drift / off-track thresholds, max hold, LLM re-eval cooldown, event sentiment threshold, and five auto-vs-HITL toggles). All `danger`-styled for the four "auto" toggles since they cede control to the LLM.
- **Health watchdog** picks up the new agent: `position_monitor` is added to `CRITICAL_AGENTS`, so a stale heartbeat fires a `AgentStaleHeartbeat` WARN on `/errors/`.
- **What to test:** open a small paper trade with SL set close to entry. Within ~60 s of the next sampler tick crossing SL, the trade should auto-close with status **🛑 SL hit** on `/trades/`. With milestones present on a longer trade, watch `/trades/<ulid>` for the Health card to fill in within a minute. Force a manual milestone deviation → expect drift → off-track → an `adjustment_suggested` row on `/proposals/?state=adjustment_suggested`.

## 2.24.119 — 2026-05-28
- **🐛 LLM spend under-counted vs OpenRouter dashboard.** Our local cost estimate was `prompt_tokens × $/M + completion_tokens × $/M` using hardcoded per-model prices. That **misses real OpenRouter charges** in three places:
  1. **Cached input tokens** — OpenRouter discounts them but the `prompt_tokens` field doesn't split cached vs non-cached, so our math can drift either direction depending on cache-hit rate.
  2. **Reasoning tokens** — thinking models (o1 / o3 / Sonnet with thinking) bill these separately, often at output-rate. Not in `completion_tokens`.
  3. **Image / tool tokens** — billed at distinct rates we don't model.
  - In your case OpenRouter shows $17.40 but Plata reported $11.47 → about a $6 gap concentrated on Sonnet 4.6.
- **Fix:** the LLM client now sends `extra_body: {"usage": {"include": true}}` on every chat call, which makes OpenRouter return the **actual billed cost** in `response.usage.cost`. We use that real number when present and fall back to the local estimate only for non-OpenRouter responses. Every call also stashes both numbers in the Langfuse trace metadata (`cost_usd`, `cost_reported`, `cost_estimated`) so you can audit the delta after deploy.
- **Effect:** after this deploy, the daily-total card on `/agents/` should reconcile to the OpenRouter dashboard within rounding. Existing historical rows aren't backfilled (we can't reconstruct the real billed cost from past calls) — only new calls will be exact.

## 2.24.118 — 2026-05-28
- **🐛 `/trades/` 500 (`unsupported format character ','`)**. Python's `%` operator doesn't support the thousands-separator flag, only `str.format()` does. The new Notional column used `'%,.2f' % x` which works in f-strings but not in `%`-format. Switched to `'{:,.2f}'.format(x)` everywhere it was added (positions list + trade detail).
- **🐛 Stock symbols (NVDA, GLD, SPY, …) were being routed to Bybit** because `_client_for(symbol)` silently fell back to `self._bybit` whenever Alpaca wasn't initialized — Bybit then raised `BadSymbol: bybit does not have market symbol GLD` and the trade hit the DLQ. Two-layer fix in `agents/executor.py`:
  1. `_client_for` now returns `None` when the venue's client isn't configured (instead of returning the wrong venue).
  2. Executor detects `client is None` before calling the venue + catches any `BadSymbol` exception that slips through — both paths now **fall back to a paper-mode fill** with a `bad_symbol_fallback=true` or `unconfigured_venue=alpaca` audit flag. Same self-healing as the regulatory block path from v2.24.114.
  3. Sets `venue:blocked:<venue>` in Redis so the health watchdog writes a `VenueRegulatoryBlock`-style warning to `/errors/` (with `reason=unconfigured` or `reason=bad_symbol`).
- **🐛 Activity page jumped to top every 5 seconds.** The htmx-boost scroll-reset handler in `base.html` (added in v2.24.107) ran on **every** `htmx:afterSwap` event — including the activity feed's 5s auto-poll into `#live-pane`. Now scroll-reset only fires when the swap target is `#main-content` (i.e. a real boosted page navigation), not sub-component polls. Reading the live feed no longer yanks you back to the top.

## 2.24.117 — 2026-05-27
- **Proposals page: trigger info on every row.** New **Trigger** column shows the event title (2-line clamp), source (chip), category (chip), sentiment magnitude (color-coded chip: ≥0.7 red / ≥0.4 amber / else gray), signed polarity number, and inline ↗ source-link + ⌘ graph-link buttons. You can scan 50 rows and immediately see *what* triggered each proposal — no more "click Details to find out". If the event has expired from Redis (7-day TTL), shows a quiet "event expired (ulid…)" placeholder instead.
- **Positions: notional column added.** Every position row now shows **Notional = qty × entry price** (in `$N,NNN.NN`). Visible in:
  - `/trades/` table — new column between Entry and Current.
  - `/trades/<ulid>` detail header — two cards: **Notional invested** (at entry) and **Notional at close** (at exit, when closed).
  - Sortable: new `Notional ($)` option in the Sort-by dropdown.

## 2.24.116 — 2026-05-27
- **Proposals page rebuilt around the Flowbite advanced-table pattern** (per <https://flowbite.com/blocks/application/advanced-tables/>). Everything is now contained in a single bordered card:
  - **Header toolbar inside the card** — state filter pills + symbol/side/search form + drop-reason sub-row. No more loose chip strips floating above the table.
  - **Pill-style state chips** (rounded-full, blue when active, gray neutral) with icon + label + count, matching the Flowbite reference.
  - **Search input** with an inline magnifying-glass icon; symbol + side dropdowns sit beside it. Apply / Reset buttons styled as proper Flowbite primary / outline buttons.
  - **Table** uses `divide-y` rows, `hover:bg-blue-50/40` accent, smaller tracked-uppercase headers, conviction color-graded (≥0.7 green / ≥0.5 amber / else gray), state badge is now a rounded pill.
  - **Empty state**: friendly icon + "No proposals match this filter" + "Clear filters →" link when any filters are active.

## 2.24.115 — 2026-05-27
- **🐛 Halted-agents banner overlayed the topbar instead of pushing it down.** The red `⚠ N agents halted` banner was `fixed top-0 z-[59]` — same layer as the topbar but a higher z-index, so it sat ON TOP of the PnL chips and obscured them. Now: when the banner is shown, `body` gets a `banner-visible` class, and CSS shifts the topbar's `top` from `0` to `36px`, the sidebar's `top` to `36px` (with matching `height: calc(100vh - 36px)`), and `#main-content`'s `padding-top` from `5rem` to `calc(5rem + 36px)`. The whole layout pushes down cleanly; no more overlap with the topbar KPIs.

## 2.24.114 — 2026-05-27
- **🐛 Bybit regulatory block (PermissionDenied, retCode 10024) no longer DLQ's trades.** The venue refused live orders due to your account's KYC / region (`Dear User, The product or service you are seeking to access is not available to you due to regulatory restrictions`). Executor used to capture this as an ERROR and skip — every blocked proposal hit the dead-letter queue. Now: on detecting `PermissionDenied` / `retCode 10024` / `regulatory` keywords, the executor **transparently falls back to a paper-mode fill** for that trade (records it in the ledger with `raw_response.regulatory_fallback=true`), stores a venue-wide block flag in Redis `venue:blocked:bybit`, and continues. The health watchdog picks that up and writes a `VenueRegulatoryBlock` WARN to `/errors/` once per 10-minute window so you know live trading on Bybit is currently unavailable.
- **Proposals page: friendlier names + tooltips on every state badge.** "Dropped" was technically correct but vague. Renamed:
  - **Dropped → Not traded** 🛑 — strategist saw the event but didn't open a trade. Sub-reasons explain why.
  - Below threshold → **Too quiet** 📉
  - LLM said don't trade → **Strategist declined** 🤔
  - Event missing → **Event expired** ❓
  - No embedding → **Couldn't analyze** 🧬
  - Rejected → **Blocked by risk** 🛡️
  - Pending HITL → **Awaiting you** ⏳
  - HITL approved/rejected → **You approved / You rejected** 👤
  - Approved → **Risk OK** ✅
  - Executed → **Filled** 📈
  - Failed execution → **Venue error** 💥
  - Manual override → **Your override** ✋
  - Every chip + badge now has a hover tooltip explaining what the state actually means (what triggered it, what to do).
- **More filters on `/proposals/`:**
  - **Symbol** text input (was URL-only).
  - **Side** picker (long / short / any).
  - **Search** free-text box matching against `reasoning`, `state_reason`, and `symbol` simultaneously — useful for finding e.g. "every proposal that mentioned 'rate cut'".
  - Reset button appears when any are active. All filters preserve through paging via the querystring.

## 2.24.113 — 2026-05-26
- **Pagination on `/proposals/`.** With strategist drops being persisted (and 1000+ events flowing through some days) the page was bottlenecking on a 200-row dump. Now: 25 per page by default, with `Prev / 1 … N / Next` controls + a `Per page` selector (10 / 25 / 50 / 100 / 200). The footer shows `1–25 of 1,205 · filtered` when filters are active. All filter chips (state / reason / symbol) preserve via querystring through page navigation.
- Backend: new `count_recent()` helper in `core.proposals` for the `total` math, and `list_recent()` now takes an `offset` param so paging is true OFFSET/LIMIT against Postgres (not a slice in Python). The drop-reason filter (which lives in JSON, not a column) still uses a fetch-then-filter strategy, but only when a reason chip is active.

## 2.24.112 — 2026-05-26
- **Positions: Group-by + Sort-by controls.** Above the table:
  - **Group by** — `none` (default) / Status / Symbol / Side / Venue / Mode. When grouped, a sticky-styled group header row appears between sections showing `<group name> · N position(s) · ΣPnL` (green/red).
  - **Sort by** — Started (default) / PnL / % move / Held duration / Symbol A→Z / Conviction.
  - **Direction toggle** — `↓ desc` ↔ `↑ asc`.
  - **Summary** at the right shows total row count + ΣPnL across the entire view.
  - All client-side (no round-trip) — every row carries `data-symbol / data-side / data-venue / data-mode / data-status / data-started / data-held / data-pnl / data-pct / data-conviction`, the JS just resorts/regroups. **Choice persists in `localStorage`** so navigating away and back keeps your view.

## 2.24.111 — 2026-05-26
- **🐛 Sparkline on `/positions/` overflowed its card.** ApexCharts in sparkline mode renders at its default width (~600 px) on first paint until the layout settles, which spilled past the right edge of the card. Three fixes: `overflow-hidden` on the card, `w-full overflow-hidden` on the `.spark` div, explicit `chart.width: '100%'` in the ApexCharts config, plus a `ResizeObserver` that calls `updateOptions({ chart: { width: el.clientWidth } })` whenever the card resizes (window resize, sidebar collapse, htmx-boost swap). Sparkline now always fits.

## 2.24.110 — 2026-05-26
- **More context on every positions row.** Beyond the live PnL added in v2.24.109:
  - **Status** column with proper labels + tooltips:
    - 🟢 `Open` (pulsing) — "Position still open — live mark-to-market shown."
    - 🛑 `SL hit` — "Closed automatically by rule: sl"
    - 🎯 `TP hit` — "Closed automatically by rule: tp"
    - ✋ `Closed by you` — "Closed manually"
    - ⏱ `Timed out` — "Closed automatically by rule: timeout"
    - ⚠ `Kill switch` — "Closed automatically by rule: kill_switch"
  - **Started** + **Held** columns split out the timeline: when the trade opened, and how long it has been (or was) held (`12m` / `3h 14m` / `2d 5h`). Held ticks live for open positions.
  - **Conviction** of the originating strategist proposal shown under the ULID (`conv 0.78`).
  - **Symbol cell tooltip** = the strategist's reasoning preview (first 280 chars) — hover to remind yourself why this trade was even taken.
  - Bulk-loads `Proposal` rows in a single query keyed by `proposal_id` so adding these columns didn't add N round-trips.
- Closed column now just shows the relative timestamp; the reason is properly merged into the Status badge instead of duplicated.

## 2.24.109 — 2026-05-26
- **Positions table is now actually useful.** Was just ULIDs + raw prices. Now each row shows live market data + computed metrics:
  - **Symbol** (font-mono header + short ULID below).
  - **Side** as a coloured arrow (`↑ long` green / `↓ short` red).
  - **Qty** + **Entry** — same as before but right-aligned and trimmed to 4 decimal places.
  - **Current / Exit** — for open positions: live market price from the per-symbol watch cache (refreshed every 5 min). For closed: the exit price. Hover shows the cache timestamp.
  - **% move** — signed for the side (long: positive = up, short: positive = down). Green / red / gray.
  - **PnL** — realized for closed, **unrealized mark-to-market for open** (was always `—` before, now you can actually see how each position is doing without clicking in). Tooltip clarifies "unrealized" when relevant.
  - **SL / TP** — both threshold prices side-by-side (red SL / green TP) on lg+. `—` when unset.
  - **Venue · Mode** — `📈 alpaca` or `🪙 bybit` chip + paper/live chip (md+).
  - **Closed** column now includes the close reason (`sl` / `tp` / `manual` / `timeout` / `kill_switch`) underneath the timestamp. Open positions show a green `● open` indicator.

## 2.24.108 — 2026-05-26
- **Actions on an open position.** Trade detail page (`/trades/<ulid>`) gets a new **Actions** block visible only while the trade is open, with three side-by-side cards:
  - 🛑 **Close now (red).** Fetches the current ticker via the venue router, synthesizes a `TradeClosure` with `close_reason=manual`, publishes to `trade_closures:stream` — same path SL/TP/timeout closures take, so the reviewer updates the ledger + emits an SSE `trade_closed` event. Confirms with `plataConfirm` before firing. Falls back to the per-symbol watch cache if the venue ticker call fails.
  - 🎯 **Adjust SL / TP (amber).** Updates `sl_price` / `tp_price` on the ledger row. Reviewer reads these on every price sample for auto-exit. Either field can be blank to leave it unchanged.
  - 📝 **Add note (blue).** Pin a free-text thought to this trade. Stored in `raw_bybit_response.notes` with timestamp + actor email; rendered as a list at the bottom of the Actions block.
- New endpoints (POST, audited): `/trades/<ulid>/close`, `/trades/<ulid>/sl_tp`, `/trades/<ulid>/note`.

## 2.24.107 — 2026-05-26
- **🐛 Agent grid was hiding ~$4 of LLM spend** (sum of visible agents < total at the top of `/agents/`). The grid listed only agents with a live `agent_status:<name>` heartbeat hash; agents that crashed / were renamed / had their heartbeat key expire vanished from the grid even though their historical `cost:daily:<date>:agent:<name>` keys were still rolling into the daily totals. Likely candidates: enricher, historian, translator. **Now the grid takes the union** of `agent_status:*` keys + a Redis SCAN of `cost:daily:*:agent:*` so any agent that ever spent money appears as a row with a grey `STOPPED` badge (hover tooltip explains: *"Agent has historical LLM spend but no live heartbeat — service may have been removed, renamed, or its heartbeat key expired. Its past spend still contributes to the daily totals at the top."*). Math should now reconcile: Σ visible agents = total.
- **App-like nav (htmx-boost).** Sidebar + topbar links now swap only `#main-content` instead of full page reloads. Sidebar stays mounted, KPIs don't flicker, mobile drawer auto-closes after nav, history is pushed, browser back/forward work. A 2-pixel blue progress bar across the top during the swap. Flowbite widgets re-bind via `initFlowbite()` on `htmx:afterSwap`.
- **🐛 `/activity/` Bybit/Alpaca said NOT SET even when configured via the UI.** v2.24.077 fixed Settings → Environment but the Activity page's `_api_statuses()` still only consulted `settings.bybit_api_key` env-vars. Keys saved via the 🔑 API Keys tab live in Postgres. Activity now also consults `credentials.get_sync(<provider>)` for OpenRouter, Voyage, Bybit, Alpaca, Telegram, Langfuse, CryptoPanic, NewsAPI, CryptoNews, LunarCrush, WhaleAlert. Same source of truth across the dashboard.
- **Recent-signals table rebuilt** on `/activity/`:
  - **Header explains `dup` and `enriched`** inline (`dup` = collapsed onto a master signal, skipped enrichment to save LLM $; `enriched` = ran through the enricher LLM and became a graph event).
  - **`Lag` column** (md+) — source publish time → fetch time, formatted `Xs / Xm / Xh / Xd`. Surfaces scraper-cadence issues per source.
  - **Source as a chip** for readability.
  - **`Dup` column tooltip** on the badge: shows the master ULID when known (`Duplicate of signal 01J… — that one was enriched, this one was skipped`).
  - **`Enriched` column** (new) — green `✓ event` when `ingested_to_graph=true`, clickable through to `/graph/?focus=<event_ulid>` to see the event's neighbourhood. `—` when pending or dropped.
  - **Title shows body preview on hover** (first 400 chars in the tooltip), URL still opens in new tab.
  - Duplicate rows get `opacity-60` to visually de-emphasise without hiding.
- **What to test:** click sidebar items — content swaps, sidebar stays, URL updates, blue progress bar. `/activity/` external-API grid shows Bybit + Alpaca as `CONFIGURED` matching `/settings/?tab=env`. Hover any `dup` badge → see what it was deduplicated against. Click any green `✓ event` → graph page focused on the event.

## 2.24.106 — 2026-05-26
- **🐛 Sliders/toggles disappeared after Save.** The friendly grouped form on `/settings/?tab=risk` posted to `/risk_config/<key>/update`, which hardcoded a redirect to the legacy `/risk_config/` table view. So every Save threw you out of the nice UI and onto the raw key/value table.
- Fix: the `/risk_config/{key}/update`, `/create`, and `/delete` endpoints now redirect to a `?next=<path>` query param if provided, falling back to the `Referer` header, falling back to `/risk_config/` for back-compat. All four forms on the settings page now pass `?next=/settings/?tab=risk`. **Slider Save / toggle flip stays on the same screen.**

## 2.24.105 — 2026-05-26
- **🐛 Topbar `PAPER` badge is now dynamic** — was hardcoded as `PAPER` in the template, so toggling `paper_trading_mode` off in Settings → Risk flipped the underlying value in Redis (and the executor *was* going live to Bybit/Alpaca) but the badge kept lying. Now `/api/header_stats` returns `paper_mode`, the badge reads it on every refresh and flips to a **red `LIVE`** when off.
- **Why no new positions from the system in the last ~10 h with 6 open**: `max_open_positions` defaults to 3 in `risk_config`; every new strategist proposal hit the `max_open_positions_reached` gate (visible on `/proposals/?state=rejected` after v2.24.102). Either raise the cap on `/settings/?tab=risk` → *Portfolio limits → Max simultaneous positions*, or close some trades — the strategist will start filling slots again automatically.

## 2.24.104 — 2026-05-26
- **🐛 Fix `AttributeError: 'str' object has no attribute 'value'` in risk_manager.** The opposing-side guard read `proposal.side.value.lower()` — assumes `.side` is the `Side` StrEnum. In some serialization paths (in particular manual-override re-submits round-tripping through the Redis stream) it deserialized as a plain string, no `.value`. Switched to `str(proposal.side).lower()` which works for both — `Side` is a `StrEnum`, so `str(Side.LONG) == "long"`.

## 2.24.103 — 2026-05-26
- **Why some trades had no chart**: the strategist LLM returns a `milestones` array but the JSON schema allowed `minItems: 0`, so the model sometimes shipped an empty list. The trade-detail page only rendered the chart `{% if proposal.milestones %}`, so milestone-less trades looked broken.
- **Two fixes:**
  - **Schema tightened**: `milestones.minItems` bumped from 0 → 2 in `PROPOSAL_SCHEMA`. The LLM is now forced to produce at least 2 milestones whenever it says `should_trade=true`. New proposals from here on will always have a chart.
  - **Legacy trades still render**: chart block now renders even when `msData` is empty — falls back to *actual price line only* (orange) so you can still track the trade live. An inline amber note says `⚠ Strategist did not predict milestones for this proposal — showing live price only.` so you know why the predicted dashed line is missing.
- **What to test:** open any trade that previously showed no trajectory chart — the chart should now appear with the orange actual-price line and the amber notice. New trades created after this deploy will have the full predicted+actual overlay.

## 2.24.102 — 2026-05-26
- **🐛 Fix `/settings/` 500.** Two help strings in `risk_field_meta.py` used double-quote literals inside double-quoted Python strings (`"0.6 = "more confident than 50/50""`) — SyntaxError on import. Replaced outer quotes with single quotes on both rows. Confirms why the file passed local edits but exploded at import time on the server.
- **Rejected proposals no longer write to `error_log`.** `risk_manager._reject()` was calling `error_reporter.capture(severity="INFO", error_type="ProposalRejected")` on every gate failure — a `max_open_positions_reached` rejection (which is the system *working as designed*) was filling `/errors/` with noise. Rejections live on `/proposals/?state=rejected` with full reasoning; that's the right place to investigate them. Removed the capture call.
- **What to test:** load `/settings/` — page renders. Trigger a rejection (e.g. open enough trades to hit the cap, then watch a new proposal) — appears as a row on `/proposals/?state=rejected`, **not** on `/errors/`.

## 2.24.101 — 2026-05-26
- **Mobile header slimmed.** The KPI strip was pushing the hamburger menu out of reach on phones. New layout:
  - **Mobile (< 640 px)**: only **`Today` = total PnL** (realized + unrealized, single chip) + PAPER badge + avatar dropdown (which now contains theme toggle + sign-out). Version chip + theme button + notification bell + 5 other KPIs all hidden.
  - **Tablet (≥ 640 px)**: adds `Total today` (full label) + theme toggle + 🔔 + version chip.
  - **Desktop (≥ 768 px)**: adds `Realized today` and `Open · unrealized`.
  - **Large (≥ 1024 px)**: adds `Next poll` countdown and `LLM $`.
- HITL chip still appears on all sizes when there's an actionable pending HITL (it's action-required, not size-dependent).
- **What to test:** open the dashboard on a phone — header should fit on one line, ☰ menu reachable left, single `Today` chip + PAPER + avatar on the right. Rotate to landscape or open on tablet — the strip progressively expands.

## 2.24.100 — 2026-05-26
- **Re-submit chain is now visible from both sides.** Previously: clone-and-edit created a new `manual_override` proposal row linking back to the original via `extras.source_proposal_ulid`, but the original `dropped`/`rejected` row had no idea its rescued version existed. So on the proposals page it looked abandoned.
  - Now: when you Re-submit a row, we also append the new ULID to the parent's `extras.children` list. So:
    - The **child** row (the manual one) shows `↩ This row is a manual re-submit of <parent>` with a clickable link.
    - The **parent** row (the rejected/dropped one) shows `↪ This row was re-submitted N time(s)` listing each child with its state.
  - Clicking either link opens the linked row in the same page (the URL hash auto-expands the target detail row and scrolls to it).
- Original rationale for keeping them as separate rows (not state-updating the rejected one): **audit integrity** — the original was a system decision, the re-submit is a user decision with possibly-edited values. Squashing them would lose history, conflict with `_load_proposal()` ULID lookups, and break multi-resubmit flows. Now both views are linked, so nothing's hidden.

## 2.24.099 — 2026-05-26
- **Graph node weight is no longer just "edge count".** Per-node weight is now a composite:
  ```
  weight = Σ edges × (0.2 + 0.8 · event.sentiment_magnitude) × 0.5^(age_days / 7)
  ```
  Three factors blended **per edge**, summed per node:
  1. **Connection** — base 1.0 per edge.
  2. **Sentiment** — multiplied by `0.2 + 0.8 · sentiment_magnitude` of the event end of the edge. A connection to a dull event ≈ 0.2; to an "explosive" event ≈ 1.0. Baseline 0.2 keeps even dull events counting a bit.
  3. **Recency** — exponential decay with a **7-day half-life**. Yesterday's event ≈ 0.9; week-old ≈ 0.5; month-old ≈ 0.12; year-old ≈ ~10⁻¹⁶ (effectively zero).
- **Translation:** *one explosive event yesterday* outscores *five dull events from a month ago* — which matches your intuition for "is this country/person/company currently impactful".
- **Drives:** node size, layout repulsion, the "min weight" slider (was "min connections"; now 0–10 step 0.5), and the heaviest-entity chip ranking. Chip tooltips show `weight X.X (N edges)` so you can sanity-check.
- **`sentiment_magnitude` now exposed in `/graph/data`** per event node so the client can do this math without a second fetch.
- **What to test:** open `/graph/` → the chip strip should reorder vs before — countries with heavy *recent* coverage rise; ones with stale connections fall. Hover any chip → tooltip shows both `weight` and raw edge count. Min-weight slider takes fractional values now.

## 2.24.098 — 2026-05-26
- **🐛 Re-submit (clone-and-edit) actually executes now.** Two bugs were preventing the manual-override path from reaching the trade ledger:
  1. The bypass-risk branch synthesized a `RiskDecision` with `final_qty=Decimal("0")` — so even when it reached the executor, paper-mode rows landed with quantity 0 and live-mode orders were rejected by the venue for min-qty.
  2. The bypass branch only published to `approved_trades:stream` — but the executor's `_load_proposal()` does an XRANGE on `trading_proposals:stream` to look up symbol/side. With nothing there, `proposal_not_found` log + silent exit. **Nothing ever executed.**
  - Fix: bypass branch now (a) fetches the current ticker via the venue router to compute a real qty from a $100 default notional, (b) publishes to `trading_proposals:stream` first so the executor can find the proposal. Trades land on `/trades/` within ~5 seconds of clicking Re-submit.
- **Bypass-risk is now ON by default** in the clone-edit form, with explanation: re-submitting a rejected proposal *without* bypass almost always gets rejected again (the original triggering event already produced a trade or is on cooldown). Inline copy explains both paths.
- The manual_override state row now stores `qty`, `notional_usd`, and `price_at_manual` in extras for the audit trail.
- **What to test:** open `/proposals/` → expand any rejected/dropped row → keep "Bypass risk gates" checked → Re-submit. Within ~5s a new row should appear with state `Manual` then flip to `📈 Executed` with a link to the new trade in `/trades/`.

## 2.24.097 — 2026-05-26
- **Settings → Risk tab redesigned.** Replaced the flat key/value table with **grouped, friendly controls**: sliders for percentages / counts / minutes, toggles for booleans, currency inputs with `$` prefix, inline help text for every field, and dynamic readouts (e.g. `5%`, `15m`). Grouped into 5 sections — Execution mode · Capital & sizing · Portfolio limits · Behavioural guards · Strategist tuning — each with a one-line description. Dangerous fields (kill-switch loss cap) get a red border.
  - Field metadata lives in `plata/dashboard/risk_field_meta.py` — add an entry there to give any new `risk_config` key its own slider/toggle/help.
  - Unknown keys still render as a raw text-input row in an "Advanced / unrecognised keys" section below — nothing's hidden, you can always edit any key.
- **Proposals: the triggering event is now visible at the top of every row's expanded detail.** Shows title, source, category, sentiment + magnitude, timestamp, link to the original URL, and a link to the event's neighbourhood in the graph (`/graph/?focus=<ulid>`). Plus the original event summary itself with the 🌐 translate icon. If the event has expired (events have a 7-day TTL in Redis), an amber notice explains why the content isn't visible anymore.
- **What to test:** open `/settings/?tab=risk` → see grouped cards with sliders + toggles + help. Drag any slider → readout updates instantly; click Save (or flip a toggle, which auto-saves). Open `/proposals/` → expand any row → at the top of the detail you see the actual news headline that triggered the proposal, with sentiment and a clickable source link.

## 2.24.096 — 2026-05-26
- **Proposals page: 🌐 translate icon** added to the strategist reasoning block and every analog summary in the expandable detail. Same `data-translate` pattern as `/trades/<ulid>`.
- **Strategist thresholds are now live-editable from the UI** — moved from hardcoded module constants in `agents/strategist.py` to the `risk_config` Redis hash, alongside every other tunable. Two new keys appear on `/settings/?tab=risk` (and `/risk_config/`):
  - `strategist_sentiment_threshold` (default 0.5) — events below this `sentiment_magnitude` are dropped before the strategist LLM runs. Raise to 0.7 for only-the-big-news; lower to 0.3 for more candidates (costs more LLM $).
  - `strategist_analog_k` (default 8) — number of historical analog events fetched per event. More = more context per LLM call (more tokens) but better-grounded reasoning.
  - Read fresh per event (no agent restart needed). Risk-manager backfills any new keys to existing deploys on its next config reload.
- **Existing thresholds — quick map of where to change them all:** `/settings/?tab=risk` (the Risk Config table) holds **everything**: `guard_min_conviction`, `guard_block_opposing_side`, `guard_symbol_cooldown_min`, `guard_dedup_event_ulid`, `guard_max_per_category_day`, `max_open_positions`, `max_correlated_positions`, `max_gross_exposure_pct`, `max_net_exposure_pct`, `max_daily_loss_pct`, `risk_per_trade_pct`, `auto_approve_threshold_usd`, `paper_trading_mode`, and the two new strategist ones. Changes audit + version in Postgres; no redeploy needed.
- **What to test:** open `/settings/?tab=risk` → see the two new rows at the bottom. Drop `strategist_sentiment_threshold` to 0.3 → the next batch of events will produce more rows on `/proposals/` (mostly `Dropped` with `llm_no_trade` reason, but at least the LLM was consulted).

## 2.24.095 — 2026-05-26
- **Graph: named-entity pin chips (multi-select).** The "top 5 countries" abstract chip is replaced with **real entity names**, derived from the current dataset: heaviest countries (Israel 🇮🇱, USA 🇺🇸, Russia 🇷🇺, Ukraine 🇺🇦, …), heaviest people, heaviest companies, heaviest assets and orgs. Each chip shows the entity's name + a small degree number (e.g. `🇮🇱 Israel 42`). Click any to pin it; click multiple to pin several (OR mode). When any chip is pinned, the canvas renders only the pinned entities + their immediate neighbours. A "clear" button appears when one or more are selected.
- Top-N per type today: 8 countries, 6 people, 6 companies, 4 assets, 3 orgs — picks the heaviest of each by edge count.
- Chips refresh on every dataset reload (so they reflect what's currently heavy, not a stale list).
- **What to test:** open `/graph/` → strip of named chips with flags appears. Click `🇺🇦 Ukraine` and `🇷🇺 Russia` → canvas shows only those two countries + the events/people/orgs directly linked to them. Click again to unpin; "clear" wipes the whole selection.

## 2.24.094 — 2026-05-26
- **Graph: "heaviest-N" focus chips.** New chip row beneath the show/category filters: `🌍 top 5 countries`, `👤 top 5 people`, `🏢 top 5 companies`, `📰 top 5 events`, `🏷️ top 5 / cat` (top 5 events per category), `⭐ top 10 overall`. Picks the highest-degree nodes of that type (plus their immediate neighbours so the seeds aren't isolated) and hides the rest. Click `off` to clear.
- **How "weight" is computed today** (visible inline next to the chips): `weight = edge count` — same metric driving node size + layout repulsion + the min-connections slider. **Nothing else** feeds in yet (no sentiment, no recency, no centrality). Picked degree so the chips agree visually with the sizing.
- **What to test:** open `/graph/` → click `🌍 top 5 countries` → only the 5 most-connected countries + their immediate events/people/entities render. Click `🏷️ top 5 / cat` → 5 most-connected events per category (so 7 categories × 5 = 35 events max) — useful for seeing the dominant narrative in each lane.

## 2.24.093 — 2026-05-26
- **Three ways to restart halted scraper sources.**
  - **▶ on each card** — halted source cards in the Sleeping lane now show a green ▶ instead of ✕. One click resumes that source.
  - **▶ Resume all sources** — page-level button at the top of `/workflow/`. One-click clears every halted source at once.
  - **🤖 Auto-start (the real fix).** The health watchdog now actively **self-heals**: every 60 s, if the system is RUNNING and any source is halted with `halted_by=system` (left over from a past system halt that didn't clean up), it **auto-resumes** them and writes a `ScrapersAutoResumed` WARN to `/errors/` so you see what happened. User-halted sources stay sticky and trigger a separate `AllScrapersHalted` warning only if every source is in that state.
- New endpoints: `POST /workflow/resume/source/<name>`, `POST /workflow/resume/sources/all`.
- **What to test:** open `/workflow/` — every halted source card has a ▶ button. Click any → that one resumes within ~5 s. Or click the page header's **▶ Resume all sources** → all clear at once. Halt the whole system → resume → within 60 s the watchdog auto-clears any source the resume action missed.

## 2.24.092 — 2026-05-26
- **🐛 Root cause of "Next poll: all halted".** When the system is HALTED (kill-switch, daily-loss guard, LLM-budget cap), the scraper writes `status=halted` to every source's Redis hash on every tick. **Resume only flipped `system:state` back to `RUNNING` — it did not clear the per-source halted flags.** So sources stayed halted forever (silently) even though the dashboard banner says RUNNING. Fixed two ways:
  - Sources now carry `halted_by=system|user`. The `/api/resume` endpoint auto-clears any source with `halted_by=system`. User-cancelled sources stay sticky (intentional).
  - **Health watchdog (new):** a 60-second background loop in the dashboard checks for *silently broken core functions* and writes WARN entries to `error_log` (visible on `/errors/`) — *because not every problem is an exception*. Conditions detected today:
    - `AllScrapersHalted`: System is RUNNING but every source is halted → "no new signals can reach the pipeline."
    - `AgentStaleHeartbeat`: critical agent (`enricher`, `strategist`, `executor`, `risk_manager`) hasn't heartbeated for > 180 s while system is RUNNING and not flagged halted.
    - `AgentMissing`: no heartbeat ever recorded for a critical agent — its service may not have booted.
  - Each condition has a 10-min cooldown so the errors page doesn't fill up with the same warning every minute.
- **What to test:** hit Resume → "all halted" chip should flip to a real countdown within ~5 s as scrapers' next ticks see the cleared status. Halt the system again → within ~60 s, `AllScrapersHalted` appears on `/errors/`. Resume → it stays in history but the chip recovers.

## 2.24.091 — 2026-05-26
- **Rejected proposals aren't errors** — reworded the diagnostic banner on `/proposals/`. Dropped the "check logs" link and the alarming "Strategist saw events but persisted zero rows" framing. The banner now reads as a normal pipeline summary: `Strategist: seen 1105 · below threshold 682 · …`. If actual persistence is failing, a small amber sub-line shows the exact database error inline (not "go look in logs") with the failure count.
- **Self-healing `record_drop()` / `record_published()`.** If the proposals table doesn't exist on the first insert (because the strategist's service booted before the table was created in another service), we now call `ensure_aux_tables()` and retry the insert once. Most of the time the user never sees a failure at all.
- **"Next poll" chip is always visible.** Previously hidden when no source had `last_poll_at` yet (fresh deploys, 15-min GDELT cycles). Now renders an informative state:
  - `5m 42s · reddit` — real countdown
  - `due · gdelt` — the cycle should fire imminently
  - `warming up` — sources exist but none have completed a poll yet
  - `all halted` — every source has been manually halted (red)
  - `no sources` — no scrapers registered at all
- **Smart dates everywhere.** Added `.js-smart-date` class hook so any future element with a plain ISO string in its text gets auto-formatted to the relative-or-absolute format the same way `<time data-utc>` does. Plus wrapped the last unwrapped date (`event_doc.ts` on trade detail).
- **What to test:** open `/proposals/` — banner shows a clean summary, no scary red text unless something is actually broken. Topbar `Next poll` chip is visible with one of the four states. After this deploy, the strategist's next event will land as a `dropped` row (visible on the page) thanks to the self-heal — no need to bounce the service.

## 2.24.090 — 2026-05-26
- **Universal smart timestamps.** Every `<time data-utc="ISO">` element across the dashboard now renders relative for today, absolute for older — and silently re-ticks every 30 s so a 3m-ago label becomes 4m-ago without a page reload.
  - same-day: `just now` (< 45s) / `Xm ago` (< 60m) / `Xh ago` (≥ 60m)
  - yesterday: `Yesterday HH:MM`
  - this year: `DD MMM HH:MM`
  - older: `DD MMM YYYY`
  - tooltip on hover always shows the full local datetime.
- **Server-side `time_ago` Jinja filter** updated to match — so the SSR first paint and the client-side post-load text agree (no flicker).
- Stray raw `strftime` calls in `/trades/` and `/trades/<ulid>` (which weren't wrapped) now use the standard `<time data-utc>` envelope, so they participate in the auto-render.
- **What to test:** open `/trades/` — Opened/Closed columns show e.g. `5m ago` / `Yesterday 14:32` / `13 May 2026`. Leave the tab open — labels increment from `5m` to `6m` after a minute. Hover any timestamp → full local datetime in tooltip.

## 2.24.089 — 2026-05-26
- **🐛 Root cause: proposals table existed only in the dashboard service.** Strategist runs in `intelligence_sandbox` (a separate Railway service) and `Reviewer`/`Executor` in `execution_vault` — none of them created the `proposals` or `agent_activity_log` tables. When the dashboard hadn't booted yet (or had failed), every strategist `record_drop()` INSERT silently failed (`relation "proposals" does not exist`), and we were swallowing the exception. **Zero rows persisted, regardless of how many events the strategist processed.** Same root cause as why no `dropped` proposals appeared since v2.24.087.
  - New `plata.core.db.ensure_aux_tables()` called from `_bind_then_run()` in `entrypoints.py` — runs in **every** service's startup (ingestion_hub / intelligence_sandbox / execution_vault), idempotent (`checkfirst=True`).
  - First failure in `record_drop()` / `record_published()` now logs at **ERROR** (loudly, once per process) instead of WARN-then-silent. Subsequent failures go to DEBUG so logs don't fill up.
- **🐛 Header "Next poll" countdown was empty on fresh deploys.** Scrapers only write `last_poll_at` after their first poll completes — on a fresh container that can take up to 15 min (GDELT). Now falls back to `started_at` (set instantly when the source begins polling) so the chip appears immediately.
- **Strategist pipeline diagnostic banner** at the top of `/proposals/`. Even when the table is empty, you see: `processed N · below_threshold X · missing_event Y · no_embedding Z · last HB …`. Yellow banner if `processed=0` (upstream is quiet); red banner if the strategist is halted. **Plus a clickable alert** when `processed > 0` but persisted rows = 0 → pointing at the exact log line to grep for.
- **What to test:** open `/proposals/` — diagnostic banner at top tells you immediately whether the loop is alive. After the deploy lands and the strategist sees its next event, drop rows should start appearing. The header `Next poll` chip should populate within seconds.

## 2.24.088 — 2026-05-26
- **Header KPI refresh cadence picked properly per-value, not arbitrarily.** Audited each KPI's actual change rate vs the cost of fetching:

  | KPI | Real change rate | Cost | Picked |
  |---|---|---|---|
  | Realized today | Only when a trade closes | Postgres `sum` | 60s |
  | Open count + unrealized | Sampler floor = 60s per trade — anything faster gives no new info | PG scan + Redis hgetall | 60s |
  | HITL pending | Rare — but must surface instantly when it changes | Redis SCAN | 60s + SSE push |
  | LLM $ today | Tiny increments many times/min | Redis GET | 60s smooths noise |
  | Next poll | Already a local 1s tick on a server-anchor | Redis SCAN | 60s anchor |

  Net: **6× fewer polls per active tab** (~1,440/day → ~240) without losing real-time feel. The SSE listener for `proposal_pending` / `trade_opened` / `trade_closed` now also invalidates the KPI cache and triggers an immediate refetch, so anything meaningful still surfaces within ~100ms.
- **What to test:** open the dashboard, watch the network tab — `/api/header_stats` calls drop to ~1/min. Open a paper trade in another window → topbar KPI updates within a second via the SSE push, not 60s later.

## 2.24.087 — 2026-05-26
- **Every drop reason is now persisted** — not just `should_trade=false`. New helper `record_drop()` is called for all four early-return paths in the strategist:
  - `📉 below_threshold` — sentiment_magnitude < 0.5
  - `❓ event_missing_in_graph` — event document expired from Redis JSON
  - `🧬 no_embedding` — Voyage didn't return a vector (rate-limited / budget cap)
  - `🤔 llm_no_trade` — strategist LLM said don't trade (with its reasoning + the 8 KNN analogs it considered)
  Each row stores the full context in `extras` (event title, sentiment, category, LLM raw decision) so you can fine-tune thresholds against real data.
- **Proposals page: drop-reason filter strip.** New chip row appears beneath the state filter once any drops exist: `📉 Below magnitude threshold · 145` / `🤔 LLM said don't trade · 22` etc. Click to filter. URL: `?state=dropped&reason=below_threshold`.
- **Symbol watch is a top-level page** at `/positions/` (sidebar "📡 Symbol watch" under Trading). Old `/trades/watch` still works. The new page is **card grid** with a **sparkline** per symbol (ApexCharts area, color = up/down vs first sample), age badge, unrealized PnL.
- **Per-symbol detail page** `/positions/<SYMBOL>` — big 24h price chart (5-min cadence, auto-refreshes every 30s without page reload), open-trades list on that symbol, net long/short, unrealized PnL. Sampler keeps the last 288 history points per symbol in Redis (`symbol:history:<sym>`, 7d TTL).
- **Header KPIs no longer reload on every page navigation.** Cached in `sessionStorage` with a 10s freshness window; navigating from /trades/ to /proposals/ paints the previous values instantly and only fetches `/api/header_stats` in the background if the cache is older than 10s.
- **What to test:** open `/proposals/?state=dropped` → reason chips appear. Open `/positions/` → cards with sparklines. Click a symbol → big chart. Navigate Dashboard → Positions → back → KPIs don't blink (they paint instantly from cache and update silently).

## 2.24.086 — 2026-05-26
- **Header: "Next poll" countdown.** New KPI chip in the topbar showing the soonest scraper poll (e.g. `4m 18s · reddit`). Each tick is computed locally from a server-given remaining-seconds anchor; the full server value is refreshed every 10 s. Clicking it jumps to `/workflow/`. This is the most concrete answer to *"when could a new proposal possibly land?"* — strategist itself is event-driven (no schedule), but a scraper poll is the deterministic upstream trigger.
- **Strategist now persists "dropped" proposals.** When the LLM says `should_trade=false` for an event that passed the magnitude gate, a row is written to `proposals` with state `🚫 Dropped` and the LLM's reasoning. Answers the "we deployed proposal-saving and nothing showed up — make sense?" question: yes, because we were only saving *published* ones. Now you can see the strategist considering events and deciding against them, with the full reasoning visible in the expandable detail.
- **What to test:** open the dashboard → topbar shows `Next poll Xs · <source>` ticking down each second. Once a few enriched events flow through, `/proposals/` should show a mix of `Published` and `Dropped` rows even on quiet trading days. Filter chip `🚫 Dropped` appears once there's at least one.

## 2.24.085 — 2026-05-26
- **Breadcrumbs work for ANY URL now**, not just section roots. On a sub-page like `/trades/<ulid>` you get `Positions › <H1-text-or-fallback>`; on `/trades/watch` you get `Positions › Symbol watch`; on `/agents/strategist` (when those exist) `Agent Health › Agent: strategist`. The leaf label is derived from the page's `<h1>`, falling back to `<title>` (with the ` · Plata` suffix stripped), and finally a per-section synthesized label (e.g. `Trade 01ABCDEF…`).
- The section crumb is now a real link when there's a leaf — clicking "Positions" from a trade detail page takes you back to the list.
- Dashboard tiles now pass `?from=dashboard`; trade rows pass `?from=trades` — those compose into `Dashboard › Positions › Trade …`.
- **What to test:** click any trade from `/trades/` → top of page shows `Positions › Trade 01ABCDEF…`. Land directly on `/trades/<ulid>` (from a Telegram link) → still get the same trail. Go Dashboard → Positions tile → trade → see all three crumbs.

## 2.24.084 — 2026-05-26
- **"Trades" → "Positions"** in the UI (URL stays at `/trades/` for back-compat with bookmarks / Telegram links). Sidebar icon flipped to 💼.
- **Per-symbol watch list — decoupled from trade milestones.** Every distinct symbol that has an open position is now polled every **5 minutes** by the sampler, regardless of how the per-trade cadence behaves (which can be as long as a few hours for week-out milestones). Result lives in `symbol:latest:<symbol>` Redis hash. The topbar `Open · unrealized` KPI prefers this over the per-trade cache, so even slow-cadence trades show fresh PnL.
- **New `/trades/watch` page** — one row per symbol with: venue badge (🪙 bybit / 📈 alpaca), last price, age (gray < 5m, yellow 5–10m, red > 10m), net long/short qty, list of open trade ULIDs at that symbol, summed unrealized PnL. Auto-refreshes every 30 s. Linked from `/trades/`.
- **What to test:** open a trade on any symbol → wait 5 min → `/trades/watch` shows the symbol with a fresh age. Topbar `Open · unrealized` updates within a minute (sampler floor) or 5 min (symbol-watch worst case).

## 2.24.083 — 2026-05-26
- **Agent activity now durable in Postgres.** Previously the Done lane (`/workflow/`) was the only place agent actions lived, and it was a 50-entry Redis ring buffer per agent — chatty agents like `graph_ingestion` would overwrite their tail within an hour. Now every action is mirrored into a new `agent_activity_log` Postgres table.
  - **Redis stays small and ephemeral** (50 entries per agent) — fast for the live Done lane, no memory bloat.
  - **Postgres holds the durable history** with a **30-day TTL**: a background sweeper in the dashboard process deletes rows older than 30 days every 6h.
  - New **`/activity/history`** page with filters: agent, kind (ok/err/warn), free-text search across summaries, limit (100 / 200 / 500 / 1000). Linked from the sidebar as "Activity history" (🗄️) and from the live activity page.
- **Sampler cadence floored at 60 s.** Trades whose longest milestone is > 4 h were being sampled only every 5 min – 6 h, which made the topbar `Open · unrealized` PnL look frozen. Now every open trade gets a fresh price at least every 60 s — fully live from Bybit / Alpaca, no waiting for the trade to close.
- **What to test:** open `/activity/history` after a few agent ticks → durable table with filters. Open any trade detail with a long milestone → actual-price line updates within 60 s now. Topbar `Open · unrealized` ticks up/down at most a minute late.

## 2.24.082 — 2026-05-26
- **Topbar KPI labels clarified + added "Total today".** The previous `PnL today` showed only *realized* PnL from closed trades, so with 3 open trades and nothing closed yet it sat at `$0.00` — looked stuck. Now:
  - `Realized today` — closed trades only, since 00:00 UTC.
  - `Open · unrealized` — open count and live mark-to-market.
  - `Total today` — sum of the two (the number you actually care about glancing at).
- **What to test:** topbar should show three PnL chips. Open a paper trade in profit → `Open · unrealized` ticks up immediately; `Realized today` only moves when something closes; `Total today` reflects both.

## 2.24.081 — 2026-05-26
- **Proposals are now first-class Postgres rows** — every TradeProposal the strategist publishes is mirrored into a new `proposals` table (auto-created on dashboard startup) and its state evolves through the pipeline: `published → rejected | pending_hitl | hitl_(approved/rejected/timeout) | approved → executed | failed_execution | manual_override`. Survives Railway restarts (Redis streams are bounded; this isn't).
- **Proposals page rebuilt** (`/proposals/`). Was: pending-HITL only. Now:
  - **Filter chips** for every state, with live counts (`Published 12 · Rejected 8 · Executed 3 · …`) + `?symbol=NVDA` filter.
  - **One row per proposal**: created-at, symbol, side, conviction, state badge with icon + reason ("`cooldown:BTCUSDT last 12m ago, min 30`", "`sized $1,250 (1% of $125k Bybit equity)`", link to the resulting trade…).
  - **Expandable detail** per row: full strategist reasoning, the 8 KNN analogs with similarity scores + their historical price-impact, predicted milestones, risk snapshot JSON.
  - **Clone &amp; edit form** on every row — pre-filled with the original values; tweak symbol / side / conviction / SL / TP / reasoning and **Re-submit**. Two modes: normal (back through risk_manager) or **bypass-risk** (publishes a sized order directly to the executor, audited as `manual_override`). The new proposal is itself recorded so you can see the chain (`source_proposal_ulid` in extras).
- **Wired into agents:**
  - Strategist: persists on publish.
  - Risk manager: transitions to `rejected` (with reason), `pending_hitl`, `hitl_approved`/`hitl_rejected`, or `approved` (with sized notional + SL/TP in extras).
  - Executor: marks `executed` and links the `trade_ulid` on success.
- Sidebar entry renamed to **Proposals** (was Pending Proposals).
- **What to test:** open `/proposals/` after a few new events have come in. Should see the full lifecycle visible per row. Click "Details ▾" on any row → see analog list + clone-edit form. Try a clone-edit re-submit on a rejected proposal with `bypass_risk` checked → a new row appears with state `Manual` and (within a few seconds) `Executed` linking to a new trade.

## 2.24.080 — 2026-05-26
- **Header redesign — KPIs + account dropdown + breadcrumbs.**
  - **3 live KPIs** in the topbar (polled every 10s from new `/api/header_stats`):
    - `PnL today` — sum of `net_pnl` for trades closed since 00:00 UTC. Green / red / gray.
    - `Open · ±$unrealized` — count of open trades + live unrealized PnL computed from `trade:latest:<ulid>` against entry × qty × side.
    - `HITL n` — only shown when proposals are waiting for human approval (red).
    - `LLM $today / $cap` — only shown when a daily budget is set; turns yellow > 70 %, red > 90 %.
  - **Account dropdown** — Flowbite dropdown anchored to a circular avatar; replaces the previous inline email + Log-out button. Items: Account &amp; preferences, API keys, Controls, Sign out.
  - **Dynamic breadcrumbs** — Flowbite breadcrumb bar above every page. Auto-derives the trail from the active sidebar entry + an optional `?from=<page>` referrer param (e.g. a dashboard tile linking to `/trades/?from=dashboard` renders `Dashboard › Trades`; visiting `/trades/` directly renders just `Trades`). Pages can also override the trail via `window.PLATA_BREADCRUMBS`.
- Sidebar label renamed `Pending Proposals` → `Proposals` (the upcoming page will include dropped + rejected proposals, not just pending HITL).
- **What to test:** look at topbar after deploy — 3 numbers should populate. Click the avatar → menu. Navigate to /trades/ from dashboard via a tile vs from the sidebar — breadcrumbs differ.

## 2.24.079 — 2026-05-26
- **Graph: per-category event icons.** Events were all rendering as a single 📰 newspaper icon regardless of what they were about. Now: ⚔️ war · 🛡️ cyber · 🏦 macro · ⚖️ regulation · 📊 earnings · 🔥 social_virality · 🐋 whale_move · 🌐 geopolitics · 🪙 crypto · 💻 tech · 🏢 company. The legend on the right rail is rewritten to match the actual `EventCategory` enum.
- **What to test:** open `/graph/` — should see a mix of icons instead of a wall of newspapers.

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
