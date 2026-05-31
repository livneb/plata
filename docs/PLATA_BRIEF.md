# Plata — System Brief

> **Version:** 2.24.132  •  **Purpose:** A multi-agent autonomous trading system that ingests news/events, reasons about them with LLMs, and trades crypto + equities under risk controls with human-in-the-loop oversight.

This brief is written to be fed into NotebookLM or similar research tooling. It covers what Plata is, how it's wired, every agent's job, the data model, the decision logic, and the known gaps. Read it cold and you should be able to argue with the architecture.

---

## 1. What Plata is, in one paragraph

Plata watches the news (GDELT, Reddit, CryptoPanic, RSS feeds, optionally Telegram channels), classifies each story with an LLM, builds a knowledge graph of entities and events, looks up analogous past events via vector search, and asks a strategist LLM whether to take a trade. A risk-manager agent enforces guards (conviction floors, exposure caps, cooldowns), an executor places orders on Bybit (crypto, via ccxt) or Alpaca (equities), a position-monitor agent watches open trades for SL/TP/drift, and a reviewer agent post-mortems closed trades. Everything is observable on a FastAPI/Jinja/Flowbite dashboard. Approvals can be done from the dashboard or via a Telegram bot.

It runs on Railway as three services so failure of one stage doesn't kill the others.

---

## 2. Deployment topology

Three independent Railway services share a Postgres + Redis backend:

| Service              | Agents running                                                                                                     | Why isolated                                                                                                                  |
| -------------------- | ------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------- |
| **ingestion_hub**    | Scraper (GDELT/Reddit/CryptoPanic/RSS), GraphIngestion, Historian, Telegram bot                                    | I/O-heavy, third-party rate limits, embeds lots of text. If it stalls, trading still functions on the latest enriched events. |
| **intelligence_sandbox** | Strategist, Reviewer, PositionMonitor                                                                          | LLM-heavy, slowest tier. Spend is concentrated here; can be scaled or paused independently.                                   |
| **execution_vault**  | RiskManager, Executor, TradeSampler                                                                                | Smallest, most security-sensitive. Holds the venue API credentials. Fewest dependencies = highest uptime.                     |

Each service also runs its own slice of the **Orchestrator** (heartbeat consumer for that service) and contributes to a single FastAPI dashboard (served by whichever service has it enabled — currently ingestion_hub).

---

## 3. Data plane

### Redis (live state + message bus)
- **Streams** (`raw_signals:stream`, `enriched_events:stream`, `trading_proposals:stream`, `risk_decisions:stream`, `approved_trades:stream`, `executed_trades:stream`, `trade_closures:stream`, `agent_heartbeats:stream`) — every agent stage publishes here.
- **Pub/Sub channels** (`system:halt`, `system:resume`, `config:updated`, `hitl:requested`, `dashboard:events`).
- **Hashes**: `risk_config`, `news_config`, `agent_status:<name>`, `trade:latest:<ulid>`, `symbol:latest:<symbol>`, `position:health:<ulid>`, `cost:daily:<date>:agent:<name>`.
- **RediSearch indexes** on event documents for vector KNN.
- **JSON documents** for graph nodes/edges (entities, events).

### Postgres (audit + history)
- `SignalArchive` (every fetched signal — original body, dedup info)
- `ConfigSetting` (versioned risk_config history, every change audited)
- `TradeLedger` (entry, exit, qty, PnL, reason, raw venue response)
- `AuditLog` (every consequential action: who, when, payload)
- `Approval` (HITL request/response)
- `ErrorLog` (severity, type, traceback, context)

### Why both?
Redis = hot path (microsecond ops, pub/sub, streams). Postgres = cold path (forever audit, joins, replay).

---

## 4. Message flow (one full pass)

```
Source (GDELT / RSS / Telegram / …)
   │
   ▼
[Scraper]  → archives RawSignal → publishes to raw_signals:stream
   │
   ▼
[GraphIngestion]  → LLM extracts {category, sentiment, entities, summary}
                  → embeds (Voyage)
                  → upserts nodes/edges in graph
                  → publishes EnrichedEvent to enriched_events:stream
   │
   ▼
[Strategist]  → KNN-retrieves analogous past events
              → LLM produces TradeProposal {symbol, side, conviction, milestones, sl_pct, tp_pct, reasoning}
              → publishes to trading_proposals:stream
   │
   ▼
[RiskManager]  → runs guards (conviction floor, cooldown, dedup, exposure caps, opposing-side block, …)
               → if blocked → RiskDecision(state=rejected)
               → if HITL needed → Approval row + push to dashboard/telegram
               → if approved → publishes ApprovedTrade to approved_trades:stream
   │
   ▼
[Executor]  → routes by symbol → Bybit (ccxt) or Alpaca
            → places market order
            → records fill in TradeLedger + Redis trade:latest
            → publishes ExecutedTrade
   │
   ▼
[TradeSampler]  (continuous) polls price every 5–60s → updates symbol:latest, trade:samples:<ulid>
   │
   ▼
[PositionMonitor]  every 60s:
   - SL/TP / timeout / per-trade auto-close rules → publishes TradeClosure
   - drift check vs strategist milestones → emits position:health:<ulid>
   - off-track + event-driven re-evaluation → optional Adjustment Proposal (HITL)
   │
   ▼
[Reviewer]  consumes trade_closures:stream
   - LLM verdict (success/failure/mixed)
   - updates per-bucket stats in graph
   - every 25 closures, proposes a guard_* tweak → Tuning page (HITL)
```

---

## 5. The agents, in detail

Each section: **role**, **inputs**, **outputs**, **key files**, **model used** (if LLM-driven), **notes on rigor**.

### 5.1 Scraper (`plata/agents/scraper/`)

- **Role:** Continuously poll configured news sources, archive raw text, dedupe, and publish each unique story onto `raw_signals:stream`. Apply a configurable content filter (allowlist + blocklist + min-title-length) before publishing.
- **Sources** (each editable from `/news/`):
  - **GDELT** — 15-min poll, boolean query string
  - **Reddit** — 1-min poll, configurable subreddit list (PRAW)
  - **CryptoPanic** — free-tier news feed
  - **RSS** — generic source reading a list of `{name, url}` from `news_config.rss_feeds`
  - **Telegram channels** — the HITL Telegram bot acts as an ingestion source for any chat it's been added to (configurable channel-ID allowlist)
- **Filter:** `news_config.require_keywords` (must contain ≥1) + `news_config.block_keywords` (drop if any match) + `min_title_len`. Drop reasons counted in `scraper:filter_drops` Redis hash, visible on `/news/`.
- **Files:** `plata/agents/scraper/runner.py`, `plata/agents/scraper/news_config.py`, `plata/agents/scraper/sources/*.py`, `plata/agents/scraper/sanitizer.py` (prompt-injection screen), `plata/agents/scraper/dedup.py`.
- **Rigor:** OK — dedup + content filter are real. The weak point is that all source-level filtering is keyword-based, not semantic.

### 5.2 GraphIngestion (`plata/agents/graph_ingestion.py`)

- **Role:** Turn a raw signal into a structured EnrichedEvent with extracted entities, embed it, and upsert into the entity-event knowledge graph.
- **LLM call:** `openai/gpt-4o-mini` (configurable via `core/llm.py:AGENT_MODELS`). Structured output to `EXTRACTION_SCHEMA` with fields: `summary`, `category` (enum: `WAR | CYBER | MACRO | REGULATION | EARNINGS | SOCIAL_VIRALITY | WHALE_MOVE | OTHER`), `sentiment` (signed), `sentiment_magnitude` (0–1), `entity_refs` (list of `{type, name}`).
- **Embedding:** Voyage AI (`voyage-3` family) for downstream KNN.
- **Output:** Publishes `EnrichedEvent` to `enriched_events:stream`. Persists graph nodes/edges to Redis JSON. Best-effort backfill of `price_impact` (1h/4h/24h % move) at the entity's primary symbol after the fact.
- **Rigor:** Thin. `sentiment_magnitude` is purely an LLM guess — no calibration vs realized price moves. Entity canonicalization (Israel/IL/USA/US) happens after extraction, so junk gets normalized but bad classifications don't get corrected.

### 5.3 Strategist (`plata/agents/strategist.py`)

- **Role:** Decide whether an enriched event warrants opening a position. Produce a TradeProposal with conviction, suggested SL/TP %, expected milestones, and reasoning.
- **Process:**
  1. Drop events where `sentiment_magnitude < threshold` (default 0.5, live-editable in `risk_config`).
  2. KNN-retrieve top-K (default 8) past events by embedding cosine (RediSearch).
  3. For each analog, fetch `price_impact` dict.
  4. LLM call with: event text, analogs (summary + outcome), hardcoded legal symbol universe (`SPY, QQQ, BTCUSDT, ETHUSDT, NVDA, …`).
  5. LLM returns structured `TradeProposal` per `PROPOSAL_SCHEMA`: `symbol, side, conviction (0–1), suggested_sl_pct, suggested_tp_pct, suggested_notional_usd, milestones[{eta_minutes, expected_pct_move, confidence, rationale}], reasoning, similar_events`.
- **LLM:** `anthropic/claude-sonnet-4` (configurable).
- **Output:** Publishes to `trading_proposals:stream`.
- **Rigor:** Thin. The symbol universe is hand-curated, not data-driven. Conviction is pure LLM output — no Brier calibration. KNN is embedding-only — no regime/vol/time-of-day matching. If `price_impact` backfill failed, analogs are summary-only with no outcome data, and the strategist still produces a confident-sounding proposal.

### 5.4 RiskManager (`plata/agents/risk_manager.py`)

- **Role:** Apply portfolio + per-trade guards. Either reject the proposal, route to HITL, or approve it. Compute final notional, SL price, TP price.
- **Guards** (all in `DEFAULT_RISK_CONFIG`, editable on `/settings/?tab=risk`):

  | Guard                          | Default       | What it does                                                    |
  | ------------------------------ | ------------- | --------------------------------------------------------------- |
  | `min_conviction`               | 0.6           | Reject if proposal.conviction below                              |
  | `min_sentiment_magnitude`      | 0.5           | Reject if event sentiment too weak                               |
  | `cooldown_min`                 | 60            | Min minutes between trades on same symbol                       |
  | `max_open_positions`           | 3             | Cap on concurrently-held positions                              |
  | `max_per_sector`               | 5             | Cap per sector (`crypto`, `equity_tech`, …)                     |
  | `max_gross_exposure_pct`       | 30            | % of account equity                                              |
  | `max_net_exposure_pct`         | 20            | Net (long − short) % of equity                                  |
  | `risk_per_trade_pct`           | 1.0           | % of equity at risk per trade (drives notional sizing)          |
  | `guard_one_per_symbol_side`    | true          | Reject if `(symbol, side)` already held (NEW in v2.24.125)      |
  | `guard_dedup_event_ulid`       | true          | Reject if the same event already produced a trade                |
  | `auto_approve_max_notional`    | $1,000        | Below this → auto-approve; above → HITL                          |
  | `monitor_*`                    | various       | PositionMonitor knobs (drift threshold, off-track, cooldown, …)  |
  | `max_daily_loss_pct`           | 5.0           | **Defined but NOT currently enforced — a known gap.**            |
- **Output:** `RiskDecision` to `risk_decisions:stream` for every proposal (audit trail), plus `ApprovedTrade` to `approved_trades:stream` for the executor when approved.
- **HITL:** `auto_approve_max_notional` controls whether the trade fires or waits for a human click on `/proposals/<ulid>` or the Telegram inline keyboard.
- **Rigor:** OK on the deterministic guards. Big gaps: no correlation cap between open positions (the sector cap is too coarse — `equity_tech` can hold all of SPY+QQQ+NVDA), no daily-drawdown circuit breaker (the key exists but nothing reads it), no volatility-scaled sizing.

### 5.5 Executor (`plata/agents/executor.py`)

- **Role:** Take an approved trade, route to the right venue, place the order, record the fill.
- **Routing:** `venue_for(symbol)` — regex-based. `*USDT → bybit` (ccxt), bare ticker → `alpaca`.
- **Order type:** Market order. SL/TP forwarded to venue when supported (Bybit yes; Alpaca paper does not propagate them — falls back to position-monitor-enforced exit).
- **Paper-mode fallback:** If the venue rejects with a regulatory block (e.g. Bybit retCode 10024 / CloudFront 403 / country block) OR returns a BadSymbol OR the client isn't configured, the executor records a simulated fill (`mode=paper`, `regulatory_fallback=true`) instead of DLQ'ing. Symptom is logged once and the venue is flagged in `venue:blocked:<venue>` for 10 minutes.
- **TradeSampler:** Sibling component (not its own agent) that polls the latest price for every open symbol every 5–60s and stores `symbol:latest:<symbol>`, `trade:latest:<ulid>`, `trade:samples:<ulid>` for UI live PnL + monitor decisions.
- **Output:** Persists TradeLedger row, publishes `ExecutedTrade`.
- **Rigor:** Thin on entry quality. Market orders fire immediately at "whatever the venue gives," with no check that the price hasn't moved away from what the strategist saw. No limit-order option.

### 5.6 PositionMonitor (`plata/agents/position_monitor.py`)

- **Role:** Continuously watch every open position and either auto-close (SL/TP/timeout/user-defined auto-close rules) or surface "this trade is drifting from the predicted milestones" adjustment proposals for HITL.
- **Two loops:**
  1. **Periodic (60s):** For each open trade — evaluate user-set auto-close rules (max loss $, max loss %, trailing peak %, close-after-N-days, rolling-window loss%); enforce SL/TP; check timeout (`monitor_max_hold_min`, default 7d); compute drift vs strategist milestones (`on_track / drifting / off_track`); write `position:health:<ulid>` for the UI.
  2. **Event-driven:** Subscribe to `enriched_events:stream` with a separate consumer group. When a new event matches an open position's symbol AND sentiment_magnitude is strong (default ≥ 0.7), send the open trade + new event to the LLM and ask "hold / scale_up / scale_down / close." Output becomes an `adjustment_suggested` proposal on `/proposals/` (HITL by default; can auto-approve above a conviction threshold).
- **LLM:** `anthropic/claude-haiku-4-5` (cheap, runs frequently).
- **Output:** Publishes `TradeClosure` to `trade_closures:stream` for auto-closes; writes `Proposal` rows with state `adjustment_suggested` for HITL.
- **Rigor:** Strong on the auto-close machinery (SL/TP, rules, timeout). Weak on the drift check — it only fires once a minute, so a flash crash can blow past SL.

### 5.7 Reviewer (`plata/agents/reviewer.py`)

- **Role:** Post-mortem closed trades. Maintain win/loss statistics per `(symbol, category, conviction_bucket)`. Every 25 closures, ask an LLM whether one `guard_*` config tweak would have helped.
- **LLM verdict:** Classifies each closure as `success | failure | mixed` and writes a signed `outcome_weight` edge into the graph.
- **Tuning proposals:** Stored as `AuditLog` rows with `action=proposed_config_tweak`. Surfaced on `/tuning/` (sidebar → Trading → Tuning).
- **Rigor:** **Tissue-thin.** The verdict edges are never read back by the Strategist. Tuning proposals queue forever unless the human applies them. Buckets are too coarse to be statistically meaningful at retail-scale trade counts. There is no calibration of conviction → win rate.

### 5.8 Historian (`plata/agents/historian.py`)

- **Role:** Generate synthetic historical events from a user-provided thesis ("seed Plata with the 2022 LUNA collapse"). Lets you bootstrap the analog-retrieval graph before any live events arrive.
- **LLM:** Sonnet-class. Streams candidate events with timestamps, summaries, categories, and entities.
- **Output:** Each accepted historian event is published into the same enriched pipeline. Visible on `/historian/`.
- **Rigor:** Useful for bootstrap, dangerous for production — historian events are LLM-fabricated, so price-impact backfill is the only thing keeping them grounded.

### 5.9 Orchestrator (`plata/agents/orchestrator.py`)

- **Role:** Heartbeat consumer. Every agent emits to `agent_heartbeats:stream` every N seconds; orchestrator aggregates into `agent_status:<name>` Redis hashes for the dashboard health view at `/agents/`.
- **Halt/Resume:** Subscribes to `system:halt` / `system:resume` channels; sets `system:state = HALTED|RUNNING`. Every other agent checks this on each loop and parks consumption when halted.

### 5.10 TelegramBot (`plata/hitl/telegram_bot.py`)

- **Role:**
  1. HITL channel — pushes inline-keyboard approve/reject prompts for any proposal that crosses the `auto_approve_max_notional` threshold.
  2. CLI for the operator — `/status`, `/halt`, `/resume`, `/paper on|off`, `/positions`, `/joininfo`.
  3. **Ingestion source** (new) — for any chat it's been added to (allowlisted by chat ID in `news_config.telegram_channel_ids`), turns every message into a `RawSignal(source=TELEGRAM)` and feeds it through the same content-filter and ingestion pipeline.

---

## 6. Methodology / decision logic

Plata is a **news-driven discretionary system, mechanized**. The mental model is:

1. **Edge thesis:** News creates short-window mispricings (minutes to hours). If we can classify the news fast, retrieve historical analogs, and decide to trade before the move completes, we capture the move.
2. **Retrieval-augmented strategist:** Don't ask an LLM "is this bullish for BTC?" cold. Ask it after handing it 8 semantically-similar past events and what each one actually did to price. That's the analog-retrieval mechanism.
3. **Guards before LLM trust:** The LLM proposes; the deterministic risk manager disposes. Conviction thresholds, exposure caps, dedup, cooldown — all enforced before the LLM's number reaches the executor.
4. **Continuous oversight:** Position monitor is the second mind. It re-evaluates open trades against new events and against the strategist's own predicted milestones. A drifting trade becomes an `adjustment_suggested` row for the user.
5. **Feedback loop (aspirational):** Reviewer is supposed to close the loop — measure what worked, tighten what didn't. **Currently it writes but doesn't read.** This is the single biggest gap (see §8).

### Where the LLM is and isn't

| Stage                      | LLM?                       | Why                                                                                 |
| -------------------------- | -------------------------- | ----------------------------------------------------------------------------------- |
| Signal classification      | YES (`gpt-4o-mini`)        | Free-text → structured fields. Cheap; rate-limited; structured output.              |
| Embedding for KNN          | NO (Voyage AI)             | Deterministic, calibrated.                                                          |
| Strategist proposal        | YES (`claude-sonnet`)      | Combines news, analogs, milestones into a decision; needs language reasoning.       |
| Risk gates                 | NO                         | Pure math/lookups against `risk_config`.                                            |
| Order routing & execution  | NO                         | Deterministic mapping + ccxt/Alpaca call.                                           |
| Position-monitor drift     | NO (math)                  | Linear interpolation against milestone trajectory.                                  |
| Position-monitor off-track | YES (`claude-haiku-4-5`)   | Decides hold/scale/close given new context.                                         |
| Reviewer verdict           | YES (`claude-sonnet`)      | Reads the closure context, labels outcome.                                          |
| Reviewer tuning proposal   | YES (`claude-sonnet`)      | Suggests a single guard_* change every 25 closures.                                 |
| Translator (UI feature)    | YES (`gpt-4o-mini`)        | On-demand explain/translate of any text block.                                       |

### Cost tracking

Every LLM call is wrapped by `plata/core/llm.py:LLMClient(agent_name)` which records cost into `cost:daily:<YYYY-MM-DD>:agent:<name>` Redis keys using OpenRouter's `extra_body.usage.include=True` to get actual billed cost (not estimates). Visible on `/agents/`.

---

## 7. Operator surfaces

- **Web dashboard** — FastAPI + Jinja2 + Flowbite + Tailwind + htmx + Cytoscape.js + ApexCharts. Pages:
  - `/` Dashboard summary
  - `/workflow/` Kanban-style live state of every agent
  - `/activity/` Live action feed + activity history
  - `/agents/` Health + cost per agent
  - `/history/` Past events
  - `/graph/` Cytoscape view of the entity-event knowledge graph
  - `/historian/` Seed synthetic events
  - `/news/` News pipeline editor (sources, RSS, Telegram, content filters) — NEW in v2.24.132
  - `/proposals/` Pending and historical proposals (filterable, paginated)
  - `/trades/` Open and closed positions (split, live PnL)
  - `/positions/` Symbol watch with per-symbol chart + realized PnL
  - `/tuning/` Reviewer-proposed config tweaks
  - `/errors/` Severity-coded log
  - `/dlq/` Dead-letter queue inspector
  - `/settings/` Controls, Risk Config (sliders), Account, Environment, API keys
- **PWA support** — installable; service-worker push notifications via VAPID for `proposal_pending`, `adjustment_suggested`, `system_state=HALTED`. Bell dropdown in the topbar for non-actionable notifications + activity feed.
- **Telegram** — push prompts + slash commands (see §5.10).

---

## 8. Known gaps (the war-machine to-do list)

This is the brutally-honest list. Without these, Plata is in the 95% of AI bots that lose money in 90 days.

1. **No volatility-scaled sizing.** `risk_per_trade_pct` is a flat 1% regardless of the symbol's realized volatility. Should be `notional = (equity × risk_pct) / (ATR × stop_distance_mult)`.
2. **No daily-drawdown circuit breaker.** `max_daily_loss_pct` is defined but no code path reads it. A 10% bleed-out is currently unblocked.
3. **No correlation guard.** Sector cap is too coarse — three Tech longs are one bet.
4. **No entry confirmation.** Market orders fire immediately at "whatever the venue gives." Should: time-since-event check, price-drift check vs strategist's seen price, optional limit-order with TTL.
5. **SL/TP are LLM guesses, not ATR-derived.** Same SL% on BTC (18% vol) and SPY (10% vol) is a bug.
6. **Conviction is uncalibrated.** "0.7 conviction" should mean 70% win rate empirically; we don't measure that. Need Platt scaling / isotonic regression from reviewer's bucket stats back into the strategist.
7. **Backtest is fake.** `plata/backtest/engine.py` hardcodes BTCUSDT and assumes 1h move = outcome. Should replay the full pipeline against `SignalArchive` with realistic fees + slippage + SL/TP triggers from actual OHLCV.
8. **Reviewer feedback is write-only.** Verdict edges never get read back by the strategist. Tuning proposals queue in the audit log forever.
9. **No regime filter.** A "buy the dip" model in a regime-shift event (Luna May 2022) is catastrophic. Should gate strategies by VIX, term spread, BTC funding rate, or similar regime tells.
10. **Position-monitor frequency.** 60s loop is too slow for flash crashes that move 5–10% in seconds.

---

## 9. Open research questions (for NotebookLM)

- What's the empirical edge half-life for headline-driven news in 2026? (How fast do we need to be?)
- For LLM-derived sentiment scores, what's the best calibration method against realized 1h/4h/24h returns? (Platt vs isotonic vs beta calibration?)
- How do prop shops measure "story novelty" vs "rehash"? (Our dedup is URL+title hashing; semantic dedup might surface more value.)
- Is there a public benchmark for retrieval-augmented trading agents? (Most papers benchmark on closed datasets.)
- For a retail-scale account ($10–100k baseline), what's the realistic fee+slippage budget per trade? (We assume zero today.)
- What's the SOTA for combining a sentiment signal with a technical signal (mean-rev or trend) — additive scoring, gated entry, or trained meta-model?
- For position monitoring, what's the equivalent of an "amber alert" trigger that's faster than 60s polling without going websocket-per-symbol?
- How do practitioners decide which guards to AUTO-APPLY from a reviewer suggestion vs require HITL? (We default to HITL for everything; surely there's a safe auto-apply zone.)

---

## 10. Quick reference — file map

| Concern                | Where to look                                                    |
| ---------------------- | ---------------------------------------------------------------- |
| Streams + pub/sub      | `plata/core/bus.py`                                              |
| Pydantic schemas       | `plata/core/schemas.py`                                          |
| Postgres models        | `plata/core/db.py`                                               |
| LLM client + costs     | `plata/core/llm.py` (`AGENT_MODELS`)                             |
| Settings + env         | `plata/config/settings.py`                                       |
| Stored credentials     | `plata/config/credentials.py`                                    |
| Risk defaults          | `plata/agents/risk_manager.py:DEFAULT_RISK_CONFIG`               |
| Risk field UI meta     | `plata/dashboard/risk_field_meta.py`                             |
| News pipeline config   | `plata/agents/scraper/news_config.py`                            |
| Scraper sources        | `plata/agents/scraper/sources/{gdelt,reddit,cryptopanic,rss}.py` |
| LLM extraction schemas | `plata/agents/graph_ingestion.py:EXTRACTION_SCHEMA`              |
|                        | `plata/agents/strategist.py:PROPOSAL_SCHEMA`                     |
| Service entrypoints    | `plata/entrypoints.py`                                           |
| Dashboard app          | `plata/dashboard/app.py`                                         |
| Telegram bot           | `plata/hitl/telegram_bot.py`                                     |
| Position monitor       | `plata/agents/position_monitor.py`                               |

---

*End of brief. Version 2.24.132, 2026-05-29.*
