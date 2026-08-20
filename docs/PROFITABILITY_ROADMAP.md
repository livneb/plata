# Profitability roadmap

The system runs end-to-end, but "works" and "makes money" are different
problems. This is a prioritized list of what to measure, change, and add to
move the needle — grounded in what the codebase does today. Ordered by
expected impact per unit of effort.

## 1. Measure edge before adding anything (highest priority)

We already store everything needed for attribution — `proposals` (with
conviction, category via the triggering event, state), `trade_ledger`
(realized PnL), `event_price_windows` (what the market actually did after an
event), `llm_cost` — but nothing joins them into an **expectancy report**.

Build one page/report that answers, per *event category × source × horizon
bucket*:

- number of trades, win rate, average win / average loss, expectancy per trade
- expectancy **net of fees** and net of the LLM cost it took to produce
- the same numbers for proposals that were *rejected* (risk/HITL) — did
  rejection add or destroy value?

Until this exists, every other tuning decision is guesswork. Once it exists,
the first action is usually: **stop trading the categories/sources with
negative expectancy** — a config change that costs nothing.

## 2. Stop optimizing for activity instead of edge

The sysop pattern `signal_to_proposal_gap` auto-halves
`min_sentiment_magnitude` whenever signals flow but no proposals appear.
That mechanism optimizes for *trade count*, not for *profit* — quiet markets
are supposed to produce zero proposals. Recommendation:

- remove `lower_sentiment_threshold` from `AUTO_APPLY_SAFE` (keep it as a
  manual, approve-on-/sysop/ action), and
- derive the threshold from data instead: plot conviction / sentiment
  magnitude vs. realized outcome (from `event_price_windows` +
  `trade_ledger`) and pick the cutoff that maximizes expectancy.

## 3. Pay for the decision-critical LLM calls

A large share of changelog entries 2.24.15x–2.24.16x fight free-model failure
modes: hallucinated symbols, invalid JSON, whitespace loops, ignored schemas,
`All free models exhausted`. Those failures don't just cost retries — they
silently drop or distort *trading decisions*.

Split routing by stakes:

- **strategist + reviewer** (decisions): a small paid model with reliable
  structured output. At the current volume this is dollars per day, and one
  avoided bad trade pays for weeks of it. The `$20/day` budget guard already
  exists.
- **graph_ingestion / enrichment / dedup** (labeling): keep free models.

## 4. Source pruning by measured predictive value

The scraper ingests everything and dedup/filters by keywords. We already
compute post-event price moves (`event_price_windows`). Score each source
monthly: of its signals, how many preceded a >1σ move within the horizon it
claims? Sources with no predictive hit-rate are pure cost (LLM enrichment,
embeddings, storage, strategist attention) — disable them. This both cuts
spend and raises the signal-to-noise ratio the strategist sees.

## 5. Latency: news edge decays in minutes

`fetched_at → proposal.created_at → executed_trades` timestamps all exist,
but the pipeline's end-to-end latency isn't tracked. News-momentum edge in
crypto decays within minutes; 5-minute polling plus multi-stage LLM hops may
consume the entire edge. Add a latency panel (p50/p95 per stage). If
signal→execution is >2–3 minutes, prioritize: webhook/websocket sources for
the top-value feeds, and a fast-path for high-conviction events that skips
the re-research loop.

## 6. Cost realism in paper mode

Paper fills at the last sampled price overstate performance: no spread, no
slippage, no funding. Add venue-realistic fee + slippage models (taker fee +
half-spread + size-scaled impact) to the paper executor and the backtester —
otherwise the expectancy report from §1 will be optimistic exactly where the
strategy trades most.

## 7. Risk upgrades that compound

Present: risk-per-trade %, max open positions, daily loss cap. Missing and
cheap to add:

- **volatility-scaled sizing** — size positions by ATR/realized vol from
  `event_price_windows` instead of a flat %.
- **correlation cap** — three long alt positions are one BTC-beta bet;
  cap aggregate exposure to a common factor.
- **per-symbol cooldown** after a stop-out (avoid revenge re-entry on the
  same headline cluster).

## 8. Close the learning loop numerically

The postmortem→lessons→strategist KNN loop is qualitative (text lessons).
Add a quantitative prior: per event-category, maintain rolling expectancy
and hit-rate, and inject those numbers into the strategist prompt ("macro
events: 42% hit rate, -0.3R expectancy over the last 90 days"). Text lessons
tell the model *what happened*; the priors tell it *whether this trade class
deserves capital at all*.

---

Suggested sequence: 1 → 2 → 3 are days of work each and change decisions
immediately; 4–6 are a week-ish each and mostly reuse existing tables;
7–8 build on the data 1 produces.
