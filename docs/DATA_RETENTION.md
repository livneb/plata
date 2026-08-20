# Data retention & the Janitor agent

Since v2.24.211 a **janitor** loop (started from the dashboard lifespan in the
`ingestion_hub` container, next to sysop) runs a retention sweep every 6 hours.
Before it existed every storage layer was effectively append-only: Redis
streams were never `XTRIM`'d (heartbeats alone add ~100k entries/day), graph
event nodes — each carrying a 1024-float embedding — were kept forever, and
Postgres tables like `signal_archive` / `error_log` grew without bound. That
is what made the dashboard pages heavier over time and swelled the Redis RDB.

Code: `plata/agents/janitor.py`.

## What gets cleaned, and the defaults

### Redis streams (`XTRIM MAXLEN ~`)

| Target | Default maxlen |
|---|---|
| `agent_heartbeats:stream` | 5,000 (~4 h of 12 agents @ 10 s) |
| `raw_signals:stream`, `enriched_events:stream` | 20,000 |
| proposal / risk / trade / closure / historian streams | 10,000 |
| every `dlq:*` stream | 2,000 |

Consumer groups are unaffected — trimming only drops old entries, and every
consumer in this codebase ACKs immediately after handling.

### Knowledge graph (Redis Stack)

| Target | Default |
|---|---|
| `event:*` nodes **without** measured `price_impact` | deleted after **120 days** (their edges + `edgeidx` set go with them) |
| `event:*` nodes **with** `price_impact` (the historical-analog training set) | deleted after **365 days** |
| `edge:*` whose src event no longer exists (orphans) | deleted on every edge pass |
| `edge:*` `evidence_event_ids` lists (grow one entry per co-mention, forever) | capped at the **50** most recent |
| `entity:*` nodes | **never touched** (bounded universe, EWMA history matters) |
| `lesson:*` nodes | **never touched** (distilled learning, tiny, high-value) |

Deleting an aged event also removes it from the RediSearch HNSW index, which
keeps the strategist's KNN queries fast. At most 5,000 events are deleted per
run so a large backlog drains over several cycles instead of one long pass.

The edge pass additionally maintains `edgeidx:{src}` SETs (one per node,
listing its outgoing edge keys) and sets `graph:edgeidx_ready` after a
complete pass. Readers (`graph.neighbors`, `/graph/data`) then fetch a
node's edges with one `SMEMBERS` instead of a full-keyspace `SCAN` — the
keyspace walks were the main reason heavy dashboard pages took ~10 seconds.

### Postgres

| Table | Default retention |
|---|---|
| `signal_archive` — duplicates (`is_duplicate=true`) | 30 days |
| `signal_archive` — everything | 180 days |
| `error_log` — resolved rows | 30 days |
| `error_log` — everything | 90 days |
| `agent_activity_log` | 30 days (replaces the old dashboard-lifespan sweeper) |
| `sysop_findings` — non-`new` states | 60 days |
| `audit_log` | 365 days |
| `event_price_windows` | 365 days |
| `llm_cost` | 400 days |
| `config_settings` | last 20 versions per key |
| `proposals` — `dropped` rows (one diagnostic row per event the strategist rejects; they dominate the table) | 7 days |
| `proposals` — never became a trade (`trade_ulid IS NULL`: rejected / HITL-rejected / timed out / stale) | 7 days |
| `proposals` — became a real trade (`trade_ulid` set) | **never touched** (the learning set tied to `trade_ledger`) |
| `trade_ledger`, `backtest_*`, `users`, `api_credentials` | **never touched** |

Deletes run in batches of 5,000 rows (max 40 batches per table per run) so no
sweep holds a long lock or bloats a single transaction.

## Tuning

Overrides live in the Redis hash `janitor_config`; every number in
`plata.agents.janitor.DEFAULTS` is a valid key. `0` means "disabled / keep
forever". Non-numeric or negative values are ignored.

```bash
# Example: keep raw signals for 3 years, sweep every 12h
redis-cli HSET janitor_config signal_archive_days 1095 interval_hours 12
```

## Operations

- `POST /controls/janitor/run_now` — queue an immediate sweep (picked up
  within ~30 s).
- `GET /controls/janitor/status` — last-run summary + the effective config.
- Redis: `janitor:last_run` (JSON summary of the latest run),
  `janitor:history` (last 30 run summaries).
- Every run also logs one line to the agent activity feed (`janitor`), e.g.
  `Retention sweep: trimmed 91,500 stream entries, aged out 4,200 graph
  events, deleted 12,000 Postgres rows`.
