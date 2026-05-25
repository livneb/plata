"""Knowledge graph stored in Redis Stack.

Implementation notes:
  - Nodes (entity:*, event:*) are JSON documents stored via RedisJSON.
  - Edges (edge:*) are JSON documents keyed by `edge:{src}:{rel}:{dst}`.
  - Vector similarity uses RediSearch HNSW indexes on the `$.embedding` JSONPath.

Indexes (created idempotently by `ensure_indexes`):
  - idx:entity   — embedding vector + filterable type
  - idx:event    — embedding vector + filterable timestamp/source/category

Edges are scanned by `SCAN MATCH edge:{src_id}:*` — fine for our scale (<1M edges/node).
Heavier graph traversals can be added later via Lua scripts.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from redis.commands.search.field import NumericField, TagField, TextField, VectorField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.query import Query

from inkcliq.core.bus import get_redis
from inkcliq.core.embeddings import EMBEDDING_DIM
from inkcliq.core.observability import get_logger

_log = get_logger("graph")

ENTITY_INDEX = "idx:entity"
EVENT_INDEX = "idx:event"


def entity_key(type_: str, id_: str) -> str:
    return f"entity:{type_}:{id_}"


def event_key(ulid: str) -> str:
    return f"event:{ulid}"


def edge_key(src: str, rel: str, dst: str) -> str:
    return f"edge:{src}:{rel}:{dst}"


# ---------------------------------------------------------------------------
# Index creation
# ---------------------------------------------------------------------------

async def ensure_indexes() -> None:
    """Create RediSearch indexes if they don't exist. Idempotent."""
    redis = get_redis()
    await _ensure_entity_index(redis)
    await _ensure_event_index(redis)


async def _ensure_entity_index(redis) -> None:
    try:
        await redis.ft(ENTITY_INDEX).info()
        return
    except Exception:
        pass
    schema = (
        TagField("$.type", as_name="type"),
        TextField("$.name", as_name="name"),
        NumericField("$.sentiment_ewma", as_name="sentiment_ewma"),
        VectorField(
            "$.embedding",
            "HNSW",
            {"TYPE": "FLOAT32", "DIM": EMBEDDING_DIM, "DISTANCE_METRIC": "COSINE"},
            as_name="embedding",
        ),
    )
    definition = IndexDefinition(prefix=["entity:"], index_type=IndexType.JSON)
    await redis.ft(ENTITY_INDEX).create_index(schema, definition=definition)
    _log.info("created_index", name=ENTITY_INDEX)


async def _ensure_event_index(redis) -> None:
    try:
        await redis.ft(EVENT_INDEX).info()
        return
    except Exception:
        pass
    schema = (
        TagField("$.source", as_name="source"),
        TagField("$.category", as_name="category"),
        NumericField("$.ts_epoch", as_name="ts_epoch"),
        TextField("$.summary", as_name="summary"),
        VectorField(
            "$.embedding",
            "HNSW",
            {"TYPE": "FLOAT32", "DIM": EMBEDDING_DIM, "DISTANCE_METRIC": "COSINE"},
            as_name="embedding",
        ),
    )
    definition = IndexDefinition(prefix=["event:"], index_type=IndexType.JSON)
    await redis.ft(EVENT_INDEX).create_index(schema, definition=definition)
    _log.info("created_index", name=EVENT_INDEX)


# ---------------------------------------------------------------------------
# Upserts
# ---------------------------------------------------------------------------

async def upsert_entity(
    *,
    type_: str,
    id_: str,
    name: str,
    embedding: list[float],
    sentiment_delta: float = 0.0,
    aliases: list[str] | None = None,
    extra: dict[str, Any] | None = None,
) -> str:
    """Insert or update an entity node, EWMA-updating sentiment."""
    redis = get_redis()
    key = entity_key(type_, id_)
    existing = await redis.json().get(key)
    if existing:
        # EWMA with alpha=0.3 — newer signal weighted, but past matters.
        prev = float(existing.get("sentiment_ewma", 0.0))
        new_sentiment = 0.7 * prev + 0.3 * sentiment_delta
        doc = {
            **existing,
            "name": name,
            "embedding": embedding,
            "sentiment_ewma": new_sentiment,
            "aliases": list({*(existing.get("aliases") or []), *(aliases or [])}),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        if extra:
            doc.update(extra)
    else:
        doc = {
            "type": type_,
            "id": id_,
            "name": name,
            "embedding": embedding,
            "sentiment_ewma": float(sentiment_delta),
            "aliases": list(aliases or []),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            **(extra or {}),
        }
    await redis.json().set(key, "$", doc)
    return key


async def upsert_event(
    *,
    ulid: str,
    summary: str,
    embedding: list[float],
    source: str,
    category: str,
    ts: datetime,
    entity_refs: list[dict[str, Any]],
    extra: dict[str, Any] | None = None,
) -> str:
    """Insert/replace an event node."""
    redis = get_redis()
    key = event_key(ulid)
    doc = {
        "ulid": ulid,
        "summary": summary,
        "embedding": embedding,
        "source": source,
        "category": category,
        "ts": ts.isoformat(),
        "ts_epoch": int(ts.timestamp()),
        "entity_refs": entity_refs,
        **(extra or {}),
    }
    await redis.json().set(key, "$", doc)
    return key


async def upsert_edge(
    *, src: str, rel: str, dst: str, weight: float = 1.0,
    evidence_event_ids: list[str] | None = None,
    extra: dict[str, Any] | None = None,
) -> str:
    redis = get_redis()
    key = edge_key(src, rel, dst)
    existing = await redis.json().get(key) or {}
    doc = {
        "src": src,
        "rel": rel,
        "dst": dst,
        "weight": float(existing.get("weight", 0.0)) + weight,
        "last_seen": datetime.now(timezone.utc).isoformat(),
        "evidence_event_ids": list({
            *(existing.get("evidence_event_ids") or []),
            *(evidence_event_ids or []),
        }),
        **(extra or {}),
    }
    await redis.json().set(key, "$", doc)
    return key


async def attach_price_impact(
    event_ulid: str, symbol: str, metrics: dict[str, float]
) -> None:
    """Attach oracle-computed price impact metrics to an event node (hot-path inline)."""
    redis = get_redis()
    key = event_key(event_ulid)
    path = f"$.price_impact.{symbol}"
    # JSON.SET requires parent path to exist; ensure price_impact dict exists
    await redis.json().set(key, "$.price_impact", {}, nx=True)
    await redis.json().set(key, path, metrics)


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------

async def vector_search_events(
    embedding: list[float],
    *,
    k: int = 20,
    filter_expr: str | None = None,
) -> list[dict[str, Any]]:
    """KNN search over event embeddings. Returns list of {ulid, score, summary, ...}."""
    redis = get_redis()
    base = filter_expr or "*"
    query_str = f"({base})=>[KNN {k} @embedding $vec AS score]"
    q = (
        Query(query_str)
        .sort_by("score")
        .return_fields("ulid", "summary", "source", "category", "ts", "ts_epoch", "score")
        .dialect(2)
        .paging(0, k)
    )
    import struct
    vec_bytes = struct.pack(f"{EMBEDDING_DIM}f", *embedding)
    result = await redis.ft(EVENT_INDEX).search(q, query_params={"vec": vec_bytes})
    out: list[dict[str, Any]] = []
    for doc in result.docs:
        out.append({
            "key": doc.id,
            "ulid": getattr(doc, "ulid", None),
            "summary": getattr(doc, "summary", None),
            "source": getattr(doc, "source", None),
            "category": getattr(doc, "category", None),
            "ts": getattr(doc, "ts", None),
            "score": float(getattr(doc, "score", 0.0)),
        })
    return out


async def neighbors(node_key: str, *, rel: str | None = None) -> list[dict[str, Any]]:
    """Return outgoing edges of a node, optionally filtered by relation."""
    redis = get_redis()
    pattern = f"edge:{node_key}:{rel or '*'}:*"
    keys = []
    async for k in redis.scan_iter(match=pattern, count=200):
        keys.append(k)
    if not keys:
        return []
    pipe = redis.pipeline()
    for k in keys:
        pipe.json().get(k)
    docs = await pipe.execute()
    return [d for d in docs if d]


async def get_event(ulid: str) -> dict[str, Any] | None:
    redis = get_redis()
    return await redis.json().get(event_key(ulid))


async def get_entity(type_: str, id_: str) -> dict[str, Any] | None:
    redis = get_redis()
    return await redis.json().get(entity_key(type_, id_))
