"""3-layer news deduplication.

Layer 1: URL + title hash (Redis SET, TTL 7d).
Layer 2: Entity-set Jaccard (sorted-set fingerprint, TTL 6h).
Layer 3: Embedding cosine (HNSW index over last-24h titles).

Caller passes a `RawSignal`. We return either:
  - (False, None)                — fresh, publish + archive normally
  - (True, master_signal_ulid)   — duplicate, archive with `is_duplicate=True`
                                    and `dedup_master_ulid=master`
"""
from __future__ import annotations

import hashlib
import re
import struct
import time
from typing import Iterable

from redis.commands.search.field import TextField, VectorField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.query import Query

from plata.core.bus import get_redis
from plata.core.embeddings import EMBEDDING_DIM, embed
from plata.core.observability import get_logger
from plata.core.schemas import RawSignal

_log = get_logger("scraper.dedup")

HASH_SET_KEY = "seen_signals:hashes"
HASH_TTL_SEC = 7 * 24 * 3600

ENTITY_FP_PREFIX = "seen_signals:fp:"
ENTITY_FP_TTL_SEC = 6 * 3600
JACCARD_THRESHOLD = 0.85

RECENT_SIGNALS_INDEX = "idx:recent_signals"
RECENT_SIGNAL_PREFIX = "recent_signal:"
RECENT_TTL_SEC = 24 * 3600
COSINE_THRESHOLD = 0.92  # i.e., distance ≤ 0.08 in cosine-distance form

_TICKER_RE = re.compile(r"\$?\b([A-Z]{2,6})\b")


def _hash(*parts: str) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update((p or "").encode("utf-8"))
        h.update(b"\x1f")
    return h.hexdigest()


def _extract_quick_entities(text: str) -> set[str]:
    """Cheap NER-lite: tickers + capitalised proper nouns. Used for fingerprinting only."""
    if not text:
        return set()
    tickers = {f"$:{m.group(1).upper()}" for m in _TICKER_RE.finditer(text)}
    # Simple capitalised words (2+ letters, not at sentence start heuristic skipped)
    caps = {w.lower() for w in re.findall(r"\b[A-Z][a-z]{2,}\b", text)}
    return tickers | {f"w:{w}" for w in caps}


async def ensure_recent_index() -> None:
    redis = get_redis()
    try:
        await redis.ft(RECENT_SIGNALS_INDEX).info()
        return
    except Exception:
        pass
    schema = (
        TextField("title", as_name="title"),
        VectorField(
            "embedding",
            "HNSW",
            {"TYPE": "FLOAT32", "DIM": EMBEDDING_DIM, "DISTANCE_METRIC": "COSINE"},
            as_name="embedding",
        ),
    )
    definition = IndexDefinition(prefix=[RECENT_SIGNAL_PREFIX], index_type=IndexType.HASH)
    await redis.ft(RECENT_SIGNALS_INDEX).create_index(schema, definition=definition)
    _log.info("created_index", name=RECENT_SIGNALS_INDEX)


async def _layer_hash(signal: RawSignal) -> str | None:
    """If hash already seen, returns its master ulid; else stores and returns None."""
    redis = get_redis()
    h = _hash(signal.url or "", signal.title or "")
    field = h
    existing = await redis.hget(HASH_SET_KEY, field)
    if existing:
        return existing
    await redis.hset(HASH_SET_KEY, field, signal.ulid)
    await redis.expire(HASH_SET_KEY, HASH_TTL_SEC)
    return None


async def _layer_entity_overlap(signal: RawSignal) -> str | None:
    """Jaccard against fingerprints stored in last 6h."""
    redis = get_redis()
    new_fp = _extract_quick_entities(f"{signal.title or ''} {signal.body[:500]}")
    if not new_fp:
        return None
    now = int(time.time())
    # Sweep recent fingerprints (we store one HASH per signal w/ entities + ulid).
    # For scale, this scan is bounded — entries expire after 6h.
    cursor = 0
    best_match: tuple[float, str] | None = None
    while True:
        cursor, keys = await redis.scan(cursor=cursor, match=f"{ENTITY_FP_PREFIX}*", count=200)
        for k in keys:
            data = await redis.hgetall(k)
            if not data:
                continue
            other_fp_raw = data.get("fp", "")
            ulid = data.get("ulid")
            ts = int(data.get("ts", "0"))
            if not ulid or now - ts > ENTITY_FP_TTL_SEC:
                continue
            other_fp = set(other_fp_raw.split("|")) if other_fp_raw else set()
            if not other_fp:
                continue
            inter = len(new_fp & other_fp)
            union = len(new_fp | other_fp)
            if union == 0:
                continue
            jac = inter / union
            if jac >= JACCARD_THRESHOLD and (best_match is None or jac > best_match[0]):
                best_match = (jac, ulid)
        if cursor == 0:
            break
    # Store this signal's fingerprint regardless (so future signals can dedup against it).
    key = f"{ENTITY_FP_PREFIX}{signal.ulid}"
    await redis.hset(key, mapping={
        "fp": "|".join(sorted(new_fp)),
        "ulid": signal.ulid,
        "ts": str(now),
    })
    await redis.expire(key, ENTITY_FP_TTL_SEC)
    return best_match[1] if best_match else None


async def _layer_embedding(signal: RawSignal) -> str | None:
    """Cosine search against last-24h signal titles."""
    if not signal.title:
        return None
    redis = get_redis()
    vec = await embed(signal.title, input_type="query")
    vec_bytes = struct.pack(f"{EMBEDDING_DIM}f", *vec)
    try:
        q = (
            Query("(*)=>[KNN 1 @embedding $vec AS score]")
            .sort_by("score")
            .return_fields("ulid", "score")
            .dialect(2)
            .paging(0, 1)
        )
        result = await redis.ft(RECENT_SIGNALS_INDEX).search(q, query_params={"vec": vec_bytes})
        if result.docs:
            doc = result.docs[0]
            score = float(getattr(doc, "score", 1.0))
            # COSINE distance: 0 == identical. threshold 0.92 cosine_similarity == 0.08 distance.
            if score <= (1.0 - COSINE_THRESHOLD):
                return getattr(doc, "ulid", None)
    except Exception:
        # Index may not exist on first run.
        await ensure_recent_index()

    # Store this signal so future ones can dedup against it.
    key = f"{RECENT_SIGNAL_PREFIX}{signal.ulid}"
    await redis.hset(key, mapping={
        "ulid": signal.ulid,
        "title": signal.title,
        "embedding": vec_bytes,
    })
    await redis.expire(key, RECENT_TTL_SEC)
    return None


async def check_duplicate(signal: RawSignal) -> tuple[bool, str | None]:
    """Run all three layers in order. Returns (is_dup, master_ulid)."""
    master = await _layer_hash(signal)
    if master:
        return True, master
    master = await _layer_entity_overlap(signal)
    if master:
        return True, master
    try:
        master = await _layer_embedding(signal)
    except Exception:  # pragma: no cover
        _log.exception("embedding_dedup_failed")
        master = None
    if master:
        return True, master
    return False, None


__all__ = ["check_duplicate", "ensure_recent_index"]
