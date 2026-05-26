"""Voyage AI embedding client with simple in-process caching."""
from __future__ import annotations

import asyncio
import hashlib
from collections import OrderedDict
from functools import lru_cache

import voyageai
from voyageai import AsyncClient

from plata.config.settings import get_settings
from plata.core.observability import get_logger

_log = get_logger("embeddings")

EMBEDDING_MODEL = "voyage-3-large"
EMBEDDING_DIM = 1024


class EmbeddingCache:
    """Tiny LRU keyed by sha256(text) → vector. Bounded to avoid memory blow-up."""

    def __init__(self, capacity: int = 2048) -> None:
        self._cache: OrderedDict[str, list[float]] = OrderedDict()
        self._capacity = capacity

    def get(self, key: str) -> list[float] | None:
        if key not in self._cache:
            return None
        self._cache.move_to_end(key)
        return self._cache[key]

    def put(self, key: str, value: list[float]) -> None:
        self._cache[key] = value
        self._cache.move_to_end(key)
        if len(self._cache) > self._capacity:
            self._cache.popitem(last=False)


_VOYAGE_CLIENT: dict[str, AsyncClient] = {}


def _client() -> AsyncClient:
    settings = get_settings()
    from plata.config import credentials as _creds
    key = _creds.get_sync("voyage") or (
        settings.voyage_api_key.get_secret_value() if settings.voyage_api_key else None
    )
    if not key:
        raise RuntimeError("VOYAGE_API_KEY not configured")
    cached = _VOYAGE_CLIENT.get(key)
    if cached is not None:
        return cached
    voyageai.api_key = key
    cli = AsyncClient()
    _VOYAGE_CLIENT.clear()
    _VOYAGE_CLIENT[key] = cli
    return cli


_cache = EmbeddingCache()


def _hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class EmbeddingRateLimited(Exception):
    """Voyage returned a rate-limit error. Transient — caller should skip, not DLQ."""

    user_message = (
        "Voyage embeddings is rate-limiting us (free-tier: 3 RPM / 10K TPM). "
        "Add a payment method at https://dashboard.voyageai.com/ to unlock standard limits. "
        "The 200M free tokens still apply afterwards."
    )


async def embed(text: str, *, input_type: str = "document") -> list[float]:
    """Return a 1024-d embedding for a single text. Cached by content hash."""
    key = f"{input_type}:{_hash(text)}"
    cached = _cache.get(key)
    if cached is not None:
        return cached

    client = _client()
    # Internal retry with backoff on transient rate-limit; tenacity already retries inside
    # voyageai for some cases, but free-tier 429s often slip through.
    delays = [1, 3, 8]
    for attempt, delay in enumerate([0, *delays]):
        if delay:
            await asyncio.sleep(delay)
        try:
            result = await client.embed(
                texts=[text],
                model=EMBEDDING_MODEL,
                input_type=input_type,
            )
            vector = result.embeddings[0]
            _cache.put(key, vector)
            return vector
        except Exception as exc:  # noqa: BLE001
            name = type(exc).__name__
            msg = str(exc).lower()
            is_rate = name == "RateLimitError" or "rate limit" in msg or "429" in msg
            if is_rate and attempt < len(delays):
                _log.warning("embed_rate_limited_retrying", attempt=attempt + 1, delay=delays[attempt])
                continue
            if is_rate:
                raise EmbeddingRateLimited(EmbeddingRateLimited.user_message) from exc
            raise


async def embed_many(texts: list[str], *, input_type: str = "document") -> list[list[float]]:
    """Batch embed. Skips items already in cache."""
    if not texts:
        return []
    keys = [f"{input_type}:{_hash(t)}" for t in texts]
    results: list[list[float] | None] = [_cache.get(k) for k in keys]
    missing_idx = [i for i, r in enumerate(results) if r is None]
    if missing_idx:
        client = _client()
        missing_texts = [texts[i] for i in missing_idx]
        resp = await client.embed(
            texts=missing_texts, model=EMBEDDING_MODEL, input_type=input_type
        )
        for idx, vec in zip(missing_idx, resp.embeddings, strict=True):
            _cache.put(keys[idx], vec)
            results[idx] = vec
    return [r for r in results if r is not None]  # type: ignore[misc]


async def cosine(a: list[float], b: list[float]) -> float:
    """Cosine similarity between two equal-length vectors."""
    if len(a) != len(b):
        raise ValueError("vector length mismatch")
    dot = sum(x * y for x, y in zip(a, b, strict=True))
    na = sum(x * x for x in a) ** 0.5
    nb = sum(y * y for y in b) ** 0.5
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


# Re-export asyncio-friendly module-level test
__all__ = ["embed", "embed_many", "cosine", "EMBEDDING_MODEL", "EMBEDDING_DIM"]
