"""UI-managed API credentials stored encrypted in Postgres.

Encryption: Fernet (AES-128-CBC + HMAC-SHA256), key derived from
`DASHBOARD_SESSION_SECRET` so no new env var is needed. Rotating the session
secret rotates the encryption key — existing credentials become unreadable and
must be re-entered.

Lookup order for `get(provider)`:
  1. In-memory TTL cache (60 s) — keeps hot-path agents fast.
  2. Postgres `api_credentials` row (decrypted).
  3. Settings env-var fallback so an existing deploy keeps working.

Providers (keyed by short slug):
  openrouter, voyage, bybit_key, bybit_secret, alpaca_key, alpaca_secret,
  telegram, langfuse_public, langfuse_secret.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import time
from typing import Any

from cryptography.fernet import Fernet, InvalidToken

from plata.config.settings import get_settings
from plata.core.observability import get_logger

_log = get_logger("credentials")

_TTL_SEC = 60.0
_cache: dict[str, tuple[float, str | None]] = {}
_cache_lock = asyncio.Lock()


def _fernet() -> Fernet | None:
    s = get_settings()
    if not s.dashboard_session_secret:
        return None
    raw = s.dashboard_session_secret.get_secret_value().encode("utf-8")
    key = base64.urlsafe_b64encode(hashlib.sha256(raw).digest())
    return Fernet(key)


def _env_fallback(provider: str) -> str | None:
    s = get_settings()
    mapping = {
        "openrouter":      s.openrouter_api_key,
        "voyage":          s.voyage_api_key,
        "bybit_key":       s.bybit_api_key,
        "bybit_secret":    s.bybit_api_secret,
        "alpaca_key":      s.alpaca_api_key,
        "alpaca_secret":   s.alpaca_api_secret,
        "telegram":        s.telegram_bot_token,
        "langfuse_public": s.langfuse_public_key,
        "langfuse_secret": s.langfuse_secret_key,
    }
    v = mapping.get(provider)
    if v is None:
        return None
    try:
        return v.get_secret_value()
    except AttributeError:
        return v


async def get(provider: str) -> str | None:
    """Return the cleartext secret for `provider`, or None if unconfigured."""
    now = time.monotonic()
    async with _cache_lock:
        cached = _cache.get(provider)
        if cached and (now - cached[0]) < _TTL_SEC:
            return cached[1]
    value: str | None = None
    try:
        from plata.core.db import session_scope
        from plata.core.db.models import ApiCredential
        from sqlalchemy import select
        async with session_scope() as session:
            row = (await session.execute(
                select(ApiCredential).where(ApiCredential.provider == provider)
            )).scalar_one_or_none()
            if row and row.value_encrypted:
                fernet = _fernet()
                if fernet:
                    try:
                        value = fernet.decrypt(row.value_encrypted.encode("utf-8")).decode("utf-8")
                    except InvalidToken:
                        _log.warning("invalid_fernet_token_for_provider", provider=provider)
    except Exception as exc:  # noqa: BLE001
        # Table may not exist yet on first deploy — fall through to env-var.
        _log.debug("credential_lookup_failed", provider=provider, error=str(exc)[:160])

    if value is None:
        value = _env_fallback(provider)

    async with _cache_lock:
        _cache[provider] = (now, value)
    return value


def get_sync(provider: str) -> str | None:
    """Sync wrapper for code paths that can't await (e.g. ccxt client init).
    Falls back to the in-process cache only — does NOT touch the DB. Use `await get()`
    before instantiating long-lived clients to populate the cache."""
    cached = _cache.get(provider)
    if cached and (time.monotonic() - cached[0]) < _TTL_SEC:
        return cached[1]
    return _env_fallback(provider)


async def set_(provider: str, value: str, *, by: str | None = None,
                metadata: dict | None = None) -> None:
    """Encrypt + upsert a credential."""
    fernet = _fernet()
    if fernet is None:
        raise RuntimeError("DASHBOARD_SESSION_SECRET must be set before storing credentials")
    enc = fernet.encrypt(value.encode("utf-8")).decode("utf-8")
    from plata.core.db import session_scope
    from plata.core.db.models import ApiCredential
    from sqlalchemy.dialects.postgresql import insert
    async with session_scope() as session:
        stmt = insert(ApiCredential).values(
            provider=provider,
            value_encrypted=enc,
            metadata_=metadata or {},
            updated_by=by,
        ).on_conflict_do_update(
            index_elements=["provider"],
            set_={
                "value_encrypted": enc,
                "metadata_": metadata or {},
                "updated_by": by,
            },
        )
        await session.execute(stmt)
    async with _cache_lock:
        _cache[provider] = (time.monotonic(), value)


async def delete(provider: str) -> None:
    from plata.core.db import session_scope
    from plata.core.db.models import ApiCredential
    from sqlalchemy import delete as _delete
    async with session_scope() as session:
        await session.execute(_delete(ApiCredential).where(ApiCredential.provider == provider))
    async with _cache_lock:
        _cache.pop(provider, None)


async def list_all() -> list[dict[str, Any]]:
    """List provider rows (without decrypted secrets — last-4-only)."""
    from plata.core.db import session_scope
    from plata.core.db.models import ApiCredential
    from sqlalchemy import select
    out: list[dict[str, Any]] = []
    try:
        async with session_scope() as session:
            rows = (await session.execute(
                select(ApiCredential).order_by(ApiCredential.provider)
            )).scalars().all()
        fernet = _fernet()
        for r in rows:
            tail = ""
            if fernet:
                try:
                    cleartext = fernet.decrypt(r.value_encrypted.encode("utf-8")).decode("utf-8")
                    tail = cleartext[-4:] if len(cleartext) >= 4 else "****"
                except InvalidToken:
                    tail = "?"
            out.append({
                "provider": r.provider,
                "tail": tail,
                "updated_at": r.updated_at,
                "updated_by": r.updated_by,
                "metadata": r.metadata_ or {},
            })
    except Exception as exc:  # noqa: BLE001
        _log.debug("list_credentials_failed", error=str(exc)[:160])
    return out


async def ensure_table() -> None:
    """Create the api_credentials table if it doesn't exist. Called at app startup
    so we don't require a separate Alembic step for this single addition."""
    from plata.core.db.engine import get_engine
    from plata.core.db.models import ApiCredential
    try:
        engine = get_engine()
        async with engine.begin() as conn:
            await conn.run_sync(lambda c: ApiCredential.__table__.create(c, checkfirst=True))
    except Exception as exc:  # noqa: BLE001
        _log.warning("ensure_table_failed", error=str(exc)[:160])
