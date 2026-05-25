"""Web Push (VAPID) helpers — subscription store + send.

Subscriptions live in Redis under `push:sub:<email>` (a list of JSON subscription objects).
Sending is best-effort; permanently-failed endpoints (410 Gone) are pruned.
"""
from __future__ import annotations

import json
from typing import Any

from plata.config.settings import get_settings
from plata.core.bus import get_redis
from plata.core.observability import get_logger

_log = get_logger("push")


def _key(user: str) -> str:
    return f"push:sub:{user.lower()}"


async def store_subscription(user: str, subscription: dict[str, Any]) -> None:
    redis = get_redis()
    raw = json.dumps(subscription, sort_keys=True)
    # Dedupe — drop any existing entry with the same endpoint first.
    existing = await redis.lrange(_key(user), 0, -1)
    for entry in existing:
        try:
            parsed = json.loads(entry)
            if parsed.get("endpoint") == subscription.get("endpoint"):
                await redis.lrem(_key(user), 0, entry)
        except Exception:  # noqa: BLE001
            pass
    await redis.lpush(_key(user), raw)


async def list_subscriptions(user: str) -> list[dict[str, Any]]:
    redis = get_redis()
    raw = await redis.lrange(_key(user), 0, -1)
    out = []
    for r in raw:
        try:
            out.append(json.loads(r))
        except Exception:  # noqa: BLE001
            pass
    return out


async def remove_subscription_by_endpoint(user: str, endpoint: str) -> None:
    redis = get_redis()
    existing = await redis.lrange(_key(user), 0, -1)
    for entry in existing:
        try:
            if json.loads(entry).get("endpoint") == endpoint:
                await redis.lrem(_key(user), 0, entry)
        except Exception:  # noqa: BLE001
            pass


async def send_to_user(user: str, *, title: str, body: str, url: str = "/", tag: str | None = None) -> int:
    """Send a push notification to every subscription registered for `user`. Returns count sent OK."""
    settings = get_settings()
    if not settings.vapid_private_key or not settings.vapid_public_key:
        _log.warning("vapid_not_configured")
        return 0
    try:
        from pywebpush import WebPushException, webpush
    except Exception:  # noqa: BLE001
        _log.warning("pywebpush_not_installed")
        return 0

    payload = json.dumps({"title": title, "body": body, "url": url, "tag": tag or "plata"})
    claims = {"sub": settings.vapid_subject}
    private_key = settings.vapid_private_key.get_secret_value()
    subs = await list_subscriptions(user)
    sent = 0
    for sub in subs:
        try:
            webpush(
                subscription_info=sub,
                data=payload,
                vapid_private_key=private_key,
                vapid_claims=claims,
            )
            sent += 1
        except Exception as exc:  # noqa: BLE001
            code = getattr(exc, "response", None)
            status = getattr(code, "status_code", None) if code is not None else None
            if status in (404, 410):
                await remove_subscription_by_endpoint(user, sub.get("endpoint", ""))
                _log.info("push_pruned_dead_subscription", user=user, endpoint=sub.get("endpoint", "")[:60])
            else:
                _log.warning("push_send_failed", user=user, error=str(exc)[:160])
    return sent
