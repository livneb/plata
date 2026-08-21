"""Sticky paid-402 marker: rescue paths skip paid while OpenRouter is out of credits."""
from __future__ import annotations

from typing import Any

import plata.core.error_reporter as er_mod
import plata.core.llm as llm_mod


class _FakeRedis:
    def __init__(self, existing: bool = False) -> None:
        self.existing = existing
        self.setex_calls: list[tuple] = []

    async def exists(self, key: str) -> int:
        return 1 if self.existing else 0

    async def setex(self, *a: Any) -> bool:
        self.setex_calls.append(a)
        return True


async def test_mark_paid_402_sets_sticky_and_flags(monkeypatch):
    fake = _FakeRedis()
    flagged: list[tuple] = []

    async def _fake_flag(provider: str, message: str, **kw: Any) -> None:
        flagged.append((provider, message))

    monkeypatch.setattr(llm_mod, "get_redis", lambda: fake)
    monkeypatch.setattr(er_mod, "flag_api_limit", _fake_flag)

    await llm_mod._mark_paid_402("Error code: 402 - Insufficient credits")

    assert fake.setex_calls and fake.setex_calls[0][0] == llm_mod._PAID_402_KEY
    assert flagged == [("openrouter", "Error code: 402 - Insufficient credits")]


async def test_paid_402_active_reflects_redis(monkeypatch):
    monkeypatch.setattr(llm_mod, "get_redis", lambda: _FakeRedis(existing=True))
    assert await llm_mod._paid_402_active() is True
    monkeypatch.setattr(llm_mod, "get_redis", lambda: _FakeRedis(existing=False))
    assert await llm_mod._paid_402_active() is False


async def test_paid_402_active_survives_redis_outage(monkeypatch):
    def _boom():
        raise ConnectionError("redis down")

    monkeypatch.setattr(llm_mod, "get_redis", _boom)
    assert await llm_mod._paid_402_active() is False  # fail open: allow rescue
