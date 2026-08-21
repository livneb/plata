"""LLMExhausted is transient: BaseAgent requeues instead of dead-lettering.

Regression tests for the 2026-08-21 incident where a free-pool-wide
rate-limit caused graph_ingestion to DLQ 191 signals in an hour.
"""
from __future__ import annotations

from typing import Any

import pytest

import plata.agents.base as base_mod
from plata.agents.base import LLM_RETRY_FIELD, LLM_EXHAUSTED_MAX_RETRIES, BaseAgent
from plata.core.bus import StreamMessageRef
from plata.core.llm import LLMExhausted


class _FakeRedis:
    def __init__(self) -> None:
        self.hincrby_calls: list[tuple] = []

    async def hincrby(self, *a: Any, **kw: Any) -> int:
        self.hincrby_calls.append(a)
        return 1

    async def lpush(self, *a: Any) -> int:
        return 1

    async def ltrim(self, *a: Any) -> bool:
        return True


class _Recorder:
    """Records calls to a patched async function."""

    def __init__(self) -> None:
        self.calls: list[dict] = []

    def __call__(self, *args: Any, **kwargs: Any):
        self.calls.append({"args": args, "kwargs": kwargs})

        async def _done() -> str:
            return "ok"

        return _done()


class _ExhaustedAgent(BaseAgent):
    name = "test_llm_agent"
    input_stream = "raw_signals:stream"
    group = "test-grp"

    async def handle(self, payload: dict[str, Any]) -> None:
        raise LLMExhausted("LLM call returned no response after 10 attempts")


def _fake_consume(messages: list[StreamMessageRef]):
    async def _gen(*a: Any, **kw: Any):
        for m in messages:
            yield m

    return _gen


@pytest.fixture()
def patched(monkeypatch):
    fake_redis = _FakeRedis()
    publish_raw = _Recorder()
    to_dlq = _Recorder()
    ack = _Recorder()
    capture = _Recorder()
    monkeypatch.setattr(base_mod, "get_redis", lambda: fake_redis)
    monkeypatch.setattr(base_mod, "publish_raw", publish_raw)
    monkeypatch.setattr(base_mod, "to_dlq", to_dlq)
    monkeypatch.setattr(base_mod, "ack", ack)
    monkeypatch.setattr(base_mod, "LLM_EXHAUSTED_PAUSE_SEC", 0)
    agent = _ExhaustedAgent()
    agent.error_reporter = type("R", (), {"capture": capture})()
    return agent, publish_raw, to_dlq, ack, capture, monkeypatch


async def test_llm_exhausted_requeues_instead_of_dlq(patched):
    agent, publish_raw, to_dlq, ack, capture, monkeypatch = patched
    msg = StreamMessageRef(
        stream="raw_signals:stream", redis_id="1-0",
        payload={"ulid": "X" * 26, "body": "hello", "source": "reddit"},
    )
    monkeypatch.setattr(base_mod, "consume", _fake_consume([msg]))

    await agent._consume_loop()

    assert len(publish_raw.calls) == 1
    requeued = publish_raw.calls[0]["args"][1]
    assert requeued[LLM_RETRY_FIELD] == 1
    assert requeued["ulid"] == "X" * 26
    assert not to_dlq.calls          # NOT dead-lettered
    assert len(ack.calls) == 1       # original id acked (requeued copy replaces it)
    assert len(capture.calls) == 1   # WARN on first requeue only
    assert capture.calls[0]["kwargs"]["severity"] == "WARN"


async def test_llm_exhausted_no_warn_spam_on_later_retries(patched):
    agent, publish_raw, to_dlq, ack, capture, monkeypatch = patched
    msg = StreamMessageRef(
        stream="raw_signals:stream", redis_id="2-0",
        payload={"ulid": "Y" * 26, "body": "hi", LLM_RETRY_FIELD: 3},
    )
    monkeypatch.setattr(base_mod, "consume", _fake_consume([msg]))

    await agent._consume_loop()

    assert publish_raw.calls[0]["args"][1][LLM_RETRY_FIELD] == 4
    assert not capture.calls  # WARN only fires on the first requeue
    assert not to_dlq.calls


async def test_llm_exhausted_dlqs_after_retry_cap(patched):
    agent, publish_raw, to_dlq, ack, capture, monkeypatch = patched
    msg = StreamMessageRef(
        stream="raw_signals:stream", redis_id="3-0",
        payload={"ulid": "Z" * 26, "body": "bye", LLM_RETRY_FIELD: LLM_EXHAUSTED_MAX_RETRIES},
    )
    monkeypatch.setattr(base_mod, "consume", _fake_consume([msg]))

    await agent._consume_loop()

    assert not publish_raw.calls     # retries exhausted → no more requeues
    assert len(to_dlq.calls) == 1    # dead-lettered like any hard failure
    assert capture.calls[0]["kwargs"]["severity"] == "ERROR"


def test_llm_exhausted_is_runtime_error():
    # Existing `except RuntimeError` call sites must keep catching it.
    assert issubclass(LLMExhausted, RuntimeError)
