"""Contract for a Scraper source. New sources only need to subclass this."""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import AsyncIterator

from inkcliq.core.schemas import RawSignal


class BaseSource(ABC):
    """One subclass per data feed. Either polls or streams; runner handles both."""

    name: str = "base"
    # Polling interval in seconds; ignored if `is_streaming`.
    poll_interval_sec: int = 60
    is_streaming: bool = False

    @abstractmethod
    async def poll(self) -> list[RawSignal]:
        """One polling tick. Return new signals since last tick."""

    async def stream(self) -> AsyncIterator[RawSignal]:  # pragma: no cover
        """Override for websocket-based sources. Yields signals as they arrive."""
        if False:
            yield  # type: ignore[unreachable]
        raise NotImplementedError(f"{self.name} does not implement streaming")
