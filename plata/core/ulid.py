"""ULIDs for cross-stream correlation. Lexicographically sortable, time-ordered."""
from __future__ import annotations

from ulid import ULID


def new_ulid() -> str:
    """Return a fresh ULID as a 26-char string."""
    return str(ULID())
