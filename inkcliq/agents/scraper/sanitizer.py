"""Prompt-injection defense for untrusted content.

Layers:
  1. Strip control characters and zero-width spaces.
  2. Truncate to a hard char limit (defense against context-bombing).
  3. Wrap in <untrusted_content> tags so downstream prompts can instruct the LLM
     to treat the contents as data, never instructions.
"""
from __future__ import annotations

import re

_CONTROL = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_ZERO_WIDTH = re.compile(r"[​‌‍﻿]")
_INJECT_HINTS = re.compile(
    r"(?i)(ignore\s+(?:all\s+)?(?:previous|prior|above)\s+(?:instructions|directives))"
    r"|(system\s*prompt|you\s+are\s+now|new\s+instructions:)"
)

MAX_CHARS_DEFAULT = 4000


def sanitize(text: str, *, max_chars: int = MAX_CHARS_DEFAULT) -> str:
    """Return a cleaned version of `text` safe to inline in an LLM prompt."""
    if not text:
        return ""
    text = _CONTROL.sub("", text)
    text = _ZERO_WIDTH.sub("", text)
    text = text.replace("</untrusted_content>", "&lt;/untrusted_content&gt;")
    text = text.replace("<untrusted_content>", "&lt;untrusted_content&gt;")
    if len(text) > max_chars:
        text = text[:max_chars] + "…[truncated]"
    return text


def wrap_untrusted(text: str, *, max_chars: int = MAX_CHARS_DEFAULT) -> str:
    """Sanitize and wrap in tags. Use this when inserting scraped content into prompts."""
    return f"<untrusted_content>\n{sanitize(text, max_chars=max_chars)}\n</untrusted_content>"


def detect_likely_injection(text: str) -> bool:
    """Heuristic flag — text contains language commonly used in injection attempts."""
    if not text:
        return False
    return bool(_INJECT_HINTS.search(text))
