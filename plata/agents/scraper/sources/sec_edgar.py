"""SEC EDGAR — corporate filings firehose.

The SEC publishes every 8-K (material event), 10-Q (quarterly), 10-K
(annual), 13D/G (5%+ stake), Form 4 (insider trades), etc. as an RSS
feed updated every ~10 minutes. Free, no auth, just a User-Agent.

8-K hits the wire BEFORE news outlets cover it — the timing advantage
is the whole point of watching this source.

Filter to the form types that move stocks. The full firehose is huge;
we'd burn LLM budget on routine filings nobody cares about.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone

import httpx

from plata.agents.scraper.news_config import (
    DEFAULTS as NEWS_DEFAULTS,
    get_config as get_news_config,
    record_poll_probe,
)
from plata.agents.scraper.sources.base_source import BaseSource
from plata.core.observability import get_logger
from plata.core.schemas import RawSignal, SignalSource

_log = get_logger("scraper.sec_edgar")

# SEC requires a descriptive User-Agent with contact email. They block
# generic UAs. Use the operator's settings.contact_email if available.
USER_AGENT = "plata-research-bot contact@plata.local"

EDGAR_RSS = ("https://www.sec.gov/cgi-bin/browse-edgar"
              "?action=getcurrent&type={form}&company=&dateb=&owner=include"
              "&start=0&count=40&output=atom")


# Patterns ApexCharts couldn't help with — strip the bracketed
# "(Filer)/(Reporting)" suffix EDGAR likes to add, and the "for ..." trailer.
_TITLE_CLEAN = re.compile(r"\s*\([^)]*\)\s*$")


class SecEdgarSource(BaseSource):
    name = "sec_edgar"
    poll_interval_sec = 5 * 60

    def __init__(self) -> None:
        self._seen_ids: set[str] = set()

    async def poll(self) -> list[RawSignal]:
        try:
            cfg = await get_news_config()
        except Exception:  # noqa: BLE001
            cfg = {}
        if not cfg.get("sec_edgar_enabled", True):
            await record_poll_probe("sec_edgar", error_type="Disabled",
                                     error_message="Source is disabled in /news/ config")
            return []
        forms = cfg.get("sec_edgar_forms") or NEWS_DEFAULTS["sec_edgar_forms"]
        signals: list[RawSignal] = []
        probe_kwargs: dict = {"forms": ",".join(forms)}
        last_status: int | None = None
        async with httpx.AsyncClient(
            headers={"User-Agent": USER_AGENT, "Accept": "application/atom+xml"},
            timeout=20.0,
            follow_redirects=True,
        ) as client:
            for form in forms:
                url = EDGAR_RSS.format(form=form)
                try:
                    resp = await client.get(url)
                    last_status = resp.status_code
                    if resp.status_code >= 400:
                        probe_kwargs["error_type"] = f"HTTP{resp.status_code}"
                        probe_kwargs["error_message"] = resp.text[:200]
                        continue
                except Exception as exc:  # noqa: BLE001
                    probe_kwargs["error_type"] = type(exc).__name__
                    probe_kwargs["error_message"] = str(exc)[:200]
                    continue
                try:
                    import feedparser
                except ImportError:  # pragma: no cover
                    probe_kwargs["error_type"] = "ImportError"
                    probe_kwargs["error_message"] = "feedparser not installed"
                    break
                feed = feedparser.parse(resp.text)
                for entry in feed.entries:
                    eid = entry.get("id") or entry.get("link")
                    if not eid or eid in self._seen_ids:
                        continue
                    self._seen_ids.add(eid)
                    title = _TITLE_CLEAN.sub("", entry.get("title") or "").strip()
                    summary = entry.get("summary") or ""
                    link = entry.get("link") or ""
                    pub = None
                    if entry.get("updated_parsed"):
                        pub = datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc)
                    elif entry.get("published_parsed"):
                        pub = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
                    signals.append(RawSignal(
                        source=SignalSource.SEC_EDGAR,
                        url=link,
                        title=f"[{form}] {title}",
                        body=(summary or title)[:4000],
                        source_published_at=pub or datetime.now(timezone.utc),
                        metadata={"form": form, "entry_id": eid},
                    ))
        probe_kwargs["item_count"] = len(signals)
        if last_status is not None:
            probe_kwargs["http_status"] = last_status
        await record_poll_probe("sec_edgar", **probe_kwargs)
        if len(self._seen_ids) > 8000:
            self._seen_ids = set(list(self._seen_ids)[-4000:])
        return signals
