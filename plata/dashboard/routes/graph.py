"""Knowledge-graph visualization page.

Renders events, the entities they mention, and edges between them as an interactive
force-directed graph in the browser (Cytoscape.js). Data comes from Redis:
  - event:<ulid>         JSON docs
  - entity:<type>:<id>   JSON docs
  - edge:<src>:<rel>:<dst>
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse

from plata.core.bus import get_redis
from plata.core.graph import event_key, get_event
from plata.dashboard import templates

router = APIRouter(prefix="/graph", tags=["graph"])


CATEGORY_COLOR = {
    "macro": "#a78bfa",
    "regulatory": "#f59e0b",
    "company": "#34d399",
    "geopolitics": "#fb7185",
    "crypto": "#60a5fa",
    "tech": "#22d3ee",
    "default": "#94a3b8",
}

ENTITY_COLOR = {
    "person": "#fb923c",
    "company": "#10b981",
    "country": "#3b82f6",
    "asset": "#eab308",
    "org": "#a78bfa",
    "default": "#9ca3af",
}


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse(
        request, "pages/graph.html", {"active": "graph"}
    )


@router.get("/data")
async def graph_data(limit: int = Query(40, ge=1, le=200), focus: str | None = None) -> JSONResponse:
    """Return Cytoscape-compatible nodes + edges.

    Strategy:
      - If `focus` is an event ULID: return that event + all its entity neighbors +
        each connected event one hop away (events sharing an entity).
      - Otherwise: return up to `limit` most recent events + their entities + edges.
    """
    redis = get_redis()

    event_keys: list[str] = []
    if focus:
        event_keys = [event_key(focus)]
    else:
        # Most recent events: scan and sort by ts_epoch (best-effort; bounded by limit*4).
        scanned = []
        async for k in redis.scan_iter(match="event:*", count=500):
            scanned.append(k)
            if len(scanned) >= limit * 4:
                break
        if scanned:
            pipe = redis.pipeline()
            for k in scanned:
                pipe.json().get(k, "$.ts_epoch")
            ts_results = await pipe.execute()
            paired = []
            for k, ts in zip(scanned, ts_results, strict=True):
                ts_val = (ts[0] if isinstance(ts, list) and ts else ts) or 0
                paired.append((k, int(ts_val or 0)))
            paired.sort(key=lambda kv: kv[1], reverse=True)
            event_keys = [k for k, _ in paired[:limit]]

    nodes: dict[str, dict[str, Any]] = {}
    edges: list[dict[str, Any]] = []

    # Load events
    if event_keys:
        pipe = redis.pipeline()
        for k in event_keys:
            pipe.json().get(k)
        event_docs = await pipe.execute()
        for k, doc in zip(event_keys, event_docs, strict=True):
            if not doc:
                continue
            category = (doc.get("category") or "default").lower()
            color = CATEGORY_COLOR.get(category, CATEGORY_COLOR["default"])
            nodes[k] = {
                "data": {
                    "id": k,
                    "label": (doc.get("summary") or doc.get("ulid") or k)[:80],
                    "kind": "event",
                    "category": category,
                    "source": doc.get("source"),
                    "ts": doc.get("ts"),
                    "color": color,
                },
            }

    # Scan all edges that touch any of our events
    if event_keys:
        for ek in event_keys:
            pattern = f"edge:{ek}:*"
            edge_keys = []
            async for ek_match in redis.scan_iter(match=pattern, count=200):
                edge_keys.append(ek_match)
            if not edge_keys:
                continue
            pipe = redis.pipeline()
            for k in edge_keys:
                pipe.json().get(k)
            docs = await pipe.execute()
            for ek_key, edoc in zip(edge_keys, docs, strict=True):
                if not edoc:
                    continue
                src = edoc.get("src")
                dst = edoc.get("dst")
                rel = edoc.get("rel") or "mentions"
                if not (src and dst):
                    continue
                edges.append({
                    "data": {
                        "id": ek_key,
                        "source": src,
                        "target": dst,
                        "label": rel,
                        "weight": float(edoc.get("weight") or 1.0),
                    }
                })
                # Make sure the entity endpoint becomes a node too.
                if dst not in nodes and dst.startswith("entity:"):
                    parts = dst.split(":", 2)
                    type_ = parts[1] if len(parts) > 1 else "default"
                    name = parts[2] if len(parts) > 2 else dst
                    nodes[dst] = {
                        "data": {
                            "id": dst,
                            "label": name[:40],
                            "kind": "entity",
                            "etype": type_,
                            "color": ENTITY_COLOR.get(type_, ENTITY_COLOR["default"]),
                        }
                    }

    return JSONResponse({"nodes": list(nodes.values()), "edges": edges})


@router.get("/node")
async def node_detail(key: str) -> JSONResponse:
    """Return the raw JSON doc for a node (without the embedding vector)."""
    redis = get_redis()
    doc = await redis.json().get(key)
    if doc and isinstance(doc, dict):
        doc.pop("embedding", None)
    return JSONResponse(doc or {})
