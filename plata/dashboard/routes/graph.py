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
async def graph_data(
    limit: int = Query(40, ge=1, le=2000),
    focus: str | None = None,
    since: int | None = Query(None, description="Only return events with ts_epoch >= since (unix s)."),
) -> JSONResponse:
    """Cytoscape-compatible nodes + edges.

    `since` enables a delta fetch — only events newer than the given epoch are returned,
    along with their entities + edges. The frontend can merge with its localStorage cache.

    Performance: edges are scanned **once globally** instead of once-per-event, then
    filtered in Python by the event-key set we picked. This collapses the cost from
    O(events × all-edges) to O(all-edges).
    """
    redis = get_redis()
    server_ts_epoch = int(__import__("time").time())

    event_keys: list[str] = []
    if focus:
        event_keys = [event_key(focus)]
    else:
        scanned = []
        async for k in redis.scan_iter(match="event:*", count=500):
            scanned.append(k)
            if not since and len(scanned) >= limit * 4:
                break
        if scanned:
            pipe = redis.pipeline()
            for k in scanned:
                pipe.json().get(k, "$.ts_epoch")
            ts_results = await pipe.execute()
            paired = []
            for k, ts in zip(scanned, ts_results, strict=True):
                ts_val = (ts[0] if isinstance(ts, list) and ts else ts) or 0
                try:
                    ts_int = int(ts_val or 0)
                except (TypeError, ValueError):
                    ts_int = 0
                if since is not None and ts_int < int(since):
                    continue
                paired.append((k, ts_int))
            paired.sort(key=lambda kv: kv[1], reverse=True)
            event_keys = [k for k, _ in paired[:limit]] if since is None else [k for k, _ in paired]

    nodes: dict[str, dict[str, Any]] = {}
    edges: list[dict[str, Any]] = []

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
                    "ts_epoch": int(doc.get("ts_epoch") or 0),
                    "color": color,
                },
            }

    if event_keys:
        # ONE global edge scan, filter by source-in-set. Was: N scans (one per event).
        wanted = set(event_keys)
        all_edge_keys: list[str] = []
        async for k in redis.scan_iter(match="edge:*", count=1000):
            # Edge keys look like edge:<src>:<rel>:<dst>; we want any where <src> is one of ours.
            parts = k.split(":", 3)  # ['edge', '<srctype>', '<srcid>', '<rel>:<dst>'] OR for event:<ulid> as src: ['edge', 'event', '<ulid>', '<rel>:<dst>']
            if len(parts) < 4:
                continue
            src_candidate = parts[1] + ":" + parts[2]
            if src_candidate in wanted:
                all_edge_keys.append(k)
        if all_edge_keys:
            pipe = redis.pipeline()
            for k in all_edge_keys:
                pipe.json().get(k)
            docs = await pipe.execute()
            for ek_key, edoc in zip(all_edge_keys, docs, strict=True):
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
                if dst not in nodes and dst.startswith("entity:"):
                    parts2 = dst.split(":", 2)
                    type_ = parts2[1] if len(parts2) > 1 else "default"
                    name = parts2[2] if len(parts2) > 2 else dst
                    nodes[dst] = {
                        "data": {
                            "id": dst,
                            "label": name[:40],
                            "kind": "entity",
                            "etype": type_,
                            "color": ENTITY_COLOR.get(type_, ENTITY_COLOR["default"]),
                        }
                    }

    return JSONResponse({
        "nodes": list(nodes.values()),
        "edges": edges,
        "server_ts_epoch": server_ts_epoch,
        "delta": since is not None,
    })


@router.get("/node")
async def node_detail(key: str) -> JSONResponse:
    """Return the raw JSON doc for a node (without the embedding vector)."""
    redis = get_redis()
    doc = await redis.json().get(key)
    if doc and isinstance(doc, dict):
        doc.pop("embedding", None)
    return JSONResponse(doc or {})


@router.post("/normalize_aliases")
async def normalize_aliases(dry_run: bool = True) -> JSONResponse:
    """Merge duplicate entity nodes that are known aliases of each other.

    Walks every `entity:country:<id>` key, computes the canonical id, and if the
    canonical differs:
      - merges sentiment_ewma (EWMA-weighted average toward canonical),
      - unions the aliases list,
      - rewrites every edge that points at the alias to point at canonical instead,
      - deletes the alias node.

    Call with `?dry_run=false` to actually apply. Default is a preview.
    """
    from plata.core.entity_aliases import canonicalize_entity
    redis = get_redis()

    merges: list[dict] = []
    async for key in redis.scan_iter(match="entity:*", count=200):
        parts = key.split(":", 2)
        if len(parts) < 3:
            continue
        _, type_, id_ = parts
        doc = await redis.json().get(key)
        if not isinstance(doc, dict):
            continue
        canon_id, canon_name = canonicalize_entity(type_, id_, doc.get("name") or id_)
        if canon_id == id_:
            continue
        canon_key = f"entity:{type_}:{canon_id}"
        merges.append({
            "from": key,
            "to": canon_key,
            "alias_id": id_,
            "canonical_id": canon_id,
        })

    if dry_run or not merges:
        return JSONResponse({"dry_run": True, "would_merge": merges, "count": len(merges)})

    # Apply
    applied = 0
    for m in merges:
        src_key = m["from"]
        dst_key = m["to"]
        src_doc = await redis.json().get(src_key)
        dst_doc = await redis.json().get(dst_key) or {}
        if not isinstance(src_doc, dict):
            continue

        # 1. Merge aliases + sentiment_ewma (mean of both, weighted equally for simplicity).
        merged_aliases = list({*(dst_doc.get("aliases") or []), *(src_doc.get("aliases") or []),
                               src_doc.get("name") or "", src_doc.get("id") or ""})
        merged_aliases = [a for a in merged_aliases if a]
        s_src = float(src_doc.get("sentiment_ewma") or 0)
        s_dst = float(dst_doc.get("sentiment_ewma") or 0)
        merged = {
            **(dst_doc or src_doc),
            "id": m["canonical_id"],
            "name": m["canonical_id"],
            "type": src_doc.get("type") or dst_doc.get("type"),
            "aliases": merged_aliases,
            "sentiment_ewma": (s_src + s_dst) / 2 if (dst_doc and src_doc) else (s_src or s_dst),
            "embedding": dst_doc.get("embedding") or src_doc.get("embedding"),
        }
        await redis.json().set(dst_key, "$", merged)

        # 2. Rewrite every edge that references the alias.
        async for ek in redis.scan_iter(match="edge:*", count=500):
            edoc = await redis.json().get(ek)
            if not isinstance(edoc, dict):
                continue
            changed = False
            if edoc.get("dst") == src_key:
                edoc["dst"] = dst_key; changed = True
            if edoc.get("src") == src_key:
                edoc["src"] = dst_key; changed = True
            if not changed:
                continue
            # Move the edge to the new key path (edge:<src>:<rel>:<dst>) so the scan pattern stays consistent.
            new_ek = f"edge:{edoc['src']}:{edoc.get('rel') or 'mentions'}:{edoc['dst']}"
            await redis.json().set(new_ek, "$", edoc)
            if new_ek != ek:
                await redis.delete(ek)

        # 3. Drop the alias node.
        await redis.delete(src_key)
        applied += 1

    return JSONResponse({"dry_run": False, "merged": merges, "count": applied})
