"""On-demand translate / explain-further endpoint.

Used by the small 🌐 button next to long-form text (strategist reasoning, analog
summaries, event summaries). Caches per (text+lang+audience) hash in Redis for 30 d
so repeat clicks are instant.
"""
from __future__ import annotations

import hashlib

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from plata.core.bus import get_redis
from plata.core.llm import LLMClient
from plata.dashboard.auth import current_user_email

router = APIRouter(prefix="/api/translate", tags=["translate"])


SYSTEM = """You rewrite a passage for a user. Follow the requested LANGUAGE and AUDIENCE strictly.
- LANGUAGE 'he' = translate to Hebrew. Keep symbols/tickers/abbreviations in Latin (BTC, USD).
- LANGUAGE 'en' = English.
- AUDIENCE 'tech' = professional trader.
- AUDIENCE 'kids' = explain like the reader is 8 years old, short sentences, no jargon (translate any jargon you have to use).
Output ONLY the rewritten text. No preamble, no quotes."""


@router.post("/")
@router.post("")
async def translate(request: Request) -> JSONResponse:
    user = current_user_email(request)
    if not user:
        raise HTTPException(status_code=401, detail="not authenticated")
    payload = await request.json()
    lang = (payload.get("lang") or "en").lower()
    aud = (payload.get("aud") or "tech").lower()
    redis = get_redis()

    # Batch mode: { texts: [...] } -> { texts: [...] } in one LLM call.
    raw_texts = payload.get("texts")
    if isinstance(raw_texts, list):
        texts = [(t or "").strip() for t in raw_texts]
        if lang == "en" and aud == "tech":
            return JSONResponse({"texts": texts, "skipped": True})
        # Resolve cache hits first; only call LLM for misses.
        out: list[str | None] = [None] * len(texts)
        misses: list[int] = []
        for i, t in enumerate(texts):
            if not t:
                out[i] = ""
                continue
            h = hashlib.sha256(f"{lang}|{aud}|{t}".encode("utf-8")).hexdigest()[:32]
            cached = await redis.get(f"translate:{h}")
            if cached:
                out[i] = cached
            else:
                misses.append(i)
        if misses:
            sep = "\n<<<---PLATA_SPLIT--->>>\n"
            joined = sep.join(texts[i] for i in misses)
            llm = LLMClient("translator")
            try:
                resp = await llm.complete(
                    messages=[
                        {"role": "system", "content": SYSTEM + "\n\nMULTI: the user message contains multiple passages separated by the literal marker `<<<---PLATA_SPLIT--->>>` on its own line. Translate each passage in order and output them separated by that SAME marker on its own line. Preserve the count exactly."},
                        {"role": "user", "content": f"LANGUAGE: {lang}\nAUDIENCE: {aud}\n\n---\n{joined}"},
                    ],
                    temperature=0.3,
                    max_tokens=min(8000, max(400, len(joined) * 2)),
                )
                blob = (resp.choices[0].message.content or "").strip()
            except Exception as exc:  # noqa: BLE001
                raise HTTPException(status_code=502, detail=f"translate failed: {exc}") from exc
            parts = [p.strip() for p in blob.split("<<<---PLATA_SPLIT--->>>")]
            if len(parts) != len(misses):
                # Fallback: if the LLM dropped markers, return the whole blob as the first miss.
                parts = parts + [""] * (len(misses) - len(parts))
                parts = parts[: len(misses)]
            for idx, part in zip(misses, parts):
                out[idx] = part
                h = hashlib.sha256(f"{lang}|{aud}|{texts[idx]}".encode("utf-8")).hexdigest()[:32]
                await redis.set(f"translate:{h}", part, ex=30 * 24 * 3600)
        return JSONResponse({"texts": out})

    text = (payload.get("text") or "").strip()
    if not text:
        return JSONResponse({"text": ""})
    if lang == "en" and aud == "tech":
        return JSONResponse({"text": text, "cached": False, "skipped": True})

    h = hashlib.sha256(f"{lang}|{aud}|{text}".encode("utf-8")).hexdigest()[:32]
    key = f"translate:{h}"
    redis = get_redis()
    cached = await redis.get(key)
    if cached:
        return JSONResponse({"text": cached, "cached": True})

    llm = LLMClient("translator")
    try:
        resp = await llm.complete(
            messages=[
                {"role": "system", "content": SYSTEM},
                {"role": "user", "content": f"LANGUAGE: {lang}\nAUDIENCE: {aud}\n\n---\n{text}"},
            ],
            temperature=0.3,
            max_tokens=min(1500, max(120, len(text))),
        )
        out = (resp.choices[0].message.content or "").strip()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"translate failed: {exc}") from exc
    await redis.set(key, out, ex=30 * 24 * 3600)
    return JSONResponse({"text": out, "cached": False})
