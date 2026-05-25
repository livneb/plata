"""Web Push subscription + test endpoints."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from plata.config.settings import get_settings
from plata.dashboard.auth import current_user_email
from plata.dashboard.push import send_to_user, store_subscription

router = APIRouter(prefix="/api/push", tags=["push"])


@router.get("/vapid_public_key")
async def public_key():
    s = get_settings()
    return {"key": s.vapid_public_key or ""}


@router.post("/subscribe")
async def subscribe(request: Request):
    user = current_user_email(request)
    if not user:
        raise HTTPException(status_code=401, detail="not authenticated")
    payload = await request.json()
    sub = payload.get("subscription") if isinstance(payload, dict) else None
    if not (isinstance(sub, dict) and sub.get("endpoint")):
        raise HTTPException(status_code=400, detail="missing subscription")
    await store_subscription(user, sub)
    return {"ok": True}


@router.post("/test")
async def test_push(request: Request):
    """Fire a test notification to the logged-in user — useful right after enabling."""
    user = current_user_email(request)
    if not user:
        raise HTTPException(status_code=401, detail="not authenticated")
    sent = await send_to_user(
        user,
        title="Plata",
        body="Push notifications are working.",
        url="/",
    )
    return {"ok": True, "sent": sent}
