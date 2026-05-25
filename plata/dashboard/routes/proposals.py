"""HITL proposals — Flowbite Table + approve/reject Modal."""
from __future__ import annotations

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from plata.dashboard.app import BASE_DIR
from plata.hitl.approval_store import list_pending, resolve

router = APIRouter(prefix="/proposals", tags=["proposals"])
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    pending = await list_pending()
    return templates.TemplateResponse(
        "pages/proposals.html",
        {"request": request, "pending": pending, "active": "proposals"},
    )


@router.post("/{proposal_ulid}/decide")
async def decide(proposal_ulid: str, action: str = Form(...)):
    await resolve(
        proposal_ulid, approved=(action == "approve"), actor="dashboard"
    )
    return RedirectResponse(url="/proposals/", status_code=303)
