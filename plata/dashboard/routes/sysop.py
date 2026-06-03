"""/sysop/ — the sysop agent's findings page.

Lists OPEN findings, lets the user approve/apply auto-fixes, dismiss, or
copy a markdown-formatted evidence dump for pasting into chat.
"""
from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from sqlalchemy import desc, select

from plata.agents import sysop as sysop_agent
from plata.core.db import SysopFinding, session_scope
from plata.dashboard import templates
from plata.dashboard.auth import current_user_email

router = APIRouter(prefix="/sysop", tags=["sysop"])


@router.get("/", response_class=HTMLResponse)
async def index(request: Request, show: str = "open"):
    """show=open|all|history"""
    async with session_scope() as session:
        stmt = select(SysopFinding).order_by(desc(SysopFinding.created_at))
        if show == "open":
            stmt = stmt.where(SysopFinding.state == "new")
        elif show == "history":
            stmt = stmt.where(SysopFinding.state != "new")
        rows = (await session.execute(stmt.limit(200))).scalars().all()
    findings = []
    for r in rows:
        findings.append({
            "id": r.id,
            "created_at": r.created_at,
            "updated_at": r.updated_at,
            "pattern": r.pattern,
            "severity": r.severity,
            "title": r.title,
            "evidence": r.evidence or {},
            "proposed_fix": r.proposed_fix,
            "fix_action": r.fix_action,
            "fix_action_args": r.fix_action_args or {},
            "state": r.state,
            "actor": r.actor,
            "clipboard": sysop_agent.format_for_clipboard(r),
        })
    return templates.TemplateResponse(
        request, "pages/sysop.html",
        {"active": "sysop", "findings": findings, "show": show},
    )


@router.post("/{finding_id}/apply")
async def apply(finding_id: int, request: Request):
    actor = current_user_email(request) or "anonymous"
    try:
        result = await sysop_agent.apply_fix(finding_id, actor)
        return JSONResponse({"ok": True, "result": result})
    except Exception as exc:  # noqa: BLE001
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)


@router.post("/{finding_id}/dismiss")
async def dismiss(finding_id: int, request: Request):
    actor = current_user_email(request) or "anonymous"
    await sysop_agent.mark_state(finding_id, "dismissed", actor)
    return RedirectResponse(url="/sysop/", status_code=303)


@router.post("/{finding_id}/resolve")
async def resolve_manual(finding_id: int, request: Request):
    actor = current_user_email(request) or "anonymous"
    await sysop_agent.mark_state(finding_id, "manual_fixed", actor)
    return RedirectResponse(url="/sysop/", status_code=303)
