"""Login / logout routes."""
from __future__ import annotations

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from plata.dashboard import BASE_DIR
from plata.dashboard.auth import (
    SESSION_COOKIE,
    authenticate,
    clear_session_cookie,
    issue_session_cookie,
)

router = APIRouter(tags=["auth"])
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


@router.get("/login", response_class=HTMLResponse)
async def login_form(request: Request, next: str = "/", error: str | None = None):
    return templates.TemplateResponse(
        request, "pages/login.html", {"next": next, "error": error}
    )


@router.post("/login")
async def login_submit(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
    remember: str | None = Form(None),
    next: str = Form("/"),
):
    ok = await authenticate(email.strip().lower(), password)
    if not ok:
        return RedirectResponse(
            url=f"/login?error=Invalid+credentials&next={next}", status_code=303
        )
    target = next if next.startswith("/") else "/"
    response = RedirectResponse(url=target, status_code=303)
    issue_session_cookie(response, email.strip().lower(), remember=bool(remember))
    return response


@router.post("/logout")
async def logout():
    response = RedirectResponse(url="/login", status_code=303)
    clear_session_cookie(response)
    return response
