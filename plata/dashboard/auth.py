"""Dashboard authentication: argon2 password hashing + signed session cookies."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError
from fastapi import HTTPException, Request, status
from fastapi.responses import RedirectResponse
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer
from sqlalchemy import select

from plata.config.settings import get_settings
from plata.core.db import User, session_scope
from plata.core.observability import get_logger

_log = get_logger("dashboard.auth")
_ph = PasswordHasher()

SESSION_COOKIE = "plata_session"
SHORT_TTL = timedelta(hours=4)
REMEMBER_TTL = timedelta(hours=72)

WHITELIST_PREFIXES = ("/login", "/logout", "/health", "/static", "/api/version")


def _serializer() -> URLSafeTimedSerializer:
    settings = get_settings()
    secret = settings.dashboard_session_secret
    if secret is None:
        raise RuntimeError("DASHBOARD_SESSION_SECRET must be set")
    return URLSafeTimedSerializer(secret.get_secret_value(), salt="plata-session")


def hash_password(plain: str) -> str:
    return _ph.hash(plain)


def verify_password(plain: str, hashed: str) -> bool:
    try:
        _ph.verify(hashed, plain)
        return True
    except VerifyMismatchError:
        return False


def issue_session_cookie(response, email: str, remember: bool) -> None:
    ttl = REMEMBER_TTL if remember else SHORT_TTL
    expires_at = datetime.now(timezone.utc) + ttl
    token = _serializer().dumps({"email": email, "exp": expires_at.isoformat()})
    response.set_cookie(
        SESSION_COOKIE,
        token,
        max_age=int(ttl.total_seconds()),
        httponly=True,
        samesite="lax",
        secure=False,  # Railway terminates TLS at proxy; cookie still sent
    )


def clear_session_cookie(response) -> None:
    response.delete_cookie(SESSION_COOKIE)


def current_user_email(request: Request) -> str | None:
    token = request.cookies.get(SESSION_COOKIE)
    if not token:
        return None
    try:
        data = _serializer().loads(token, max_age=int(REMEMBER_TTL.total_seconds()))
    except (BadSignature, SignatureExpired):
        return None
    exp = datetime.fromisoformat(data["exp"])
    if datetime.now(timezone.utc) > exp:
        return None
    return data.get("email")


async def authenticate(email: str, password: str) -> bool:
    async with session_scope() as session:
        row = (await session.execute(select(User).where(User.email == email))).scalar_one_or_none()
    if row is None:
        return False
    return verify_password(password, row.password_hash)


async def ensure_admin_bootstrapped() -> None:
    """If no users exist and DASHBOARD_ADMIN_* env vars are set, create the admin row."""
    settings = get_settings()
    email = settings.dashboard_admin_email
    pw = settings.dashboard_admin_password
    if not email or not pw:
        return
    async with session_scope() as session:
        existing = (
            await session.execute(select(User).where(User.email == email))
        ).scalar_one_or_none()
        if existing is not None:
            return
        session.add(
            User(email=email, password_hash=hash_password(pw.get_secret_value()), role="admin")
        )
    _log.info("admin_user_bootstrapped", email=email)


async def auth_middleware(request: Request, call_next):
    path = request.url.path
    if any(path.startswith(p) for p in WHITELIST_PREFIXES):
        return await call_next(request)
    if current_user_email(request) is None:
        if request.headers.get("hx-request") == "true":
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED)
        return RedirectResponse(url=f"/login?next={path}", status_code=302)
    return await call_next(request)
