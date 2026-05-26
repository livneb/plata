from datetime import datetime, timezone
from pathlib import Path

from fastapi.templating import Jinja2Templates

BASE_DIR = Path(__file__).resolve().parent


_MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
           "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def _time_ago(value):
    """Smart relative-or-absolute timestamp:
      same calendar day → 'Xm ago' / 'Xh ago' / 'just now'
      yesterday         → 'Yesterday HH:MM'
      this year         → 'DD MMM HH:MM'
      older             → 'DD MMM YYYY'
    Pages that just want a raw 'Xm ago' string can use this; matches the
    JS plataFormatRelative() so server and client agree on first paint.
    """
    if not value:
        return "—"
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return value
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    delta = now - value
    secs = int(delta.total_seconds())
    if secs < 0:
        return "just now"
    same_day = value.date() == now.date()
    if same_day:
        if secs < 45:
            return "just now"
        if secs < 60 * 60:
            return f"{secs // 60}m ago"
        return f"{secs // 3600}h ago"
    # Different calendar day → absolute (relative is misleading after midnight)
    local = value  # already tz-aware UTC; users see this server-side anyway
    hhmm = f"{local.hour:02d}:{local.minute:02d}"
    if (now.date() - value.date()).days == 1:
        return f"Yesterday {hhmm}"
    mon = _MONTHS[value.month - 1]
    if value.year == now.year:
        return f"{value.day:02d} {mon} {hhmm}"
    return f"{value.day:02d} {mon} {value.year}"


templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
templates.env.filters["time_ago"] = _time_ago


def _version():
    from plata.config.settings import get_settings
    return get_settings().app_version


templates.env.globals["app_version_global"] = _version
