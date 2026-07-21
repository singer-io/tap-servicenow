"""Shared datetime helpers for ServiceNow query formatting."""

from datetime import timezone

import dateutil.parser


def to_snow_dt(value: str) -> str:
    """Normalize a datetime string to ServiceNow's native UTC format."""
    if not value:
        return value
    try:
        dt = dateutil.parser.parse(value)
        # Treat naive datetimes as UTC.
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return value
