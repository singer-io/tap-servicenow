"""Shared datetime helpers for ServiceNow query formatting."""

from datetime import timezone

import dateutil.parser
from singer import get_logger

LOGGER = get_logger()

#: ServiceNow's encoded-query grammar compares datetimes at second precision,
#: so this is the only format we can put in a sysparm_query.
SNOW_DT_FORMAT = "%Y-%m-%d %H:%M:%S"


class InvalidDatetimeError(ValueError):
    """A configured or bookmarked datetime could not be parsed.

    Raised only for values the tap controls (start_date, saved bookmarks).
    Passing an unparseable value straight through produced a query like
    `sys_updated_on>=<garbage>`, which ServiceNow answers with HTTP 200 and
    either zero rows or the condition ignored - silently wrong either way.
    """


def to_snow_dt(value: str, strict: bool = False, context: str = "") -> str:
    """Normalize a datetime string to ServiceNow's native UTC format.

    Args:
        value: the datetime string to normalize; falsy input is returned as-is.
        strict: raise InvalidDatetimeError instead of returning the value
            unchanged when it cannot be parsed. Use for config and bookmark
            values, where an unparseable string silently corrupts the query.
            Leave False for record data, where one malformed row should not
            abort the stream.
        context: short description used in error and log messages.

    Sub-second precision is truncated, because the query grammar cannot express
    it. That is safe for comparison operators but not for the exact-equality
    clause the keyset cursor uses on the `^NQ` branch, so it is logged rather
    than dropped quietly.
    """
    if not value:
        return value
    try:
        dt = dateutil.parser.parse(value)
    except Exception as exc:
        if strict:
            raise InvalidDatetimeError(
                f"Could not parse datetime {value!r}"
                f"{f' for {context}' if context else ''}. ServiceNow would "
                f"accept the resulting query and silently return the wrong "
                f"rows, so this is being rejected instead."
            ) from exc
        LOGGER.warning(
            "Could not parse datetime %r%s; using it unchanged.",
            value, f" for {context}" if context else "",
        )
        return value

    # Treat naive datetimes as UTC.
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)

    if dt.microsecond:
        # The keyset cursor's `^NQ` branch matches the boundary instant with
        # `sys_updated_on={value}`. Truncating here makes that clause miss any
        # row inside the same second, which would drop rows at a page boundary.
        # ServiceNow's Table API returns second precision today, so this is a
        # tripwire for that changing rather than a live defect.
        LOGGER.warning(
            "Datetime %r%s carries sub-second precision, which ServiceNow's "
            "query grammar cannot express. Truncating to seconds; rows sharing "
            "this second may be mishandled at a page boundary.",
            value, f" for {context}" if context else "",
        )

    return dt.strftime(SNOW_DT_FORMAT)
