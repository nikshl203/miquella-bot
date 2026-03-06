# time_utils.py
from __future__ import annotations

from datetime import datetime, timezone, timedelta

# We want MSK everywhere.
# On Windows, Python's zoneinfo may require the external 'tzdata' package.
# To keep setup simple and stable, we use a fixed UTC+3 offset.
# Moscow no longer uses DST, so fixed offset is correct for modern dates.
MSK = timezone(timedelta(hours=3))


def now_msk() -> datetime:
    """Current time in MSK (UTC+3)."""
    return datetime.now(MSK)


def msk_day_key(dt: datetime | None = None) -> str:
    """Day key in MSK: YYYY-MM-DD."""
    if dt is None:
        dt = now_msk()
    else:
        # If dt is naive, assume it's UTC and convert to MSK.
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc).astimezone(MSK)
        else:
            dt = dt.astimezone(MSK)
    return dt.strftime("%Y-%m-%d")
