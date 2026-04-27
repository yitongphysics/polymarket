import time
from datetime import datetime, timezone

import pandas as pd
import pytz


def expiry_to_timestamp_polymarket(expiry: str) -> int:
    # Convert YYMMDD expiry to Polymarket settlement time (ms, America/New_York noon).
    ny_tz = pytz.timezone("America/New_York")
    dt = datetime.strptime("20" + expiry, "%Y%m%d")
    dt = dt.replace(hour=12, minute=0, second=0, microsecond=0)
    dt_ny = ny_tz.localize(dt)
    dt_utc = dt_ny.astimezone(timezone.utc)
    return int(dt_utc.timestamp() * 1000)


def timestamp_to_expiry(timestamp: int) -> str:
    # Convert Unix ms to YYMMDD string in UTC.
    dt = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
    return dt.strftime("%y%m%d")


def get_recent_date_strs(d1: int = 0, d2: int = 3) -> list:
    # List of YYMMDD strings for UTC days d1, d1+1, …, d2-1 relative to now.
    ts = int(time.time() * 1000)
    dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
    strs = []
    for i in range(d1, d2):
        day = dt + pd.Timedelta(days=i)
        strs.append(f"{day.year % 100:02d}{day.month:02d}{day.day:02d}")
    return strs
