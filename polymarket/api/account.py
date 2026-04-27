import math

import pandas as pd
import requests
from typing import Iterable, Optional


DATA_API = "https://data-api.polymarket.com"
DATA_API_MAX_ACTIVITY_LIMIT = 500


def _fetch_positions(user_addr: str) -> pd.DataFrame:
    # Raw positions dataframe for a user from the data API.
    url = f"{DATA_API}/positions"
    params = {"user": user_addr, "limit": 100, "sizeThreshold": 0.01}
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    return pd.DataFrame(r.json())


def get_position_in_market(user_addr: str, condition_id: str):
    # Sum YES and NO position sizes for one market from a single positions fetch.
    data = _fetch_positions(user_addr)
    if len(data) == 0:
        return 0.0, 0.0
    data = data[data["conditionId"] == condition_id]
    if len(data) == 0:
        return 0.0, 0.0
    return (
        float(data[data["outcome"] == "Yes"]["size"].sum()),
        float(data[data["outcome"] == "No"]["size"].sum()),
    )


def get_markets_with_position(user_addr: str) -> list:
    # Condition ids where the user has a large imbalance between YES and NO (one API call).
    full = _fetch_positions(user_addr)
    if full.empty:
        return []
    full = full.sort_values(by="conditionId")
    candidates = full.loc[full["size"] > 5.1, "conditionId"].unique()
    condition_ids = []
    for cid in candidates:
        g = full[full["conditionId"] == cid]
        yes_pos = float(g[g["outcome"] == "Yes"]["size"].sum())
        no_pos = float(g[g["outcome"] == "No"]["size"].sum())
        if abs(yes_pos - no_pos) > 2.5:
            condition_ids.append(cid)
    return condition_ids


def read_trade_log(wallet_address: str, limit: int = 1000) -> pd.DataFrame:
    # Recent trades for a wallet as a trimmed dataframe.
    url = f"{DATA_API}/trades"
    params = {"user": wallet_address, "limit": limit}
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    data = r.json()
    return pd.DataFrame(data)[["side", "size", "price", "timestamp", "outcome", "slug"]]


def _normalize_csv_param(value):
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, Iterable):
        return ",".join(str(v) for v in value)
    return str(value)


def get_wallet_activity(
    wallet: str,
    limit: int = 100,
    offset: int = 0,
    market=None,
    event_id=None,
    activity_type=None,
    start: Optional[int] = None,
    end: Optional[int] = None,
    sort_by: Optional[str] = None,
    sort_direction: Optional[str] = None,
    side: Optional[str] = None,
) -> list:
    # Raw activity feed rows with optional Data API filters.
    url = f"{DATA_API}/activity"
    params = {
        "user": wallet.lower(),
        "limit": int(limit),
        "offset": int(offset),
    }
    optional_params = {
        "market": _normalize_csv_param(market),
        "eventId": _normalize_csv_param(event_id),
        "type": _normalize_csv_param(activity_type),
        "start": start,
        "end": end,
        "sortBy": sort_by,
        "sortDirection": sort_direction,
        "side": side,
    }
    for key, value in optional_params.items():
        if value is not None:
            params[key] = value

    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    return r.json()


def get_wallet_activity_pages(
    wallet: str,
    page_limit: int = 100,
    max_pages: int = 3,
    **kwargs,
) -> list:
    # Fetch multiple pages from /activity with a bounded page count.
    rows = []
    for i in range(max_pages):
        page = get_wallet_activity(
            wallet=wallet,
            limit=page_limit,
            offset=i * page_limit,
            **kwargs,
        )
        if not page:
            break
        rows.extend(page)
        if len(page) < page_limit:
            break
    return rows


def fetch_wallet_activity(wallet: str, limit: int = 100, offset: int = 0) -> list:
    # Backward-compatible wrapper: activity feed rows filtered to TRADE events only.
    return get_wallet_activity(
        wallet=wallet,
        limit=limit,
        offset=offset,
        activity_type="TRADE",
    )


def download_account_activity(
    wallet_address: str,
    total_limit: int = 500,
    trades_only: bool = True,
) -> pd.DataFrame:
    """Fetch account activity with paging up to `total_limit` rows.

    When `total_limit` exceeds the Data API per-request cap, issues multiple
    requests and returns one combined DataFrame, newest first when timestamps exist.
    """
    if total_limit <= 0:
        return pd.DataFrame()

    page_size = min(DATA_API_MAX_ACTIVITY_LIMIT, total_limit)
    pages = int(math.ceil(total_limit / float(page_size)))
    activity_type = "TRADE" if trades_only else None

    rows = []
    for i in range(pages):
        remaining = total_limit - len(rows)
        if remaining <= 0:
            break
        batch_limit = min(page_size, remaining)
        page = get_wallet_activity(
            wallet=wallet_address,
            limit=batch_limit,
            offset=i * page_size,
            activity_type=activity_type,
        )
        if not page:
            break
        rows.extend(page)
        if len(page) < batch_limit:
            break

    df = pd.DataFrame(rows)
    if "timestamp" in df.columns:
        df = df.sort_values("timestamp", ascending=False).reset_index(drop=True)
    return df.head(total_limit)
