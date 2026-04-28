from __future__ import annotations

import pandas as pd
import requests

from polymarket.api.account import DATA_API
from polymarket.utils.http import get_with_retry

_TRADE_COLS = [
    "timestamp",
    "proxyWallet",
    "side",
    "outcome",
    "price",
    "size",
    "slug",
    "conditionId",
    "transactionHash",
]


def _normalize_trades_df(trades: list) -> pd.DataFrame:
    df = pd.DataFrame(trades)
    if df.empty:
        return pd.DataFrame(columns=_TRADE_COLS)
    for col in _TRADE_COLS:
        if col not in df.columns:
            df[col] = None
    return df[_TRADE_COLS]


# Empirically observed limits on data-api.polymarket.com/trades (OpenAPI spec lies):
# - `limit` is silently clamped to 1000 (spec claims 10000).
# - `offset` accepts 0..3000 inclusive; offset>3000 returns HTTP 400 regardless
#   of the matched-set size.
# So the absolute ceiling per fetch is 4 pages * 1000 = 4000 rows per market.
_PAGE_SIZE = 1000


def get_market_trades_since(
    condition_id: str,
    since_ts: int,
    timeout: float = 10.0,
    min_cash: float | None = None,
    taker_only: bool = True,
) -> pd.DataFrame:
    """Paginate Data API ``/trades`` for a market until older than ``since_ts``
    or the server's offset cap rejects us. Newest first.

    ``min_cash`` applies a server-side ``filterType=CASH`` notional threshold.

    ``out.attrs["truncated"]`` is True when the server returned HTTP 400 on the
    next page (offset beyond the 3000 cap) while the oldest fetched trade was
    still within the window — i.e. the matched set has more than ~4000 trades
    but only the most recent ~4000 are reachable through this endpoint.
    """
    rows: list[pd.DataFrame] = []
    offset = 0
    truncated = False
    while True:
        params: dict = {
            "market": condition_id,
            "limit": _PAGE_SIZE,
            "offset": offset,
            "takerOnly": "true" if taker_only else "false",
        }
        if min_cash is not None and min_cash > 0:
            params["filterType"] = "CASH"
            params["filterAmount"] = float(min_cash)

        try:
            r = get_with_retry(
                f"{DATA_API}/trades", params=params, timeout=timeout
            )
        except requests.HTTPError as e:
            resp = e.response
            if resp is not None and resp.status_code == 400 and offset > 0:
                truncated = True
                break
            raise
        if not r.ok:
            if r.status_code == 400 and offset > 0:
                truncated = True
                break
            r.raise_for_status()
        page = r.json() or []
        if not page:
            break

        df = _normalize_trades_df(page)
        if df.empty:
            break

        ts = pd.to_numeric(df["timestamp"], errors="coerce")
        rows.append(df[ts >= since_ts])

        oldest = ts.min()
        if pd.isna(oldest) or oldest < since_ts:
            break

        offset += len(page)

    if not rows:
        out = pd.DataFrame(columns=_TRADE_COLS)
        out.attrs["truncated"] = truncated
        return out

    out = pd.concat(rows, ignore_index=True)
    if "transactionHash" in out.columns and out["transactionHash"].notna().any():
        out = out.drop_duplicates(subset=["transactionHash"], keep="first")
    out = out.sort_values("timestamp", ascending=False).reset_index(drop=True)
    out.attrs["truncated"] = truncated
    return out


def get_recent_market_trades(condition_id: str, limit: int = 10) -> pd.DataFrame:
    """Recent trades for a market (Data API `/trades`), sorted newest first.

    `condition_id` is the market id passed to the API as the `market` query param.
    """
    r = get_with_retry(
        f"{DATA_API}/trades",
        params={"market": condition_id, "limit": int(limit)},
        timeout=10,
    )
    r.raise_for_status()
    trades = r.json()
    df = _normalize_trades_df(trades)
    if df.empty:
        return df
    return df.sort_values("timestamp", ascending=False).reset_index(drop=True)
