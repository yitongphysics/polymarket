from __future__ import annotations

import pandas as pd
import requests

from polymarket.api.account import DATA_API

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


def get_market_trades_since(
    condition_id: str,
    since_ts: int,
    page_size: int = 500,
    max_pages: int = 50,
    timeout: float = 10.0,
    min_cash: float | None = None,
    taker_only: bool = True,
) -> pd.DataFrame:
    """Paginate Data API `/trades` for a market until older than `since_ts` or limits hit.

    Rows are filtered to ``timestamp >= since_ts`` and returned newest first.
    ``min_cash`` applies a server-side ``filterType=CASH`` notional threshold.
    """
    rows = []
    offset = 0
    for _ in range(max_pages):
        params: dict = {
            "market": condition_id,
            "limit": int(page_size),
            "offset": int(offset),
            "takerOnly": "true" if taker_only else "false",
        }
        if min_cash is not None and min_cash > 0:
            params["filterType"] = "CASH"
            params["filterAmount"] = float(min_cash)

        r = requests.get(f"{DATA_API}/trades", params=params, timeout=timeout)
        # Data API may reject large offsets; return trades collected so far.
        if not r.ok:
            if r.status_code == 400 and offset > 0:
                break
            r.raise_for_status()
        page = r.json()
        if not page:
            break

        df = _normalize_trades_df(page)
        if df.empty:
            break

        ts = pd.to_numeric(df["timestamp"], errors="coerce")
        df = df.assign(_ts=ts)
        in_window = df[df["_ts"] >= since_ts].drop(columns=["_ts"])
        rows.append(in_window)

        oldest = ts.min()
        if pd.isna(oldest) or oldest < since_ts:
            break

        # Advance by what we actually received (server may silently cap `limit`).
        received = len(page)
        offset += received
        # Stop only on an empty page; a short page can still mean "server cap < page_size".

    if not rows:
        return pd.DataFrame(columns=_TRADE_COLS)

    out = pd.concat(rows, ignore_index=True)
    if "transactionHash" in out.columns and out["transactionHash"].notna().any():
        out = out.drop_duplicates(subset=["transactionHash"], keep="first")
    out = out.sort_values("timestamp", ascending=False).reset_index(drop=True)
    return out


def get_recent_market_trades(condition_id: str, limit: int = 10) -> pd.DataFrame:
    """Recent trades for a market (Data API `/trades`), sorted newest first.

    `condition_id` is the market id passed to the API as the `market` query param.
    """
    r = requests.get(
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
