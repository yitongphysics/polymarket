"""Load markets from a condition_ids file, fetch recent trades, save to Parquet/JSONL.

Example (run from repo root):

    python3 apps/recording/download_market_activity.py \\
        --condition-ids-file apps/recording/condition_ids.txt \\
        --days 7 \\
        --min-cash 10000 \\
        -o apps/recording/data/market_trades_7d.parquet

The pipeline supervisor (`run_pipeline.py`) imports :func:`run_download` directly
so the seen-tx-hash set is reused across cycles instead of re-parsing the file.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Repo root (parent of `apps/`) so `python3 apps/recording/...` finds `polymarket`.
_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import argparse
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Iterable

import pandas as pd

from polymarket.api.markets import get_markets_by_condition_ids
from polymarket.api.trades import get_market_trades_since
from polymarket.utils.condition_ids import read_condition_ids
from polymarket.utils.jsonl import iter_jsonl

DEFAULT_CONDITION_IDS_FILE = os.path.join(os.path.dirname(__file__), "condition_ids.txt")
DEFAULT_LOOKBACK_DAYS = 1
DEFAULT_FETCH_WORKERS = 8


def parse_args():
    p = argparse.ArgumentParser(
        description="Load Gamma markets by condition_ids file, fetch Data API trades in a lookback window, write Parquet/JSONL."
    )
    p.add_argument(
        "--condition-ids-file",
        default=DEFAULT_CONDITION_IDS_FILE,
        help="Path to a text file with one condition_id per line "
        "(extra columns after whitespace are ignored; '#' lines are comments). "
        f"Default: {DEFAULT_CONDITION_IDS_FILE}",
    )
    p.add_argument(
        "--days",
        type=int,
        default=DEFAULT_LOOKBACK_DAYS,
        help=f"Lookback window in days (default: {DEFAULT_LOOKBACK_DAYS})",
    )
    p.add_argument(
        "--since-ts",
        type=int,
        default=None,
        help="Epoch seconds lower bound for trades. If set, overrides --days.",
    )
    p.add_argument(
        "-o",
        "--output",
        required=True,
        help="Output path. Suffix decides format: .jsonl appends incrementally, "
        "anything else writes Parquet.",
    )
    p.add_argument(
        "--min-cash",
        type=float,
        default=0.0,
        help="Minimum trade notional in USDC (filterType=CASH). "
        "Set to 0 to disable. Default: 0.",
    )
    p.add_argument(
        "--include-maker",
        action="store_true",
        help="Include maker fills (takerOnly=false). Default: taker-only.",
    )
    p.add_argument(
        "--price-min",
        type=float,
        default=-0.1,
        help="Min market midpoint price (inclusive) to keep. Default: -0.1.",
    )
    p.add_argument(
        "--price-max",
        type=float,
        default=1.1,
        help="Max market midpoint price (inclusive) to keep. Default: 1.1.",
    )
    p.add_argument(
        "--no-price-filter",
        action="store_true",
        help="Disable the [price-min, price-max] midpoint filter.",
    )
    p.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_FETCH_WORKERS,
        help=f"Concurrent per-market trade fetches (default: {DEFAULT_FETCH_WORKERS}).",
    )
    return p.parse_args()


def load_seen_tx_hashes(path: str) -> set[str]:
    """Build the seen-transactionHash set from an existing JSONL output file."""
    seen: set[str] = set()
    for obj in iter_jsonl(path):
        txh = obj.get("transactionHash")
        if txh:
            seen.add(str(txh))
    return seen


def _filter_live_markets(
    markets: pd.DataFrame,
    price_min: float,
    price_max: float,
    apply_price_filter: bool,
) -> pd.DataFrame:
    """Keep markets that are still live and (optionally) near-50/50 enough to be uncertain."""
    df = markets
    for col, expected in (("closed", False), ("archived", False), ("active", True)):
        if col in df.columns:
            df = df[df[col].fillna(expected) == expected]

    if apply_price_filter and "bestBid" in df.columns and "bestAsk" in df.columns:
        bb = pd.to_numeric(df["bestBid"], errors="coerce")
        ba = pd.to_numeric(df["bestAsk"], errors="coerce")
        mid = (bb + ba) / 2.0
        df = df[mid.between(price_min, price_max, inclusive="both")]

    return df


def _fetch_one(
    cid: str,
    label: str,
    since_ts: int,
    min_cash: float,
    taker_only: bool,
) -> tuple[str, str, pd.DataFrame, Exception | None]:
    """Worker for the per-market thread pool."""
    try:
        trades = get_market_trades_since(
            condition_id=cid,
            since_ts=since_ts,
            min_cash=min_cash if min_cash > 0 else None,
            taker_only=taker_only,
        )
    except Exception as e:
        return cid, label, pd.DataFrame(), e
    return cid, label, trades, None


def fetch_trades_concurrent(
    market_rows: Iterable[tuple[str, str, str | None]],
    since_ts: int,
    min_cash: float,
    taker_only: bool,
    workers: int,
) -> tuple[list[pd.DataFrame], list[tuple[str, str, int]]]:
    """Fetch trades for many markets in parallel.

    ``market_rows`` is an iterable of ``(condition_id, label, slug_or_question)``.
    Returns ``(chunks, truncated_markets)`` where ``chunks`` is a list of
    DataFrames already augmented with ``market_question``/``market_slug``.
    """
    rows = list(market_rows)
    chunks: list[pd.DataFrame] = []
    truncated: list[tuple[str, str, int]] = []
    if not rows:
        return chunks, truncated

    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = {
            pool.submit(_fetch_one, cid, label, since_ts, min_cash, taker_only): (
                cid,
                label,
                extra,
            )
            for (cid, label, extra) in rows
        }
        for fut in as_completed(futures):
            cid, label, extra = futures[fut]
            _, _, trades, err = fut.result()
            if err is not None:
                print(f"Warning: trades failed for {label!r} ({cid}): {err}")
                continue
            if trades.attrs.get("truncated"):
                truncated.append((label, cid, len(trades)))
            if trades.empty:
                continue
            trades = trades.copy()
            # `extra` is the raw `question` from Gamma; we already have slug from the trades payload.
            if isinstance(extra, str) and extra:
                trades["market_question"] = extra
            chunks.append(trades)

    return chunks, truncated


def _dedupe_and_sort(chunks: list[pd.DataFrame]) -> pd.DataFrame:
    if not chunks:
        return pd.DataFrame()
    df = pd.concat(chunks, ignore_index=True)
    if "transactionHash" in df.columns and df["transactionHash"].notna().any():
        df = df.drop_duplicates(subset=["transactionHash"], keep="first")
    if "timestamp" in df.columns:
        df = df.sort_values("timestamp", ascending=False).reset_index(drop=True)
    return df


def _append_jsonl(out_path: str, df: pd.DataFrame, seen_tx_hashes: set[str]) -> tuple[int, int]:
    """Append unseen rows to ``out_path`` and update ``seen_tx_hashes`` in place.

    Returns ``(rows_seen_before_filter, rows_appended)``.
    """
    before = len(df)
    if "transactionHash" in df.columns:
        txh = df["transactionHash"].astype(str)
        valid = df["transactionHash"].notna() & (txh != "None")
        keep = ~valid | ~txh.isin(seen_tx_hashes)
        df = df[keep].copy()
    if df.empty:
        return before, 0
    with open(out_path, "a", buffering=1) as out:
        for row in df.to_dict(orient="records"):
            out.write(json.dumps(row, default=str) + "\n")
    if "transactionHash" in df.columns:
        for txh in df["transactionHash"].dropna().astype(str):
            if txh and txh != "None":
                seen_tx_hashes.add(txh)
    return before, len(df)


def run_download(
    condition_ids: list[str],
    since_ts: int,
    out_path: str,
    *,
    min_cash: float = 0.0,
    taker_only: bool = True,
    price_min: float = -0.1,
    price_max: float = 1.1,
    apply_price_filter: bool = True,
    workers: int = DEFAULT_FETCH_WORKERS,
    seen_tx_hashes: set[str] | None = None,
) -> int:
    """Run one download cycle. Returns the number of trade rows written.

    When ``out_path`` ends in ``.jsonl`` the function appends incrementally and
    de-duplicates against ``seen_tx_hashes`` (mutated in place). Other suffixes
    overwrite a Parquet snapshot; ``seen_tx_hashes`` is ignored.
    """
    out_dir = os.path.dirname(out_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    out_is_jsonl = out_path.lower().endswith(".jsonl")
    if out_is_jsonl and seen_tx_hashes is None:
        seen_tx_hashes = load_seen_tx_hashes(out_path)

    markets = get_markets_by_condition_ids(condition_ids)
    pre_count = len(markets)
    markets = _filter_live_markets(
        markets,
        price_min=price_min,
        price_max=price_max,
        apply_price_filter=apply_price_filter,
    )
    print(
        f"Filtered to {len(markets)}/{pre_count} live markets "
        f"(price in [{price_min}, {price_max}]"
        f"{'; disabled' if not apply_price_filter else ''})."
    )
    if markets.empty:
        print("No markets matched the filters.")
        if not out_is_jsonl:
            pd.DataFrame().to_parquet(out_path, index=False)
        print(f"wrote 0 rows to {out_path}")
        return 0

    market_rows: list[tuple[str, str, str | None]] = []
    for _, row in markets.iterrows():
        cid = row.get("conditionId")
        if cid is None or (isinstance(cid, float) and pd.isna(cid)):
            continue
        cid = str(cid).strip()
        if not cid:
            continue
        question = row.get("question")
        slug = row.get("slug")
        label = slug if isinstance(slug, str) and slug else str(question)[:80]
        q_for_tag = (
            question
            if isinstance(question, str) and question
            else None
        )
        market_rows.append((cid, label, q_for_tag))

    chunks, truncated_markets = fetch_trades_concurrent(
        market_rows,
        since_ts=since_ts,
        min_cash=min_cash,
        taker_only=taker_only,
        workers=workers,
    )
    df = _dedupe_and_sort(chunks)

    if out_is_jsonl:
        before, appended = _append_jsonl(out_path, df, seen_tx_hashes or set())
        print(
            f"Matched {len(markets)} markets; wrote {appended} trade rows to {out_path} "
            f"(filtered {before - appended} already-seen rows)"
        )
        rows_written = appended
    else:
        df.to_parquet(out_path, index=False)
        print(
            f"Matched {len(markets)} markets; wrote {len(df)} trade rows to {out_path}"
        )
        rows_written = len(df)

    if truncated_markets:
        print(
            f"\nWARNING: {len(truncated_markets)} market(s) hit the Data API "
            f"offset cap (~4000 trades per market) and likely have additional "
            f"older trades within the window that this endpoint cannot return. "
            f"Raise --min-cash or shorten the window to cover them:"
        )
        for label, cid, n in truncated_markets:
            print(f"  - {label} ({cid}): {n} trades fetched")

    return rows_written


def main():
    args = parse_args()
    condition_ids = read_condition_ids(args.condition_ids_file)

    if args.since_ts is not None:
        since_ts = int(args.since_ts)
    else:
        days = max(1, args.days)
        since_ts = int(time.time()) - days * 86400

    out_path = os.path.abspath(args.output)
    run_download(
        condition_ids=condition_ids,
        since_ts=since_ts,
        out_path=out_path,
        min_cash=args.min_cash,
        taker_only=not args.include_maker,
        price_min=args.price_min,
        price_max=args.price_max,
        apply_price_filter=not args.no_price_filter,
        workers=args.workers,
    )


if __name__ == "__main__":
    main()
