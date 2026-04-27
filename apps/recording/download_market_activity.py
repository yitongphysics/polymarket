"""Search markets by keyword / anti-keyword, fetch recent trades, save to Parquet."""

import sys
from pathlib import Path

# Repo root (parent of `apps/`) so `python3 apps/recording/...` finds `polymarket`.
_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import argparse
import os
import time
from datetime import datetime, timezone

import pandas as pd

from polymarket.api.markets import get_markets_by_slug_keyword
from polymarket.api.trades import get_market_trades_since

DEFAULT_KEYWORDS = ["Will Donald Trump win the 2028 US Presidential Election"]                        # contains at least one of these keywords
DEFAULT_ANTI_KEYWORDS: list[str] = ['israel']       # does not contain any of these keywords
DEFAULT_LOOKBACK_DAYS = 7
DEFAULT_OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "data")


def parse_args():
    p = argparse.ArgumentParser(
        description="Search Gamma markets by keywords, fetch Data API trades in a lookback window, write Parquet."
    )
    p.add_argument(
        "-k",
        "--keyword",
        action="append",
        dest="keywords",
        help="Substring to match in market question (case-insensitive, OR). "
        "Repeatable. Default: bitcoin.",
    )
    p.add_argument(
        "-x",
        "--exclude",
        action="append",
        dest="excludes",
        help="Substring to exclude from questions. Repeatable.",
    )
    p.add_argument(
        "--days",
        type=int,
        default=DEFAULT_LOOKBACK_DAYS,
        help=f"Lookback window in days (default: {DEFAULT_LOOKBACK_DAYS})",
    )
    p.add_argument(
        "-o",
        "--output",
        default=None,
        help="Parquet output path (default: data/market_trades_<UTC timestamp>.parquet)",
    )
    p.add_argument(
        "--page-size",
        type=int,
        default=10000,
        help="Per-request limit for /trades pagination (default: 10000)",
    )
    p.add_argument(
        "--max-pages",
        type=int,
        default=50,
        help="Max pages per market for /trades (default: 50)",
    )
    p.add_argument(
        "--min-cash",
        type=float,
        default=10000.0,
        help="Minimum trade notional in USDC (filterType=CASH). "
        "Set to 0 to disable. Default: 10000.",
    )
    p.add_argument(
        "--include-maker",
        action="store_true",
        help="Include maker fills (takerOnly=false). Default: taker-only.",
    )
    p.add_argument(
        "--price-min",
        type=float,
        default=0.2,
        help="Min market midpoint price (inclusive) to keep. Default: 0.2.",
    )
    p.add_argument(
        "--price-max",
        type=float,
        default=0.8,
        help="Max market midpoint price (inclusive) to keep. Default: 0.8.",
    )
    p.add_argument(
        "--no-price-filter",
        action="store_true",
        help="Disable the [price-min, price-max] midpoint filter.",
    )
    return p.parse_args()


def _filter_live_markets(
    markets: pd.DataFrame,
    price_min: float,
    price_max: float,
    apply_price_filter: bool,
) -> pd.DataFrame:
    """Keep markets that are still live and (optionally) near-50/50 enough to be uncertain."""
    df = markets
    # Drop closed/archived/inactive markets using whichever flags Gamma returned.
    for col, expected in (("closed", False), ("archived", False), ("active", True)):
        if col in df.columns:
            df = df[df[col].fillna(expected) == expected]

    if apply_price_filter and "bestBid" in df.columns and "bestAsk" in df.columns:
        bb = pd.to_numeric(df["bestBid"], errors="coerce")
        ba = pd.to_numeric(df["bestAsk"], errors="coerce")
        mid = (bb + ba) / 2.0
        df = df[mid.between(price_min, price_max, inclusive="both")]

    return df


def main():
    args = parse_args()
    keywords = args.keywords if args.keywords else list(DEFAULT_KEYWORDS)
    excludes = args.excludes if args.excludes is not None else list(DEFAULT_ANTI_KEYWORDS)

    days = max(1, args.days)
    since_ts = int(time.time()) - days * 86400

    if args.output:
        out_path = os.path.abspath(args.output)
    else:
        os.makedirs(DEFAULT_OUTPUT_DIR, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        out_path = os.path.join(DEFAULT_OUTPUT_DIR, f"market_trades_{stamp}.parquet")
        out_path = os.path.join(DEFAULT_OUTPUT_DIR, f"market_trades_test.parquet")

    out_dir = os.path.dirname(out_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    markets = get_markets_by_slug_keyword(
        keyWordList=keywords,
        doNotContainList=excludes,
    )
    pre_count = len(markets)
    markets = _filter_live_markets(
        markets,
        price_min=args.price_min,
        price_max=args.price_max,
        apply_price_filter=not args.no_price_filter,
    )
    print(
        f"Filtered to {len(markets)}/{pre_count} live markets "
        f"(price in [{args.price_min}, {args.price_max}]"
        f"{'; disabled' if args.no_price_filter else ''})."
    )
    if markets.empty:
        print("No markets matched the filters.")
        empty = pd.DataFrame()
        empty.to_parquet(out_path, index=False)
        print(f"wrote 0 rows to {out_path}")
        return
    else:
        for _, row in markets.iterrows():
            print(row.get("question"))

    chunks = []
    for _, row in markets.iterrows():
        cid = row.get("conditionId")
        if cid is None or (isinstance(cid, float) and pd.isna(cid)):
            continue
        cid = str(cid).strip()
        if not cid:
            continue

        question = row.get("question")
        slug = row.get("slug")

        try:
            trades = get_market_trades_since(
                condition_id=cid,
                since_ts=since_ts,
                page_size=args.page_size,
                max_pages=args.max_pages,
                min_cash=args.min_cash if args.min_cash > 0 else None,
                taker_only=not args.include_maker,
            )
        except Exception as e:
            label = slug if isinstance(slug, str) and slug else str(question)[:80]
            print(f"Warning: trades failed for {label!r} ({cid}): {e}")
            continue

        if trades.empty:
            continue

        trades = trades.copy()
        if question is not None and not (isinstance(question, float) and pd.isna(question)):
            trades["market_question"] = question
        if slug is not None and not (isinstance(slug, float) and pd.isna(slug)):
            trades["market_slug"] = slug

        chunks.append(trades)

    if not chunks:
        df = pd.DataFrame()
    else:
        df = pd.concat(chunks, ignore_index=True)
        if "transactionHash" in df.columns and df["transactionHash"].notna().any():
            df = df.drop_duplicates(subset=["transactionHash"], keep="first")
        if "timestamp" in df.columns:
            df = df.sort_values("timestamp", ascending=False).reset_index(drop=True)
    
    df.to_parquet(out_path, index=False)
    print(
        f"Matched {len(markets)} markets; wrote {len(df)} trade rows to {out_path}"
    )


if __name__ == "__main__":
    main()
