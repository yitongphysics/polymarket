"""Download Polymarket price history to CSV."""

import argparse
import os
import time

import pandas as pd

from polymarket.api.markets import get_markets_by_slug_keyword
from polymarket.api.orderbook import get_history_price


OUTFILE = os.path.join(os.path.dirname(__file__), "data", "polymarket_price_history.csv")
HISTORY_LOOKBACK_S = 60 * 60 * 24 * 8


def parse_args():
    # CLI for keywords, exclusions, optional date filters, and output CSV path.
    p = argparse.ArgumentParser(
        description="Download YES-token price history for markets whose questions match keywords."
    )
    p.add_argument(
        "-k",
        "--keyword",
        action="append",
        dest="keywords",
        help="Substring to match in the market question (case-insensitive). "
        "Repeat for OR logic. Default: bitcoin and ethereum.",
    )
    p.add_argument(
        "-x",
        "--exclude",
        action="append",
        dest="excludes",
        help="Substring to exclude from questions. Repeatable.",
    )
    p.add_argument(
        "-o",
        "--output",
        default=OUTFILE,
        help=f"Output CSV path (default: {OUTFILE})",
    )
    p.add_argument(
        "--end-date-min",
        default=None,
        metavar="ISO_DATE",
        help="Only include markets with end date after this (e.g. 2026-04-01).",
    )
    p.add_argument(
        "--start-date-max",
        default=None,
        metavar="ISO_DATE",
        help="Only include markets with start date before this.",
    )
    return p.parse_args()


def main():
    # Fetch matching markets and append YES-token price history rows to the output CSV.
    args = parse_args()
    keywords = args.keywords if args.keywords else ["trump announces end of military operations against iran"]
    excludes = args.excludes or []

    out_dir = os.path.dirname(os.path.abspath(args.output))
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    markets = get_markets_by_slug_keyword(
        keyWordList=keywords,
        doNotContainList=excludes,
        end_date_min=args.end_date_min,
        start_date_max=args.start_date_max,
    )
    if markets.empty:
        print("No markets matched the filters.")
        return

    print(f"Found {len(markets)} markets; writing history to {args.output}")

    now = int(time.time())
    start_ts = now - HISTORY_LOOKBACK_S
    chunks = []

    for _, row in markets.iterrows():
        yes = row.get("YES")
        if yes is None or pd.isna(yes):
            continue
        yes = str(yes).strip()
        if not yes:
            continue

        slug = row.get("slug") or ""
        question = row.get("question") or ""
        label = slug if isinstance(slug, str) and slug else str(question)[:80]
        print("Downloading:", label)

        try:
            history = get_history_price(
                yes,
                startTs=start_ts,
                endTs=now,
                fidelity=1,
            )
        except Exception as e:
            print(f"  Error: {e}")
            continue

        if history.empty:
            continue

        history = history.copy()
        history["yes_token_id"] = yes
        for key in ("slug", "question", "conditionId", "id"):
            val = row.get(key)
            if val is not None and not (isinstance(val, float) and pd.isna(val)):
                history[key if key != "id" else "market_id"] = val
        chunks.append(history)

    if not chunks:
        print("No price history rows were downloaded.")
        return

    df = pd.concat(chunks, ignore_index=True)
    df.to_csv(args.output, index=False)
    print(df.head())


if __name__ == "__main__":
    main()
