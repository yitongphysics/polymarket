"""Poll Polymarket Data API /trades for a list of condition_ids and append to JSONL."""

import sys
from pathlib import Path

# Repo root (parent of `apps/`) so `python3 apps/recording/...` finds `polymarket`.
_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import argparse
import json
import os
import random
import signal
import time
from collections import deque
from datetime import datetime, timezone

import requests

from polymarket.api.trades import _TRADE_COLS, get_market_trades_since

DEFAULT_OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "data")


def parse_args():
    p = argparse.ArgumentParser(
        description="Listen to Polymarket trades for a list of markets via Data API polling."
    )
    p.add_argument(
        "--condition-ids-file",
        required=True,
        help="Path to a text file with one condition_id per line. Blanks and lines starting with '#' are ignored.",
    )
    p.add_argument(
        "-o",
        "--output",
        default=None,
        help="Output JSONL path (default: data/trades_<UTC>.jsonl).",
    )
    p.add_argument(
        "--poll-interval",
        type=float,
        default=60.0,
        help="Seconds between poll cycles (default: 60).",
    )
    p.add_argument(
        "--lookback-seconds",
        type=float,
        default=None,
        help="On startup, look back this many seconds for the first poll (default: same as --poll-interval).",
    )
    p.add_argument(
        "--min-cash",
        type=float,
        default=0.0,
        help="Server-side minimum trade notional in USDC (filterType=CASH). 0 disables. Default: 0.",
    )
    return p.parse_args()


def read_condition_ids(path: str) -> list[str]:
    cids = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            cids.append(line)
    if not cids:
        raise SystemExit(f"No condition_ids found in {path}")
    return cids


def is_rate_limited(exc: Exception) -> bool:
    return (
        isinstance(exc, requests.HTTPError)
        and exc.response is not None
        and exc.response.status_code == 429
    )


def backoff_sleep(attempt: int) -> float:
    # 1, 2, 4, 8, 16, 30s with jitter; cap at 30s.
    delay = min(2 ** attempt, 30) + random.uniform(0, 1)
    time.sleep(delay)
    return delay


def main():
    args = parse_args()

    cids = read_condition_ids(args.condition_ids_file)

    if args.output:
        out_path = os.path.abspath(args.output)
    else:
        os.makedirs(DEFAULT_OUTPUT_DIR, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        out_path = os.path.join(DEFAULT_OUTPUT_DIR, f"trades_{stamp}.jsonl")

    out_dir = os.path.dirname(out_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    lookback = args.lookback_seconds if args.lookback_seconds is not None else args.poll_interval
    now = int(time.time())
    last_seen: dict[str, int] = {cid: now - int(lookback) for cid in cids}

    # Cross-poll dedup. Each request asks for trades >= last_seen[cid], so the
    # boundary trade (timestamp == last_seen) is re-fetched and must be deduped.
    # Pruned to entries newer than now - prune_window.
    prune_window = max(int(args.poll_interval * 4), 30)
    seen: deque[tuple[int, str]] = deque()
    seen_set: set[str] = set()

    print(f"Polling {len(cids)} markets every {args.poll_interval}s -> {out_path}")
    if args.min_cash > 0:
        print(f"Server-side filter: min cash = ${args.min_cash:,.0f}")

    stop = {"flag": False}

    def on_sigint(signum, frame):
        stop["flag"] = True
        print("\nStopping after current poll...")

    signal.signal(signal.SIGINT, on_sigint)

    rows_written = 0
    poll_idx = 0

    with open(out_path, "a", buffering=1) as out:
        while not stop["flag"]:
            poll_started = time.time()
            poll_rows = 0

            cid_summaries = []
            for cid in cids:
                if stop["flag"]:
                    break

                window_start = last_seen[cid]

                attempt = 0
                while True:
                    try:
                        df = get_market_trades_since(
                            condition_id=cid,
                            since_ts=window_start,
                            page_size=10000,
                            max_pages=50,
                            taker_only=True,
                            min_cash=args.min_cash if args.min_cash > 0 else None,
                        )
                        break
                    except Exception as e:
                        if is_rate_limited(e) and attempt < 6:
                            d = backoff_sleep(attempt)
                            print(f"429 on {cid[:10]}... backing off {d:.1f}s")
                            attempt += 1
                            continue
                        print(f"Warning: poll failed for {cid[:10]}...: {e}")
                        df = None
                        break

                if df is None or df.empty:
                    continue

                observed_at = int(time.time())
                # Oldest first so last_seen advances monotonically per market.
                df_iter = df.sort_values("timestamp", ascending=True)
                cid_new = 0
                for row in df_iter.itertuples(index=False):
                    txh = getattr(row, "transactionHash", None)
                    ts = int(getattr(row, "timestamp"))
                    last_seen[cid] = max(last_seen[cid], ts)
                    if not txh or txh in seen_set:
                        continue
                    record = {col: getattr(row, col) for col in _TRADE_COLS}
                    record["observed_at"] = observed_at
                    out.write(json.dumps(record, default=str) + "\n")

                    seen_set.add(txh)
                    seen.append((ts, txh))
                    cid_new += 1
                    poll_rows += 1

                if cid_new > 0:
                    cid_summaries.append(
                        f"{cid[:10]}.. [{window_start} -> {last_seen[cid]}] +{cid_new}"
                    )

            # Prune dedup memory.
            cutoff = int(time.time()) - prune_window
            while seen and seen[0][0] < cutoff:
                _, old_tx = seen.popleft()
                seen_set.discard(old_tx)

            rows_written += poll_rows
            poll_idx += 1
            elapsed = time.time() - poll_started
            if poll_rows > 0 or poll_idx % 5 == 0:
                print(
                    f"poll #{poll_idx}: +{poll_rows} rows ({rows_written} total) "
                    f"in {elapsed:.2f}s, dedup_size={len(seen_set)}"
                )
                for summary in cid_summaries:
                    print(f"  {summary}")

            sleep_for = max(0.0, args.poll_interval - elapsed)
            slept = 0.0
            while slept < sleep_for and not stop["flag"]:
                step = min(0.5, sleep_for - slept)
                time.sleep(step)
                slept += step

    print(f"Wrote {rows_written} rows to {out_path}")


if __name__ == "__main__":
    main()
