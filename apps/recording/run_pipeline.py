"""Single-process recording pipeline: WS listener + periodic Data API backfill.

Example (run from repo root):
    python3 apps/recording/run_pipeline.py \
        --condition-ids-file apps/recording/condition_ids.txt \
        --output-dir apps/recording/data \
        --file-name iran \
        --interval-seconds 60

Runs the WebSocket recorder and the trades downloader in the same Python
process. The downloader executes in a worker thread on a fixed interval; the
WS listener (which auto-reconnects internally) is monitored continuously and
is never blocked by a slow downloader cycle.
"""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import argparse
import asyncio
import os
import signal
import time

from apps.recording.download_market_activity import (
    load_seen_tx_hashes,
    run_download,
)
from apps.recording.listen_market_book_and_trades import (
    resolve_yes_tokens,
    run_recorder,
)
from polymarket.utils.condition_ids import read_condition_ids
from polymarket.utils.jsonl import repair_jsonl_tail


def parse_args():
    p = argparse.ArgumentParser(
        description=(
            "Run the WebSocket recorder and trigger trade backfills on a fixed interval."
        )
    )
    p.add_argument(
        "--condition-ids-file",
        required=True,
        help="Path to condition_ids text file.",
    )
    p.add_argument(
        "--output-dir",
        default=os.path.join(os.path.dirname(__file__), "data"),
        help="Directory for output files.",
    )
    p.add_argument(
        "--file-name",
        default="pipeline",
        help="Base filename X used for X_updates.jsonl, X_trades.jsonl, and X_trades_wallet.jsonl.",
    )
    p.add_argument(
        "--interval-seconds",
        type=int,
        default=60,
        help="Seconds between downloader runs (default: 60).",
    )
    p.add_argument(
        "--min-cash",
        type=float,
        default=0.0,
        help="Minimum trade notional (USDC) for the downloader. Default: 0 (disabled).",
    )
    p.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Concurrent per-market trade fetches inside each downloader cycle.",
    )
    return p.parse_args()


async def _periodic_downloader(
    condition_ids: list[str],
    out_path: str,
    interval: int,
    min_cash: float,
    workers: int,
    seen_tx_hashes: set[str],
    stop_event: asyncio.Event,
) -> None:
    """Trigger ``run_download`` every ``interval`` seconds in a worker thread."""
    last_run_started_at: int | None = None
    next_run_at = time.time()
    while not stop_event.is_set():
        now = time.time()
        if now < next_run_at:
            try:
                await asyncio.wait_for(
                    stop_event.wait(), timeout=next_run_at - now
                )
            except asyncio.TimeoutError:
                pass
            continue

        run_started = int(time.time())
        since_ts = (
            run_started - interval
            if last_run_started_at is None
            else int(last_run_started_at)
        )
        print(
            f"[supervisor] downloader cycle: since_ts={since_ts} "
            f"({run_started - since_ts}s window)"
        )
        t0 = time.time()
        try:
            written = await asyncio.to_thread(
                run_download,
                condition_ids,
                since_ts,
                out_path,
                min_cash=min_cash,
                workers=workers,
                seen_tx_hashes=seen_tx_hashes,
            )
            print(
                f"[supervisor] downloader done: wrote {written} rows "
                f"in {time.time() - t0:.1f}s"
            )
        except Exception as e:
            print(f"[supervisor] downloader cycle failed: {e}")

        last_run_started_at = run_started
        next_run_at = run_started + interval


async def supervise(args: argparse.Namespace) -> None:
    target_dir = os.path.join(args.output_dir, f"market_data_{args.file_name}")
    os.makedirs(target_dir, exist_ok=True)
    book_path = os.path.join(target_dir, f"{args.file_name}_updates.jsonl")
    trades_path = os.path.join(target_dir, f"{args.file_name}_trades.jsonl")
    dl_path = os.path.join(target_dir, f"{args.file_name}_trades_wallet.jsonl")

    truncated = repair_jsonl_tail(dl_path)
    if truncated:
        print(f"[supervisor] repaired {truncated} bytes of partial tail in {dl_path}")
    seen_tx_hashes = load_seen_tx_hashes(dl_path)
    print(f"[supervisor] loaded {len(seen_tx_hashes)} seen transactionHashes")

    condition_ids = read_condition_ids(args.condition_ids_file)
    print(f"[supervisor] resolving {len(condition_ids)} condition_ids via Gamma...")
    token_meta = resolve_yes_tokens(condition_ids)
    print(f"[supervisor] subscribing to {len(token_meta)} YES tokens")

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _request_stop():
        if not stop_event.is_set():
            print("[supervisor] stop requested")
            stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_stop)
        except NotImplementedError:
            pass

    recorder_task = asyncio.create_task(
        run_recorder(token_meta, book_path, trades_path, stop_event),
        name="recorder",
    )
    downloader_task = asyncio.create_task(
        _periodic_downloader(
            condition_ids,
            dl_path,
            args.interval_seconds,
            args.min_cash,
            args.workers,
            seen_tx_hashes,
            stop_event,
        ),
        name="downloader",
    )

    try:
        await stop_event.wait()
    finally:
        # `run_recorder` shuts itself down on stop_event; just join both tasks.
        for t in (recorder_task, downloader_task):
            try:
                await asyncio.wait_for(t, timeout=10)
            except asyncio.TimeoutError:
                t.cancel()
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass
            except (asyncio.CancelledError, Exception):
                pass
        print("[supervisor] stopped")


def main():
    args = parse_args()
    asyncio.run(supervise(args))


if __name__ == "__main__":
    main()
