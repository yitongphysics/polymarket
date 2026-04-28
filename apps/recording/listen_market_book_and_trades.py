"""Listen to Polymarket CLOB WebSocket for YES tokens and record trades + top-of-book changes.

Example (run from repo root):
    python3 apps/recording/listen_market_book_and_trades.py \
        --condition-ids-file apps/recording/condition_ids.txt \
        --book-output apps/recording/data/pipeline_updates.jsonl \
        --trades-output apps/recording/data/pipeline_trades.jsonl

The pipeline supervisor (`run_pipeline.py`) imports :func:`resolve_yes_tokens`
and :func:`run_recorder` directly instead of spawning this script.
"""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import argparse
import asyncio
import json
import os
import signal
import time
from datetime import datetime, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

CHICAGO_TZ = ZoneInfo("America/Chicago")

from polymarket.utils.clob import yes_token_id
from polymarket.utils.condition_ids import read_condition_ids
from polymarket.utils.http import get_with_retry
from polymarket.utils.jsonl import repair_jsonl_tail
from polymarket.ws.listener import AsyncWebSocketListenQuotes

GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"

DEFAULT_OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "data")


def parse_args():
    p = argparse.ArgumentParser(
        description="Record trades and top-of-book changes for YES tokens via CLOB WebSocket."
    )
    p.add_argument(
        "--condition-ids-file",
        required=True,
        help="Path to a text file with one condition_id per line. Blanks and lines starting with '#' are ignored.",
    )
    p.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help=f"Directory for output JSONL files (default: {DEFAULT_OUTPUT_DIR}).",
    )
    p.add_argument(
        "--run-name",
        default=None,
        help="Suffix for output filenames (default: UTC timestamp).",
    )
    p.add_argument(
        "--book-output",
        default=None,
        help="Explicit JSONL output path for book updates. Overrides --output-dir/--run-name naming.",
    )
    p.add_argument(
        "--trades-output",
        default=None,
        help="Explicit JSONL output path for trade events. Overrides --output-dir/--run-name naming.",
    )
    return p.parse_args()


def resolve_yes_tokens(condition_ids: list[str]) -> dict[str, dict]:
    """Return ``{yes_token_id: {"condition_id", "slug"}}``.

    Verifies the YES leg via the market's ``outcomes`` field rather than
    blindly trusting ``clobTokenIds[0]``. Markets that are closed, missing,
    or whose YES leg can't be confirmed are skipped with a warning.
    """
    markets: list[dict] = []
    BATCH = 25
    for i in range(0, len(condition_ids), BATCH):
        chunk = condition_ids[i : i + BATCH]
        r = get_with_retry(
            GAMMA_MARKETS_URL,
            params={"condition_ids": chunk},
            timeout=15,
        )
        r.raise_for_status()
        markets.extend(r.json() or [])
    if not markets:
        raise SystemExit(
            f"Gamma returned no markets for {len(condition_ids)} condition_ids."
        )

    by_cid = {str(m.get("conditionId")): m for m in markets if m.get("conditionId")}

    token_meta: dict[str, dict] = {}
    missing: list[str] = []
    closed: list[str] = []
    unverified: list[str] = []
    for cid in condition_ids:
        m = by_cid.get(cid)
        if m is None:
            missing.append(cid)
            continue
        if m.get("closed"):
            closed.append(cid)
            continue
        # Prefer shortOutcomes when present (e.g. "Yes"/"No"); fall back to outcomes.
        yes_id = yes_token_id(
            m.get("clobTokenIds"), m.get("shortOutcomes") or m.get("outcomes")
        )
        if not yes_id:
            unverified.append(cid)
            continue
        token_meta[yes_id] = {
            "condition_id": cid,
            "slug": str(m.get("slug") or ""),
        }

    for cid in missing:
        print(f"Warning: condition_id not returned by Gamma: {cid}")
    for cid in closed:
        print(f"Warning: market is closed, skipping: {cid}")
    for cid in unverified:
        print(f"Warning: could not verify YES leg for {cid}; skipping.")

    if not token_meta:
        raise SystemExit("No subscribable YES tokens after resolution.")
    return token_meta


def _best(levels, side: str):
    if not levels:
        return None
    prices = [Decimal(l["price"]) for l in levels if "price" in l]
    if not prices:
        return None
    return max(prices) if side == "bid" else min(prices)


def _to_str(x) -> str | None:
    if x is None:
        return None
    return str(x)


class Recorder:
    def __init__(self, token_meta: dict[str, dict], trades_file, book_file):
        self.token_meta = token_meta
        self.trades_file = trades_file
        self.book_file = book_file
        self.last_top: dict[str, tuple[str | None, str | None]] = {}
        self.trades_count = 0
        self.book_count = 0

    def _write(self, fh, record: dict) -> None:
        fh.write(json.dumps(record) + "\n")

    def _maybe_emit_book(
        self,
        token_id: str,
        bb: str | None,
        ba: str | None,
        ts: str | None,
        source: str,
    ) -> None:
        prev = self.last_top.get(token_id)
        cur = (bb, ba)
        if prev == cur:
            return
        self.last_top[token_id] = cur
        meta = self.token_meta[token_id]
        self._write(
            self.book_file,
            {
                "event_type": "book",
                "ts": ts,
                "observed_at": int(time.time()),
                "token_id": token_id,
                "condition_id": meta["condition_id"],
                "slug": meta["slug"],
                "best_bid": bb,
                "best_ask": ba,
                "source": source,
            },
        )
        self.book_count += 1

    async def handle(self, message) -> None:
        if not message or message == "PONG":
            return
        try:
            data = json.loads(message)
        except (TypeError, json.JSONDecodeError):
            return

        msgs = data if isinstance(data, list) else [data]
        for m in msgs:
            if not isinstance(m, dict):
                continue
            await self._handle_one(m)

    async def _handle_one(self, m: dict) -> None:
        et = m.get("event_type")
        if et == "book":
            tid = m.get("asset_id") or m.get("market")
            if tid not in self.token_meta:
                return
            bb = _best(m.get("bids") or [], "bid")
            ba = _best(m.get("asks") or [], "ask")
            self._maybe_emit_book(
                tid, _to_str(bb), _to_str(ba), m.get("timestamp"), source="book"
            )

        elif et == "price_change":
            ts = m.get("timestamp")
            for pc in m.get("price_changes") or []:
                tid = pc.get("asset_id") or pc.get("market")
                if tid not in self.token_meta:
                    continue
                self._maybe_emit_book(
                    tid,
                    _to_str(pc.get("best_bid")),
                    _to_str(pc.get("best_ask")),
                    ts,
                    source="price_change",
                )

        elif et == "last_trade_price":
            tid = m.get("asset_id") or m.get("market")
            if tid not in self.token_meta:
                return
            meta = self.token_meta[tid]
            self._write(
                self.trades_file,
                {
                    "event_type": "trade",
                    "ts": m.get("timestamp"),
                    "observed_at": int(time.time()),
                    "token_id": tid,
                    "condition_id": meta["condition_id"],
                    "slug": meta["slug"],
                    "taker_side": m.get("side"),
                    "price": _to_str(m.get("price")),
                    "size": _to_str(m.get("size")),
                },
            )
            self.trades_count += 1


def open_jsonl_appendable(path: str):
    """Repair any partial trailing line, then open ``path`` for line-buffered append."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    truncated = repair_jsonl_tail(path)
    if truncated:
        print(f"Repaired {truncated} bytes of partial tail in {path}")
    return open(path, "a", buffering=1)


async def run_recorder(
    token_meta: dict[str, dict],
    book_path: str,
    trades_path: str,
    stop_event: asyncio.Event,
    status_interval: float = 120.0,
) -> None:
    """Run the WS recorder until ``stop_event`` is set. Used by the pipeline.

    Closes the output files on exit and prints a final summary.
    """
    trades_file = open_jsonl_appendable(trades_path)
    book_file = open_jsonl_appendable(book_path)
    print(f"Trades -> {trades_path}")
    print(f"Book   -> {book_path}")

    recorder = Recorder(token_meta, trades_file, book_file)
    listener = AsyncWebSocketListenQuotes(
        token_ids=list(token_meta.keys()),
        channel_type="market",
        handle_message=recorder.handle,
    )

    listener_task = asyncio.create_task(listener.run(), name="ws-listener")
    status_task = asyncio.create_task(
        _status_loop(recorder, stop_event, status_interval), name="ws-status"
    )

    try:
        await stop_event.wait()
    finally:
        await listener.stop()
        for t in (listener_task, status_task):
            t.cancel()
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        trades_file.close()
        book_file.close()
        print(
            f"Wrote {recorder.trades_count} trades, "
            f"{recorder.book_count} book updates."
        )


async def run(args):
    cids = read_condition_ids(args.condition_ids_file)
    print(f"Resolving {len(cids)} condition_ids via Gamma...")
    token_meta = resolve_yes_tokens(cids)
    print(f"Subscribing to {len(token_meta)} YES tokens.")

    if args.book_output or args.trades_output:
        if not args.book_output or not args.trades_output:
            raise SystemExit("Both --book-output and --trades-output must be provided together.")
        book_path = os.path.abspath(args.book_output)
        trades_path = os.path.abspath(args.trades_output)
    else:
        os.makedirs(args.output_dir, exist_ok=True)
        stamp = args.run_name or datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        trades_path = os.path.join(args.output_dir, f"trades_{stamp}.jsonl")
        book_path = os.path.join(args.output_dir, f"book_{stamp}.jsonl")

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _request_stop():
        if not stop_event.is_set():
            print("\nStopping...")
            stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_stop)
        except NotImplementedError:
            pass

    await run_recorder(token_meta, book_path, trades_path, stop_event)


async def _status_loop(
    recorder: Recorder, stop_event: asyncio.Event, interval: float
) -> None:
    last_t = recorder.trades_count
    last_b = recorder.book_count
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass
        dt = recorder.trades_count - last_t
        db = recorder.book_count - last_b
        last_t, last_b = recorder.trades_count, recorder.book_count
        now_chi = datetime.now(CHICAGO_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
        print(
            f"[{now_chi}] +{dt} trades (+{db} book updates) "
            f"[total {recorder.trades_count}/{recorder.book_count}]"
        )


def main():
    args = parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
