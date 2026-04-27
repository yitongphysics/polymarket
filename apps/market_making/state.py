"""Heavy SharedStates for market making: orderbook, inventory, queues, workers."""

import asyncio
import datetime
import json
import math
import time
from collections import deque
from itertools import islice

from sortedcontainers import SortedDict
from py_clob_client.order_builder.constants import BUY, SELL

from polymarket.api.account import get_position_in_market
from polymarket.execution.orders import GTC_Order
from apps.market_making.functions import (
    compute_fair_price_VAMP,
    compute_spread_VAMP,
    adjust_inventory,
)


class SharedStates:
    # Per-market order books, quotes, workers, and logs for the market-making loop.
    def __init__(
        self,
        client,
        proxy_address,
        questions,
        condition_ids,
        yes_ids,
        no_ids,
        tick_sizes,
        minShares,
        maxSpreads,
        clearing,
        orderbook_name=None,
        trade_log_name=None,
        quote_log_name=None,
    ):
        # Allocate per-market structures, locks, queues, and log paths.
        self.client = client
        self.proxy_address = proxy_address
        self.condition_ids = condition_ids
        self.questions = questions
        self.yes_ids = yes_ids
        self.yes_set = set(yes_ids.values())
        self.no_ids = no_ids
        self.no_set = set(no_ids.values())

        self.tick_size = tick_sizes
        self.max_spread = maxSpreads
        self.minShares = minShares

        self.orderbook = {
            cid: {
                "bids": SortedDict(),
                "asks": SortedDict(),
                "best_bid": None,
                "best_ask": None,
                "time_book": None,
            }
            for cid in self.condition_ids
        }
        self.clearing = clearing

        self.quote = {
            cid: {"bid_price": None, "ask_price": None, "bid_size": 0.0, "ask_size": 0.0}
            for cid in self.condition_ids
        }
        self.inventory = {cid: {"Yes": 0, "No": 0} for cid in self.condition_ids}
        self.locks = {cid: asyncio.Lock() for cid in self.condition_ids}

        self.orderbook_file = orderbook_name
        self.quote_log_file = quote_log_name
        self.trade_log_file = trade_log_name
        self.depth = 5

        self.queues = {cid: asyncio.Queue() for cid in self.condition_ids}
        self.worker_tasks = []
        self._stop_workers = asyncio.Event()
        self.wait_for_order = {cid: time.time() + 0.0 for cid in self.condition_ids}
        self.msg_queue = {cid: deque(maxlen=1000) for cid in self.condition_ids}

        self._refresh_event = asyncio.Event()
        self.clock_time = time.time()
        self._clock_task = None
        self.stop_clock = asyncio.Event()
        self.canceled = False
        self.born = time.time()

    # ---- Queue / Worker Management ----

    async def _clock_loop(self):
        # Stop the bot near the hour after startup (schedules close_all).
        while not self.stop_clock.is_set():
            await asyncio.sleep(1.0)
            self.clock_time = datetime.datetime.now()
            if self.clock_time.minute == 59 and time.time() - self.born > 120.0:
                self.stop_clock.set()
                break
        await self.close_all()

    async def start_workers(self):
        # Load inventory and spawn the per-market worker and clock tasks.
        await self.init_inventory()
        self._clock_task = asyncio.create_task(self._clock_loop())
        for cid in self.condition_ids:
            task = asyncio.create_task(self._worker_loop(cid))
            self.worker_tasks.append(task)

    async def _worker_loop(self, condition_id):
        # Drain the queue for one market and run inventory-driven cancel/requote when idle.
        q = self.queues[condition_id]
        while True:
            msg = await q.get()
            if msg is None or self._stop_workers.is_set():
                q.task_done()
                break

            try:
                await self._process_condition_message(condition_id, msg)
            except Exception as e:
                print(f"Error processing message for {condition_id}: {e}")
            finally:
                q.task_done()

            inventoryDiff = self.inventory[condition_id]["Yes"] - self.inventory[condition_id]["No"]
            if abs(inventoryDiff) > 2.5 or not self.clearing[condition_id]:
                while q.empty() and not self._stop_workers.is_set():
                    if time.time() > self.wait_for_order[condition_id]:
                        await self.cancel_or_retreat(condition_id)
                        break
                    await asyncio.sleep(0.1)

    async def _process_condition_message(self, condition_id, message):
        # Dispatch WebSocket payloads to order book, trade, or tick-size handlers.
        et = message.get("event_type")
        if et == "price_change":
            messages = message.get("price_changes")
            time_book = message.get("timestamp")
            for msg in messages:
                asset_id = msg.get("asset_id")
                if asset_id not in self.yes_set:
                    continue
                price = msg.get("price")
                size = msg.get("size")
                side = msg.get("side")
                best_bid = msg.get("best_bid")
                best_ask = msg.get("best_ask")
                await self.edit_orderbook(condition_id, price, size, side, best_bid, best_ask, time_book)

        elif et == "book":
            asset_id = message.get("asset_id")
            if asset_id not in self.yes_set:
                return
            bids = message.get("bids") or []
            asks = message.get("asks") or []
            time_book = message.get("timestamp")
            await self.new_orderbook(condition_id, bids, asks, time_book)

        elif et == "tick_size_change":
            asset_id = message.get("asset_id")
            if asset_id not in self.yes_set:
                return
            tick_size = float(message.get("new_tick_size"))
            await self.update_tick_size(condition_id, tick_size)

        elif et == "trade":
            if condition_id not in self.condition_ids:
                return
            await self.cancel_market_orders(condition_id=condition_id)
            self.wait_for_order[condition_id] = time.time() + float("inf")
            if message.get("status") == "MATCHED":
                time_trade = float(message.get("match_time"))
                price = message.get("price")
                size = message.get("size")
                side = message.get("side")
                outcome = message.get("outcome")
                await self.write_market_trades(condition_id, price, outcome, size, side, time_trade, is_mine=1)
            if message.get("status") == "CONFIRMED":
                await self.update_inventory(condition_id)

        elif et == "last_trade_price":
            price = message.get("price")
            size = message.get("size")
            side = message.get("side")
            time_trade = message.get("timestamp")
            asset_id = message.get("asset_id")
            if asset_id in self.yes_set:
                outcome = "Yes"
            elif asset_id in self.no_set:
                outcome = "No"
            else:
                return
            await self.cancel_market_orders(condition_id=condition_id)
            await self.write_market_trades(condition_id, price, outcome, size, side, time_trade)
            self.wait_for_order[condition_id] = max(
                self.wait_for_order[condition_id], time.time() + 60.0
            )

    # ---- Trade Functions ----

    async def cancel_market_orders(self, condition_id, asset_id=None):
        # Cancel all or one-sided orders and clear local quote state on success.
        try:
            time_quote = time.time()
            if asset_id is not None:
                self.client.cancel_market_orders(asset_id=asset_id)
                if asset_id in self.yes_set:
                    self.quote[condition_id]["bid_price"] = None
                    self.quote[condition_id]["bid_size"] = 0.0
                elif asset_id in self.no_set:
                    self.quote[condition_id]["ask_price"] = None
                    self.quote[condition_id]["ask_size"] = 0.0
            else:
                self.client.cancel_market_orders(market=condition_id)
                self.quote[condition_id]["bid_price"] = None
                self.quote[condition_id]["ask_price"] = None
                self.quote[condition_id]["bid_size"] = 0.0
                self.quote[condition_id]["ask_size"] = 0.0
            await self.write_quote_log(condition_id, time_quote)
        except Exception as e:
            print(f"Error canceling market orders: {e}")
            self.wait_for_order[condition_id] = max(
                self.wait_for_order[condition_id], time.time() + 60.0
            )

    async def update_quote(self, condition_id, asset_id, price, size, outcome, side):
        # Post a GTC and mirror filled quote state for YES/NO display conventions.
        size = round(math.floor(size * 100) / 100, 2)
        price = round(price, 3)

        time_quote = time.time()
        order_status = GTC_Order(self.client, asset_id, price, size, side)

        if order_status:
            if outcome == "No":
                price = round(1 - price, 3)

            if (outcome == "Yes" and side == BUY) or (outcome == "No" and side == SELL):
                self.quote[condition_id]["bid_price"] = price
                self.quote[condition_id]["bid_size"] = size
            elif (outcome == "No" and side == BUY) or (outcome == "Yes" and side == SELL):
                self.quote[condition_id]["ask_price"] = price
                self.quote[condition_id]["ask_size"] = size

            await self.write_quote_log(condition_id, time_quote)
            return True
        else:
            await self.update_inventory(condition_id)
        return False

    async def cancel_or_retreat(self, condition_id):
        # Recompute fair/spread from the book and refresh or cancel resting quotes.
        inventoryDiff = self.inventory[condition_id]["Yes"] - self.inventory[condition_id]["No"]
        fair_price = compute_fair_price_VAMP(
            self.orderbook[condition_id]["bids"],
            self.orderbook[condition_id]["asks"],
            self.quote[condition_id],
            5 * self.minShares[condition_id],
        )
        fair_price = adjust_inventory(
            fair_price,
            inventoryDiff / self.minShares[condition_id],
            self.tick_size[condition_id],
        )
        spread = compute_spread_VAMP(
            self.orderbook[condition_id]["bids"],
            self.orderbook[condition_id]["asks"],
            self.quote[condition_id],
            5 * self.minShares[condition_id],
        )

        if fair_price is None or spread is None:
            bid_quote = None
            ask_quote = None
        else:
            bid_quote = fair_price - spread
            ask_quote = fair_price + spread

            bid_quote = round(
                round(bid_quote / self.tick_size[condition_id]) * self.tick_size[condition_id], 3
            )
            ask_quote = round(
                round(ask_quote / self.tick_size[condition_id]) * self.tick_size[condition_id], 3
            )

            if bid_quote > self.orderbook[condition_id]["best_ask"] - 0.5 * self.tick_size[condition_id]:
                bid_quote = round(
                    self.orderbook[condition_id]["best_ask"] - self.tick_size[condition_id], 3
                )
            if ask_quote < self.orderbook[condition_id]["best_bid"] + 0.5 * self.tick_size[condition_id]:
                ask_quote = round(
                    self.orderbook[condition_id]["best_bid"] + self.tick_size[condition_id], 3
                )

            if bid_quote < 0.01:
                bid_quote = None
            if ask_quote > 0.99:
                ask_quote = None

        if bid_quote is None or ask_quote is None:
            if (
                self.quote[condition_id]["bid_price"] is not None
                or self.quote[condition_id]["ask_price"] is not None
            ):
                await self.cancel_market_orders(condition_id=condition_id)
        else:
            if self.quote[condition_id]["bid_price"] is None:
                await self.update_quote(
                    condition_id, self.yes_ids[condition_id], bid_quote, self.minShares[condition_id], "Yes", BUY
                )
            elif bid_quote != self.quote[condition_id]["bid_price"]:
                await self.cancel_market_orders(condition_id=condition_id, asset_id=self.yes_ids[condition_id])
                await self.update_quote(
                    condition_id, self.yes_ids[condition_id], bid_quote, self.minShares[condition_id], "Yes", BUY
                )

            if self.quote[condition_id]["ask_price"] is None:
                await self.update_quote(
                    condition_id,
                    self.no_ids[condition_id],
                    round(1 - ask_quote, 3),
                    self.minShares[condition_id],
                    "No",
                    BUY,
                )
            elif ask_quote != self.quote[condition_id]["ask_price"]:
                await self.cancel_market_orders(condition_id=condition_id, asset_id=self.no_ids[condition_id])
                await self.update_quote(
                    condition_id,
                    self.no_ids[condition_id],
                    round(1 - ask_quote, 3),
                    self.minShares[condition_id],
                    "No",
                    BUY,
                )

    # ---- Orderbook Functions ----

    async def new_orderbook(self, condition_id, bids, asks, time_book):
        # Replace the full YES order book snapshot and log the touch.
        async with self.locks[condition_id]:
            self.orderbook[condition_id]["bids"] = SortedDict(
                lambda p: -p, {float(bid["price"]): float(bid["size"]) for bid in bids}
            )
            self.orderbook[condition_id]["asks"] = SortedDict(
                lambda p: p, {float(ask["price"]): float(ask["size"]) for ask in asks}
            )
            self.orderbook[condition_id]["best_bid"] = float(bids[-1]["price"]) if bids else None
            self.orderbook[condition_id]["best_ask"] = float(asks[-1]["price"]) if asks else None
            self.orderbook[condition_id]["time_book"] = time_book
            await self.write_orderbook_log(condition_id)

    async def edit_orderbook(self, condition_id, price, size, side, best_bid, best_ask, time_book):
        # Apply an incremental price_change update to the sorted book and best bid/ask.
        async with self.locks[condition_id]:
            price = float(price)
            size = float(size)
            best_bid = float(best_bid)
            best_ask = float(best_ask)
            time_book = float(time_book)
            if side == "BUY":
                if price == 0.0:
                    self.orderbook[condition_id]["bids"].pop(price, None)
                else:
                    self.orderbook[condition_id]["bids"][price] = size
            elif side == "SELL":
                if price == 0.0:
                    self.orderbook[condition_id]["asks"].pop(price, None)
                else:
                    self.orderbook[condition_id]["asks"][price] = size
            self.orderbook[condition_id]["best_bid"] = best_bid
            self.orderbook[condition_id]["best_ask"] = best_ask
            self.orderbook[condition_id]["time_book"] = time_book
            await self.write_orderbook_log(condition_id)

    # ---- Tick Size ----

    async def update_tick_size(self, condition_id, tick_size):
        # Update tick size, cancel orders, and pause quoting until inventory refresh.
        async with self.locks[condition_id]:
            self.tick_size[condition_id] = tick_size
            await self.cancel_market_orders(condition_id=condition_id)
            self.wait_for_order[condition_id] = time.time() + float("inf")

    # ---- Inventory ----

    async def init_inventory(self):
        # Pull on-chain positions once per tracked market and set initial quote delays.
        for cid in self.condition_ids:
            pos_yes, pos_no = get_position_in_market(self.proxy_address, cid)
            self.inventory[cid]["Yes"] = pos_yes
            self.inventory[cid]["No"] = pos_no
            self.wait_for_order[cid] = time.time() + 10.0

    async def update_inventory(self, condition_id):
        # Poll positions until inventory changes or retries exhaust, then allow quoting.
        async with self.locks[condition_id]:
            t1 = time.time()
            for i in range(11):
                pos_yes, pos_no = get_position_in_market(self.proxy_address, condition_id)
                if (
                    pos_yes != self.inventory[condition_id]["Yes"]
                    or pos_no != self.inventory[condition_id]["No"]
                ):
                    self.inventory[condition_id]["Yes"] = pos_yes
                    self.inventory[condition_id]["No"] = pos_no
                    self.wait_for_order[condition_id] = time.time()
                    break
                await asyncio.sleep(10.0)

    # ---- Message Handling ----

    async def handle_single_message(self, message):
        # Route one WS message to the correct condition queue after asset/market checks.
        et = message.get("event_type")

        if et in ("book", "tick_size_change", "last_trade_price"):
            asset_id = message.get("asset_id")
            if asset_id not in self.yes_set:
                return
            condition_id = message.get("market")
        elif et in ("trade", "price_change"):
            condition_id = message.get("market")
            if condition_id not in self.condition_ids:
                return
        else:
            return

        if condition_id not in self.condition_ids:
            return

        self.msg_queue[condition_id].append(time.time())
        await self.queues[condition_id].put(message)

    async def handle_messages(self, messages):
        # Parse JSON (object or list) from the socket and fan out to per-market queues.
        if not messages or messages == "PONG":
            return
        try:
            data = json.loads(messages)
        except Exception:
            return

        if isinstance(data, list):
            for msg in data:
                await self.handle_single_message(msg)
        else:
            await self.handle_single_message(data)

    # ---- Close ----

    async def close_all(self):
        # Stop workers, cancel all orders, and signal the outer loop to refresh markets.
        self._stop_workers.set()
        for cid in self.condition_ids:
            try:
                self.queues[cid].put_nowait(None)
            except Exception:
                pass
        if self.worker_tasks:
            await asyncio.gather(*self.worker_tasks, return_exceptions=True)
        for cid in self.condition_ids:
            await self.cancel_market_orders(condition_id=cid)
        self._refresh_event.set()
        return True

    # ---- Logging ----

    async def write_market_trades(self, condition_id, price, outcome, size, side, time_trade, is_mine=0):
        # Append a trade line to the trade log if configured.
        if self.trade_log_file:
            with open(self.trade_log_file, "a+") as f:
                f.write(
                    f"{self.questions[condition_id]}+{price}+{outcome}+{size}+{side}+{time_trade}+{is_mine}\n"
                )

    async def write_orderbook_log(self, condition_id):
        # Append top-of-book and depth snippet to the order book log.
        if not self.orderbook_file:
            return

        def serialize(sd):
            return ",".join(f"{p}:{q}" for p, q in islice(sd.items(), 5))

        with open(self.orderbook_file, "a+") as f:
            ob = self.orderbook[condition_id]
            f.write(
                f"{self.questions[condition_id]}+{ob['best_bid']}+{ob['best_ask']}+"
                f"{serialize(ob['bids'])}+{serialize(ob['asks'])}+{ob['time_book']}\n"
            )

    async def write_quote_log(self, condition_id, time_quote):
        # Append bid/ask quote and inventory skew to the quote log.
        if not self.quote_log_file:
            return
        with open(self.quote_log_file, "a+") as f:
            diff = self.inventory[condition_id]["Yes"] - self.inventory[condition_id]["No"]
            q = self.quote[condition_id]
            f.write(
                f"{self.questions[condition_id]}+{q['bid_price']}+{q['ask_price']}+{diff}+{time_quote}\n"
            )
