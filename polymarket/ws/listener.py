import asyncio
import inspect
import json
from decimal import Decimal

import websockets


MARKET_CHANNEL = "market"
USER_CHANNEL = "user"


def _best(levels, side):
    """Return the best price from a list of orderbook levels."""
    if not levels:
        return None
    prices = [Decimal(l["price"]) for l in levels if "price" in l]
    if not prices:
        return None
    return max(prices) if side == "bid" else min(prices)


class AsyncWebSocketListenQuotes:
    """Polymarket CLOB WebSocket listener with auto-reconnect."""

    def __init__(self, client=None, token_ids=None, channel_type="market", auth=None, handle_message=print):
        self.client = client
        self.base_url = "wss://ws-subscriptions-clob.polymarket.com"
        self.channel_type = channel_type
        self.token_ids = list(token_ids or [])
        self.handle_message = handle_message
        self.url = f"{self.base_url}/ws/{self.channel_type}"
        self.auth = auth or {}
        self._stop_event = asyncio.Event()
        self._ids_lock = asyncio.Lock()
        self._ws = None

    async def update_tokens(self, new_token_ids):
        new_list = list(new_token_ids)
        async with self._ids_lock:
            if set(new_list) == set(self.token_ids):
                return
            self.token_ids = new_list
            if self._ws is not None:
                try:
                    await self._ws.close(code=1001, reason="resubscribe")
                except Exception:
                    pass

    async def connect(self):
        """Main connection loop with auto-reconnect."""
        while not self._stop_event.is_set():
            try:
                async with websockets.connect(
                    self.url, ping_interval=None, ping_timeout=None
                ) as ws:
                    self._ws = ws

                    async with self._ids_lock:
                        ids = list(self.token_ids)

                    if self.channel_type == "market":
                        payload = {"assets_ids": ids, "type": self.channel_type}
                    elif self.channel_type == "user":
                        payload = {
                            "markets": ids,
                            "type": self.channel_type,
                            "auth": self.auth,
                        }
                    else:
                        raise ValueError(f"Invalid channel type: {self.channel_type}")

                    await ws.send(json.dumps(payload))
                    asyncio.create_task(self._ping(ws))

                    async for msg in ws:
                        await self.on_message(msg)

            except Exception as e:
                if self._stop_event.is_set():
                    break
                print(f"Connection error: {e}. Reconnecting in 5s...")
                await asyncio.sleep(5)
            finally:
                self._ws = None

    async def on_message(self, message):
        try:
            if inspect.iscoroutinefunction(self.handle_message):
                await self.handle_message(message)
            else:
                self.handle_message(message)
        except Exception as e:
            print("Error in handle_message:", self.channel_type)
            print(e)

    async def _ping(self, ws):
        try:
            while not self._stop_event.is_set():
                await ws.send(json.dumps({"type": "PING"}))
                await asyncio.sleep(10)
        except Exception:
            pass

    async def stop(self):
        self._stop_event.set()
        if self._ws is not None:
            try:
                await self._ws.close(code=1000, reason="stop")
            except Exception:
                pass

    async def run(self):
        await self.connect()


class SharedStates:
    """Lightweight quote-tracking state for WebSocket market data."""

    def __init__(self, market_info, quote_file=None):
        self.marketInfo = market_info
        self.quotes = {m: {"bid": 0.0, "ask": 1.0, "time": 0.0} for m in self.marketInfo}
        self.token_to_market = {}
        for m, (_, (yes_id, no_id, _)) in self.marketInfo.items():
            self.token_to_market[yes_id] = m
        self.lock = asyncio.Lock()
        self.quote_file = quote_file

    async def _set_quote_unlocked(self, market, bid, ask, ts):
        if market not in self.quotes:
            self.quotes[market] = {"bid": 0.0, "ask": 1.0, "time": 0.0}
        if bid is not None:
            self.quotes[market]["bid"] = float(bid)
        if ask is not None:
            self.quotes[market]["ask"] = float(ask)
        self.quotes[market]["time"] = ts

    async def record_quote_update(self, market, bid, ask, ts):
        if self.quote_file is not None:
            with open(self.quote_file, "a") as f:
                f.write(f"{market[0]},{market[1]},{market[2]},{bid},{ask},{ts}\n")

    async def update_quotes(self, market, bid, ask, ts):
        async with self.lock:
            await self._set_quote_unlocked(market, bid, ask, ts)
            await self.record_quote_update(market, bid, ask, ts)

    async def handle_single_message(self, message):
        et = message.get("event_type")
        if et == "book":
            tid = message.get("asset_id") or message.get("market")
            market = self.token_to_market.get(tid)
            if not market:
                return
            bids = message.get("bids") or []
            asks = message.get("asks") or []
            bb = _best(bids, "bid")
            ba = _best(asks, "ask")
            await self.update_quotes(
                market,
                float(bb) if bb is not None else None,
                float(ba) if ba is not None else None,
                message.get("timestamp"),
            )
        elif et == "price_change":
            for pc in message.get("price_changes", []):
                tid = pc.get("asset_id") or pc.get("market")
                market = self.token_to_market.get(tid)
                if not market:
                    continue
                bb = pc.get("best_bid")
                ba = pc.get("best_ask")
                await self.update_quotes(
                    market,
                    float(bb) if bb is not None else None,
                    float(ba) if ba is not None else None,
                    message.get("timestamp"),
                )

    async def handle_messages(self, messages):
        if not messages or messages == "PONG":
            return
        try:
            data = json.loads(messages)
        except json.JSONDecodeError:
            return

        if isinstance(data, list):
            for msg in data:
                await self.handle_single_message(msg)
        else:
            await self.handle_single_message(data)

    async def replace_markets(self, new_marketInfo: dict):
        async with self.lock:
            self.marketInfo = new_marketInfo
            self.token_to_market = {}
            for m, (_, (yes_id, no_id, _)) in self.marketInfo.items():
                self.token_to_market[yes_id] = m
            self.quotes = {
                m: self.quotes.get(m, {"bid": 0.0, "ask": 1.0, "time": 0.0})
                for m in self.marketInfo
            }


def yes_token_ids(market_info: dict) -> list:
    return [v[1][0] for v in market_info.values()]


async def listen_polymarket(shared_states: SharedStates, listener: AsyncWebSocketListenQuotes):
    """Run the WebSocket listener with auto-reconnect."""
    while True:
        try:
            async with shared_states.lock:
                token_ids = []
                for key, (symbol, (yes_id, no_id, cid)) in shared_states.marketInfo.items():
                    token_ids.append(yes_id)
            await listener.run()
        except Exception as e:
            print(f"Polymarket listener crashed: {e}, retrying in 5s")
            await asyncio.sleep(5)
