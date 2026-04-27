"""Market making entry point."""

import asyncio
import os
import signal

import pandas as pd

from polymarket.config import load_config, create_client, get_auth
from polymarket.api.markets import get_markets_by_condition_ids
from polymarket.api.account import get_position_in_market, get_markets_with_position
from polymarket.api.rewards import rank_reward_scores
from polymarket.ws.listener import AsyncWebSocketListenQuotes

from apps.market_making.state import SharedStates


shutdown_event = asyncio.Event()


def _handle_shutdown():
    # Flip the global shutdown flag so the main loop exits cleanly.
    if not shutdown_event.is_set():
        print("Shutdown signal received, stopping gracefully...")
        shutdown_event.set()


def find_best_markets(proxy_address, marketInfoFile=None):
    # Merge existing positions with ranked reward markets and build id maps for the WS layer.
    condition_ids = get_markets_with_position(proxy_address)
    data1 = get_markets_by_condition_ids(condition_ids)
    data1["clearing"] = True
    data2 = rank_reward_scores()
    data2["clearing"] = False
    data = pd.concat([data1, data2], ignore_index=True)
    data = data.drop_duplicates(subset="conditionId")
    data["rewardsMinSize"] = data["rewardsMinSize"].clip(upper=20.0, lower=5.0)

    condition_ids = data["conditionId"].tolist()
    yes_ids = dict(zip(condition_ids, data["YES"].tolist()))
    no_ids = dict(zip(condition_ids, data["NO"].tolist()))
    tick_sizes = {cid: float(ts) for cid, ts in zip(condition_ids, data["orderPriceMinTickSize"].tolist())}
    minShares = {cid: int(ms) for cid, ms in zip(condition_ids, data["rewardsMinSize"].tolist())}
    maxSpreads = {cid: float(ms) for cid, ms in zip(condition_ids, data["rewardsMaxSpread"].tolist())}
    clearing = dict(zip(condition_ids, data["clearing"].tolist()))
    questions = dict(zip(condition_ids, data["question"].tolist()))

    if marketInfoFile:
        out = data[["conditionId", "question", "rewardsMinSize", "rewardsMaxSpread", "orderPriceMinTickSize"]]
        file_exists = os.path.isfile(marketInfoFile)
        out.to_csv(marketInfoFile, mode="a", index=False, header=not file_exists)

    return condition_ids, yes_ids, no_ids, tick_sizes, minShares, maxSpreads, questions, clearing


async def listen_polymarket(state, listener_market, listener_user):
    # Run market and user WebSocket listeners until either stops or errors; then backoff.
    while True:
        if listener_market._stop_event.is_set() or listener_user._stop_event.is_set():
            break
        try:
            await asyncio.gather(listener_market.run(), listener_user.run())
        except Exception as e:
            if listener_market._stop_event.is_set() or listener_user._stop_event.is_set():
                break
            print(f"Polymarket listener crashed: {e}, retrying in 5s")
            await asyncio.sleep(5)
        else:
            if listener_market._stop_event.is_set() or listener_user._stop_event.is_set():
                break
            await asyncio.sleep(5)


async def refresh_markets(
    client, proxy_address, auth, orderbookFile, tradelogFile, quote_log_file, marketInfoFile=None
):
    # Recompute the market universe and rebuild SharedStates plus new WebSocket clients.
    condition_ids, yes_ids, no_ids, tick_sizes, minShares, maxSpreads, questions, clearing = find_best_markets(
        proxy_address, marketInfoFile=marketInfoFile
    )
    new_state = SharedStates(
        client, proxy_address, questions, condition_ids, yes_ids, no_ids,
        tick_sizes, minShares, maxSpreads, clearing, orderbookFile, tradelogFile, quote_log_file,
    )
    await new_state.start_workers()

    new_listener_market = AsyncWebSocketListenQuotes(
        client=client,
        token_ids=yes_ids.values(),
        channel_type="market",
        handle_message=new_state.handle_messages,
    )
    new_listener_user = AsyncWebSocketListenQuotes(
        client=client,
        token_ids=condition_ids,
        channel_type="user",
        auth=auth,
        handle_message=new_state.handle_messages,
    )
    return new_state, new_listener_market, new_listener_user


async def main(
    client, proxy_address, auth, orderbookFile, tradelogFile, quote_log_file, marketInfoFile=None
):
    # Install signal handlers, start state and listeners, and rotate on refresh or shutdown.
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, _handle_shutdown)
    loop.add_signal_handler(signal.SIGTERM, _handle_shutdown)

    condition_ids, yes_ids, no_ids, tick_sizes, minShares, maxSpreads, questions, clearing = find_best_markets(
        proxy_address,
        marketInfoFile=marketInfoFile,
    )

    state = SharedStates(
        client, proxy_address, questions, condition_ids, yes_ids, no_ids,
        tick_sizes, minShares, maxSpreads, clearing, orderbookFile, tradelogFile, quote_log_file,
    )
    await state.start_workers()

    listener_market = AsyncWebSocketListenQuotes(
        client=client,
        token_ids=list(yes_ids.values()),
        channel_type="market",
        handle_message=state.handle_messages,
    )
    listener_user = AsyncWebSocketListenQuotes(
        client=client,
        token_ids=condition_ids,
        channel_type="user",
        auth=auth,
        handle_message=state.handle_messages,
    )

    while not shutdown_event.is_set():
        state._refresh_event.clear()

        listener_task = asyncio.create_task(listen_polymarket(state, listener_market, listener_user))
        refresh_wait = asyncio.create_task(state._refresh_event.wait())
        shutdown_wait = asyncio.create_task(shutdown_event.wait())

        done, pending = await asyncio.wait(
            {refresh_wait, shutdown_wait},
            return_when=asyncio.FIRST_COMPLETED,
        )
        for t in pending:
            t.cancel()

        await asyncio.gather(listener_market.stop(), listener_user.stop())
        await listener_task

        if shutdown_wait in done and shutdown_event.is_set():
            await state.close_all()
            break

        state, listener_market, listener_user = await refresh_markets(
            client, proxy_address, auth, orderbookFile, tradelogFile, quote_log_file,
            marketInfoFile=marketInfoFile,
        )


if __name__ == "__main__":
    api_key, proxy_address = load_config()
    client = create_client(api_key, proxy_address, use_proxy=os.getenv("CLASH_HTTP_PROXY"))
    auth = get_auth(client)

    orderbook_name = "data/orderbook.txt"
    trade_log_name = "data/trade_log.txt"
    quote_log_file = "data/quote_log.txt"
    market_info_file = "data/market_info.csv"

    asyncio.run(
        main(
            client,
            proxy_address,
            auth,
            orderbook_name,
            trade_log_name,
            quote_log_file,
            marketInfoFile=market_info_file,
        )
    )
