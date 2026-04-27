"""Copy-trading bot: monitors a wallet and mirrors its trades at reduced size."""

import math
import time

from py_clob_client.clob_types import OrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY, SELL

from polymarket.config import load_config, create_client
from polymarket.api.account import fetch_wallet_activity, get_position_in_market


# Target wallet to copy
WATCH_WALLET = "0xca85f4b9e472b542e1df039594eeaebb6d466bf2"


def run(client, proxy_address, watch_wallet=WATCH_WALLET):
    # Poll the watch wallet for trades and mirror BUY/SELL FAK orders at scaled size.
    seen = set()
    while True:
        try:
            trades = fetch_wallet_activity(watch_wallet, limit=100)
        except Exception as e:
            print("Error fetching trades:", e)
            time.sleep(2)
            continue

        trades.sort(key=lambda x: x.get("timestamp", 0))
        for t in trades:
            tx_id = t.get("transactionHash") + "_" + str(t.get("timestamp"))
            if tx_id in seen:
                continue
            seen.add(tx_id)

            side = t.get("side", "").upper()
            market_id = t.get("asset")
            condition_id = t.get("conditionId")
            size = t.get("size")
            price = t.get("price")

            if side == "BUY":
                if price > 0.92 or price < 0.03:
                    continue
                size *= 0.25
                if size * price > 20:
                    size = math.ceil(20 / price)
                if size * price < 2:
                    size = math.ceil(2 / price)
                size = max(2, int(size))
                price = 0.99
                order_args = OrderArgs(
                    price=price,
                    size=size,
                    side=BUY,
                    token_id=market_id,
                )
                signed_order = client.create_order(order_args)
                try:
                    client.post_order(signed_order, OrderType.FAK)
                    print("Order placed:", price, side, size)
                except Exception as e:
                    print("Error placing order:", e)
            else:
                try:
                    balance_data = get_position_in_market(proxy_address, condition_id)
                except Exception as e:
                    print("Error fetching balance:", e)
                    continue
                pos_yes, pos_no = balance_data
                balance = max(pos_yes, pos_no)
                if balance < 1:
                    continue
                price = 0.01
                size = math.floor(balance)
                order_args = OrderArgs(
                    price=price,
                    size=size,
                    side=SELL,
                    token_id=market_id,
                )
                signed_order = client.create_order(order_args)
                try:
                    client.post_order(signed_order, OrderType.FAK)
                    print("Sell order placed:", price, size)
                except Exception as e:
                    print("Error placing sell order:", e)

        time.sleep(2)


if __name__ == "__main__":
    api_key, proxy_address = load_config()
    client = create_client(api_key, proxy_address)
    run(client, proxy_address)
