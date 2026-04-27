import asyncio
import math

from py_clob_client.clob_types import OrderArgs, OrderType


def _post_order(client, market_id, price, size, side, order_type: OrderType) -> bool:
    # Create and post a signed order of the given type; return True if the CLOB accepts it.
    order_args = OrderArgs(
        price=price,
        size=size,
        side=side,
        token_id=market_id,
    )
    signed_order = client.create_order(order_args)
    try:
        client.post_order(signed_order, order_type)
        return True
    except Exception as e:
        print("Error placing order:", e)
        return False


def FAK_Order(client, market_id, price, size, side) -> bool:
    # Place a fill-and-kill order at the exchange.
    return _post_order(client, market_id, price, size, side, OrderType.FAK)


def GTC_Order(client, market_id, price, size, side) -> bool:
    # Place a good-till-cancelled resting order.
    return _post_order(client, market_id, price, size, side, OrderType.GTC)


async def preprocess_Order(client, market_id, price, size, side, trade_size):
    # Round price/size for the side, then submit a FAK in a worker thread.
    if side == "BUY":
        price = math.ceil(price * 100) / 100
        size = math.ceil(trade_size / price)
        if price >= 1.0:
            price = 0.99
    elif side == "SELL":
        price = math.floor(price * 100) / 100
        size = math.floor(size)
        if price <= 0.0:
            price = 0.01

    try:
        await asyncio.to_thread(FAK_Order, client, market_id, price, size, side)
    except Exception as e:
        print("Error placing order:", e)
