"""Market-making computation functions: VWAP, fair price, spread, quotes."""


def compute_microprice(orderbook):
    # Volume-weighted microprice at the top of book using best bid/ask sizes.
    best_bid = orderbook["best_bid"]
    best_ask = orderbook["best_ask"]

    bid_size = orderbook["bids"][best_bid]
    ask_size = orderbook["asks"][best_ask]

    best_bid = float(best_bid)
    best_ask = float(best_ask)

    return (best_bid * ask_size + best_ask * bid_size) / (bid_size + ask_size)


def compute_imbalance(orderbook):
    # Order-book imbalance from bid vs ask size at the touch.
    best_bid = orderbook["best_bid"]
    best_ask = orderbook["best_ask"]

    bid_size = orderbook["bids"][best_bid]
    ask_size = orderbook["asks"][best_ask]

    return (bid_size - ask_size) / (bid_size + ask_size)


def _top_two_excluding_self(book, quote_price, quote_size, min_other=0.001):
    # First two price levels on one side after subtracting our quote size at quote_price.
    first_price = first_size = None
    second_price = second_size = None

    for price, total_size in book.items():
        if quote_price is not None and quote_size is not None and price == quote_price:
            other_size = total_size - quote_size
        else:
            other_size = total_size

        if other_size <= min_other:
            continue

        if first_price is None:
            first_price, first_size = price, other_size
        else:
            second_price, second_size = price, other_size
            break

    return first_price, first_size, second_price, second_size


def find_first_2_best(bids, asks, quotes):
    # Top two bid/ask levels excluding our posted quote sizes.
    quote_bid = quotes["bid_price"]
    quote_bid_size = quotes["bid_size"]
    quote_ask = quotes["ask_price"]
    quote_ask_size = quotes["ask_size"]

    first_bid, first_bid_size, second_bid, second_bid_size = _top_two_excluding_self(
        bids, quote_bid, quote_bid_size
    )
    first_ask, first_ask_size, second_ask, second_ask_size = _top_two_excluding_self(
        asks, quote_ask, quote_ask_size
    )

    return (
        first_bid, first_bid_size, second_bid, second_bid_size,
        first_ask, first_ask_size, second_ask, second_ask_size,
    )


def compute_quotes(bids, asks, quote, inventoryDiff, tick_size):
    # Inventory-aware bid/ask candidates from depth and spread regimes.
    (
        first_bid, first_bid_size, second_bid, second_bid_size,
        first_ask, first_ask_size, second_ask, second_ask_size,
    ) = find_first_2_best(bids, asks, quote)
    spread = first_ask - first_bid

    if inventoryDiff >= 5.0:
        if second_ask is not None:
            ask_quote = (first_ask * first_ask_size * 5 + second_ask * second_ask_size) / (
                first_ask_size * 5 + second_ask_size
            )
        else:
            ask_quote = first_ask
        bid_quote = None
    elif inventoryDiff <= -5.0:
        if second_bid is not None:
            bid_quote = (first_bid * first_bid_size * 5 + second_bid * second_bid_size) / (
                first_bid_size * 5 + second_bid_size
            )
        else:
            bid_quote = first_bid
        ask_quote = None
    else:
        if second_bid is not None:
            bid_quote = (first_bid * first_bid_size * 2 + second_bid * second_bid_size) / (
                first_bid_size * 2 + second_bid_size
            )
        else:
            bid_quote = first_bid
        if second_ask is not None:
            ask_quote = (first_ask * first_ask_size * 2 + second_ask * second_ask_size) / (
                first_ask_size * 2 + second_ask_size
            )
        else:
            ask_quote = first_ask

    if spread > 0.06:
        if bid_quote is not None:
            bid_quote += tick_size
        if ask_quote is not None:
            ask_quote -= tick_size

    if spread < 0.04 and abs(inventoryDiff) < 5.0:
        bid_quote = None
        ask_quote = None

    if bid_quote is not None:
        bid_quote = round(round(bid_quote / tick_size) * tick_size, 3)
    if ask_quote is not None:
        ask_quote = round(round(ask_quote / tick_size) * tick_size, 3)

    return bid_quote, ask_quote


def compute_VWAP(book, quote_price, quote_size, VOL, min_other=0.001):
    # VWAP up to VOL shares on one side, excluding our quote size at the touch.
    total_size = 0
    total_price = 0
    best_price = None
    for price, size in book.items():
        if quote_price is not None and quote_size is not None and price == quote_price:
            other_size = size - quote_size
        else:
            other_size = size

        if other_size <= min_other:
            continue

        if best_price is None:
            best_price = price

        if total_size + other_size >= VOL:
            total_price += price * (VOL - total_size)
            total_size = VOL
            break
        else:
            total_size += other_size
            total_price += price * other_size

    return total_price / total_size if total_size > 0 else None, total_size


def compute_fair_price_VAMP(bids, asks, quote, VOL=100):
    # Mid of bid and ask VWAPs when both sides fill at least ~90% of VOL excluding our quotes.
    bid_VWAP, bid_total = compute_VWAP(bids, quote["bid_price"], quote["bid_size"], VOL)
    ask_VWAP, ask_total = compute_VWAP(asks, quote["ask_price"], quote["ask_size"], VOL)

    if bid_total is None or ask_total is None or bid_total < VOL * 0.9 or ask_total < VOL * 0.9:
        return None

    return (bid_VWAP + ask_VWAP) / 2


def adjust_inventory(fair_price, inventoryDiff, tick_size, beta=1.0):
    # Shift fair value down when long YES / up when short (inventory skew).
    return fair_price - beta * inventoryDiff * tick_size


def compute_spread_VAMP(bids, asks, quote, VOL=100):
    # Half the bid–ask VWAP width when depth on both sides is sufficient.
    bid_VWAP, bid_total = compute_VWAP(bids, quote["bid_price"], quote["bid_size"], VOL)
    ask_VWAP, ask_total = compute_VWAP(asks, quote["ask_price"], quote["ask_size"], VOL)

    if bid_total is None or ask_total is None or bid_total < VOL * 0.9 or ask_total < VOL * 0.9:
        return None

    return (ask_VWAP - bid_VWAP) / 2
