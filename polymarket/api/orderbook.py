import pandas as pd
import requests

from polymarket.api.markets import get_token_ids_for_strike


def get_book_summary(token_id: str) -> dict:
    # Fetch full order book JSON from the CLOB for a single outcome token.
    r = requests.get(
        "https://clob.polymarket.com/book",
        params={"token_id": token_id},
        timeout=15,
    )
    r.raise_for_status()
    return r.json()


def get_best_quote(coin: str, strike, day: str):
    # Return best bid/ask prices for YES and NO tokens for a dated strike market.
    ids = get_token_ids_for_strike(coin, strike, day)
    yes_book = get_book_summary(ids["YES"])
    no_book = get_book_summary(ids["NO"])
    yb, ya = yes_book.get("bids") or [], yes_book.get("asks") or []
    nb, na = no_book.get("bids") or [], no_book.get("asks") or []
    if not yb or not ya or not nb or not na:
        raise ValueError("Incomplete order book (missing bids or asks).")
    return (
        yb[-1]["price"],
        ya[-1]["price"],
        nb[-1]["price"],
        na[-1]["price"],
    )


def get_best_quote_with_id(token_id: str):
    # Best bid and ask for one token, or (None, None) if a side is empty.
    book = get_book_summary(token_id)
    bids, asks = book.get("bids") or [], book.get("asks") or []
    bid = float(bids[-1]["price"]) if bids else None
    ask = float(asks[-1]["price"]) if asks else None
    return bid, ask


def get_history_price(token_id: str, startTs=None, endTs=None, fidelity=1) -> pd.DataFrame:
    # Historical mid prices for a token; params omit None so the API does not get empty strings.
    params = {"market": token_id, "fidelity": fidelity}
    if startTs is not None:
        params["startTs"] = startTs
    if endTs is not None:
        params["endTs"] = endTs
    r = requests.get(
        "https://clob.polymarket.com/prices-history",
        params=params,
        timeout=15,
    )
    r.raise_for_status()
    return pd.DataFrame(r.json()["history"])
