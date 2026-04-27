from datetime import datetime, timedelta

from polymarket.api.orderbook import get_book_summary
from polymarket.api.markets import get_markets_by_slug_keyword


def compute_reward_score(
    token_id, rewards, rewardsMaxSpread, orderPriceMinTickSize, rewardsMinSize
):
    # Score liquidity reward eligibility using depth within max spread around the mid (CLOB token id).
    orderbook = get_book_summary(token_id)
    bids = [float(b["price"]) for b in orderbook["bids"]]
    asks = [float(a["price"]) for a in orderbook["asks"]]
    ask_sizes = [float(a["size"]) for a in orderbook["asks"]]
    bid_sizes = [float(b["size"]) for b in orderbook["bids"]]
    if not bids or not asks:
        return 0.0
    mid_price = (bids[-1] + asks[-1]) / 2
    size = 0.0
    band = rewardsMaxSpread * orderPriceMinTickSize

    if len(bids) < 2 or bids[-2] < mid_price - band:
        return 0.0
    if len(asks) < 2 or asks[-2] > mid_price + band:
        return 0.0

    for i in range(len(bids) - 1, -1, -1):
        if bids[i] < mid_price - band:
            break
        size += bid_sizes[i]
    for i in range(len(asks) - 1, -1, -1):
        if asks[i] > mid_price + band:
            break
        size += ask_sizes[i]

    if size < rewardsMinSize * 10:
        return 0.0
    return rewards / (size + rewardsMinSize)


def rank_reward_scores(top_n: int = 10):
    # Rank open reward markets by proxy score (24h volume × spread) and return the top slice.
    date_earlier = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    date_later = (datetime.now() + timedelta(days=2)).strftime("%Y-%m-%d")

    data = get_markets_by_slug_keyword(
        keyWordList=[" "],
        doNotContainList=["$", "(", ")", "Up or Down"],
        end_date_min=date_later,
        start_date_max=date_earlier,
    )
    data = data[data["orderPriceMinTickSize"] > 0.005]
    data = data[data["bestBid"] > 0.2]
    data = data[data["bestAsk"] < 0.8]
    data = data[data["rewardsMinSize"] < 21]

    data["reward_score"] = data["volume24hr"] * data["spread"]
    data = data.sort_values(by="reward_score", ascending=False)
    return data.head(top_n)
