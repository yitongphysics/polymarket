from datetime import datetime, date, timedelta

import pandas as pd
import requests


BTC_range = [x * 1000 for x in range(90, 135)]
ETH_range = [x * 100 for x in range(30, 60)]
coin_range = {"BTC": BTC_range, "ETH": ETH_range}


def _yes_no_columns(clob_series):
    # Split Gamma API clobTokenIds string into YES and NO token id columns.
    def split_pair(x):
        a, b = x[2:-2].split('", "')
        return pd.Series({"YES": a, "NO": b})

    return clob_series.apply(split_pair)


def get_markets_by_condition_ids(condition_ids: list) -> pd.DataFrame:
    # Load open markets for the given condition ids, filter spreads, attach YES/NO token ids.
    url = "https://gamma-api.polymarket.com/markets"
    params = {"condition_ids": condition_ids}
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    data = r.json()
    results = pd.DataFrame(data)
    results = results[~results["orderPriceMinTickSize"].isna()]
    results = results[~results["closed"]]

    results = results[
        results["question"]
        .str.lower()
        .apply(lambda q: not any(w in q for w in ["ukraine"]))
    ]

    if "bestBid" not in results.columns:
        results["bestBid"] = 0.0
    if "bestAsk" not in results.columns:
        results["bestAsk"] = 1.0
    results.loc[results["bestBid"].isna(), "bestBid"] = 0.0
    results.loc[results["bestAsk"].isna(), "bestAsk"] = 1.0

    results["spread"] = results["bestAsk"] - results["bestBid"]

    if "clobRewards" not in results.columns:
        results["rewardsDailyRate"] = 0.0
    else:
        results["rewardsDailyRate"] = results["clobRewards"].apply(
            lambda x: x[0]["rewardsDailyRate"]
            if isinstance(x, list) and len(x) > 0
            else 0.0
        )

    results["bestBid"] = results["bestBid"].astype(float)
    results["bestAsk"] = results["bestAsk"].astype(float)

    results = results[results["bestBid"] < 0.95]
    results = results[results["bestAsk"] > 0.05]

    yn = _yes_no_columns(results["clobTokenIds"])
    results["YES"] = yn["YES"]
    results["NO"] = yn["NO"]

    date_later = (datetime.now() + timedelta(days=2)).strftime("%Y-%m-%d")
    results = results[results["endDate"] > date_later]

    return results


def get_markets_by_slug_keyword(
    keyWordList=None,
    doNotContainList=None,
    rewardsSizeMin=None,
    start_date_min=None,
    start_date_max=None,
    end_date_min=None,
    end_date_max=None,
    timeout=10.0,
) -> pd.DataFrame:
    # Page through Gamma markets and filter by question keywords and reward/date bounds.
    if keyWordList is None:
        keyWordList = [" "]
    if doNotContainList is None:
        doNotContainList = []
    print(f"keyWordList: {keyWordList}")
    print(f"doNotContainList: {doNotContainList}")

    url = "https://gamma-api.polymarket.com/markets"
    kws = [kw.lower() for kw in keyWordList]
    excludes = [w.lower() for w in doNotContainList]
    results = []
    limit, offset = 500, 0

    base_params = {
        "limit": limit,
        "closed": False,
        "rewards_min_size": rewardsSizeMin,
        "start_date_min": start_date_min,
        "start_date_max": start_date_max,
        "end_date_min": end_date_min,
        "end_date_max": end_date_max,
    }
    base_params = {k: v for k, v in base_params.items() if v is not None}

    while True:
        params = dict(base_params)
        params["offset"] = offset

        r = requests.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        data = r.json()
        markets = (
            data
            if isinstance(data, list)
            else (data.get("data") or data.get("markets") or [])
        )
        if not markets:
            break

        markets = pd.DataFrame(markets)
        results.append(markets)
        if len(markets) < limit:
            break
        offset += limit

    results = pd.concat(results)
    results = results[
        results["question"].str.lower().apply(lambda q: all(kw in q for kw in kws)) # contains all of the keywords
    ]
    results = results[
        results["question"]
        .str.lower()
        .apply(lambda q: not any(w in q for w in excludes)) # does not contain any of the excludes
    ]
    #results = results[~results["clobRewards"].isna()]
    #results = results[~results["orderPriceMinTickSize"].isna()]
    if "bestBid" not in results.columns:
        results["bestBid"] = 0.0
    if "bestAsk" not in results.columns:
        results["bestAsk"] = 1.0
    results.loc[results["bestBid"].isna(), "bestBid"] = 0.0
    results.loc[results["bestAsk"].isna(), "bestAsk"] = 1.0

    results["spread"] = results["bestAsk"] - results["bestBid"]
    #results["rewardsDailyRate"] = results["clobRewards"].apply(lambda x: x[0]["rewardsDailyRate"])
    results["bestBid"] = results["bestBid"].astype(float)
    results["bestAsk"] = results["bestAsk"].astype(float)

    if "clobTokenIds" not in results.columns:
        results["YES"] = None
        results["NO"] = None
    else:
        yn = _yes_no_columns(results["clobTokenIds"])
        results["YES"] = yn["YES"]
        results["NO"] = yn["NO"]

    return results


def construct_slug(coin: str, expiry: str, strike: int) -> str:
    # Build the Gamma market slug for an "above strike on date" crypto daily contract.
    if coin == "BTC":
        coin_name = "bitcoin"
    elif coin == "ETH":
        coin_name = "ethereum"
    else:
        coin_name = coin.lower()

    expiry = expiry[2:]
    if len(expiry) == 4 and expiry.isdigit():
        month_num = int(expiry[:2])
        day_num = int(expiry[2:])
        try:
            d = date(2025, month_num, day_num)
            expiry = f"{d.strftime('%B').lower()}-{day_num}"
        except ValueError:
            pass

    if coin_name == "bitcoin":
        return f"bitcoin-above-{int(strike) // 1000}k-on-{expiry}"
    return f"{coin_name}-above-{strike}-on-{expiry}"


def get_token_ids_for_strike(coin: str, strike, day: str) -> dict:
    # Resolve YES/NO CLOB token ids (and condition id when present) from slug metadata.
    slug = construct_slug(coin, day, strike)
    url = f"https://gamma-api.polymarket.com/markets/slug/{slug}"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    data = r.json()

    ids = []
    raw_ids = data.get("clobTokenIds") or ""
    if isinstance(raw_ids, str):
        ids = [x.strip() for x in raw_ids.split(",") if x.strip()]
    elif isinstance(raw_ids, list):
        ids = [str(x) for x in raw_ids]

    labels = None
    for key in ("shortOutcomes", "outcomes"):
        if isinstance(data.get(key), list) and len(data[key]) >= 2:
            labels = [str(x).upper() for x in data[key][:2]]
            break
    if labels and len(ids) >= 2:
        return {"slug": slug, labels[0]: ids[0], labels[1]: ids[1], "all": ids}
    return {
        "slug": slug,
        "YES": ids[0][2:-1] if len(ids) > 0 else None,
        "NO": ids[1][1:-2] if len(ids) > 1 else None,
        "conditionId": data.get("conditionId"),
    }


def get_all_active_token_ids(date_strs, ranges=None):
    # Enumerate (coin, strike, date) keys with YES/NO/conditionId for markets that resolve.
    if ranges is None:
        ranges = coin_range
    active_markets = {}
    for coin in ranges:
        for strike in ranges[coin]:
            for date_str in date_strs:
                try:
                    ids = get_token_ids_for_strike(coin, strike, date_str)
                except Exception:
                    continue
                active_markets[(coin, strike, date_str)] = (
                    ids["YES"],
                    ids["NO"],
                    ids["conditionId"],
                )
    return active_markets
