import json
from typing import Optional

import pandas as pd
import requests


DATA_API = "https://data-api.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"
MAX_HOLDERS_LIMIT = 20


def _parse_clob_token_ids(raw_ids) -> list[str]:
    if raw_ids is None:
        return []
    if isinstance(raw_ids, list):
        return [str(x).strip() for x in raw_ids if str(x).strip()]
    if isinstance(raw_ids, str):
        s = raw_ids.strip()
        if not s:
            return []
        try:
            parsed = json.loads(s)
            if isinstance(parsed, list):
                return [str(x).strip() for x in parsed if str(x).strip()]
        except json.JSONDecodeError:
            pass
        return [x.strip().strip('"').strip("'") for x in s.split(",") if x.strip()]
    return []


def _to_api_market_param(condition_ids: list[str]) -> str:
    ids = [str(x).strip() for x in condition_ids if str(x).strip()]
    if not ids:
        raise ValueError("condition_ids must contain at least one condition id")
    return ",".join(ids)


def _normalize_side(label: Optional[str]) -> Optional[str]:
    if not label:
        return None
    val = str(label).strip().upper()
    if val in ("YES", "NO"):
        return val
    return None


def _get_token_side_map(condition_ids: list[str], timeout: float = 10.0) -> dict:
    r = requests.get(
        f"{GAMMA_API}/markets",
        params={"condition_ids": _to_api_market_param(condition_ids)},
        timeout=timeout,
    )
    r.raise_for_status()
    data = r.json()
    markets = (
        data if isinstance(data, list) else (data.get("data") or data.get("markets") or [])
    )

    token_map = {}
    for market in markets:
        cid = market.get("conditionId")
        token_ids = _parse_clob_token_ids(market.get("clobTokenIds"))
        labels = market.get("shortOutcomes") or market.get("outcomes") or []
        labels = [str(x).strip().upper() for x in labels]
        for idx, token in enumerate(token_ids):
            side = None
            if idx < len(labels):
                side = _normalize_side(labels[idx])
            if side is None:
                if idx == 0:
                    side = "YES"
                elif idx == 1:
                    side = "NO"
            token_map[token] = {
                "conditionId": cid,
                "side": side,
            }
    return token_map


def get_top_holders_for_markets(
    condition_ids: list[str],
    limit: int = 20,
    min_balance: int = 1,
    timeout: float = 10.0,
) -> pd.DataFrame:
    # Top holders for outcome tokens in one or more markets.
    if limit < 0 or limit > MAX_HOLDERS_LIMIT:
        raise ValueError(f"limit must be between 0 and {MAX_HOLDERS_LIMIT}")

    token_map = _get_token_side_map(condition_ids=condition_ids, timeout=timeout)
    params = {
        "market": _to_api_market_param(condition_ids),
        "limit": int(limit),
        "minBalance": int(min_balance),
    }
    r = requests.get(f"{DATA_API}/holders", params=params, timeout=timeout)
    r.raise_for_status()
    payload = r.json()
    default_condition_id = condition_ids[0] if len(condition_ids) == 1 else None

    rows = []
    for token_block in payload:
        token = token_block.get("token")
        meta = token_map.get(token, {})
        for holder in token_block.get("holders", []):
            outcome_index = holder.get("outcomeIndex")
            side = meta.get("side")
            if side is None:
                if outcome_index == 0:
                    side = "YES"
                elif outcome_index == 1:
                    side = "NO"
            rows.append(
                {
                    "conditionId": meta.get("conditionId") or default_condition_id,
                    "token": token,
                    "side": side,
                    "outcomeIndex": outcome_index,
                    "wallet": holder.get("proxyWallet"),
                    "amount": holder.get("amount"),
                    "name": holder.get("name"),
                    "pseudonym": holder.get("pseudonym"),
                    "bio": holder.get("bio"),
                    "asset": holder.get("asset"),
                    "profileImage": holder.get("profileImage"),
                    "profileImageOptimized": holder.get("profileImageOptimized"),
                }
            )

    if not rows:
        return pd.DataFrame(
            columns=[
                "conditionId",
                "token",
                "side",
                "outcomeIndex",
                "wallet",
                "amount",
                "name",
                "pseudonym",
                "bio",
                "asset",
                "profileImage",
                "profileImageOptimized",
            ]
        )

    out = pd.DataFrame(rows)
    out["amount"] = pd.to_numeric(out["amount"], errors="coerce")
    out = out.sort_values(by=["conditionId", "side", "amount"], ascending=[True, True, False])
    out = out.reset_index(drop=True)
    return out


def get_top_holders_for_market(
    condition_id: str,
    top_n: int = 20,
    min_balance: int = 1,
    timeout: float = 10.0,
) -> pd.DataFrame:
    # Top N holders for one market split by YES and NO side.
    if top_n < 0 or top_n > MAX_HOLDERS_LIMIT:
        raise ValueError(f"top_n must be between 0 and {MAX_HOLDERS_LIMIT}")

    full = get_top_holders_for_markets(
        condition_ids=[condition_id],
        limit=top_n,
        min_balance=min_balance,
        timeout=timeout,
    )
    if full.empty:
        return full

    full = full.sort_values(by=["side", "amount"], ascending=[True, False])
    top = full.groupby("side", as_index=False, group_keys=False).head(top_n)
    return top.reset_index(drop=True)


def resolve_condition_id(market_ref: str, timeout: float = 10.0) -> str:
    # Resolve a condition id from either a condition id string or a market slug.
    ref = str(market_ref).strip()
    if ref.startswith("0x") and len(ref) == 66:
        return ref

    r = requests.get(f"{GAMMA_API}/markets/slug/{ref}", timeout=timeout)
    r.raise_for_status()
    data = r.json()
    condition_id = data.get("conditionId")
    if not condition_id:
        raise ValueError(f"Could not resolve conditionId from market_ref={market_ref}")
    return condition_id
