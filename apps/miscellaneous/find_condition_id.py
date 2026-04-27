"""Search Gamma markets by keyword(s) and print question + conditionId for each match."""

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from polymarket.api.markets import get_markets_by_slug_keyword


KEYWORDS = ["iran", "Hormuz"]      # at least one appear in question (OR, case-insensitive)
EXCLUDES: list[str] = []          # substrings to exclude

OUTPUT_PATH = _ROOT / "apps" / "recording" / "condition_ids.txt"


def main():
    markets = get_markets_by_slug_keyword(
        keyWordList=KEYWORDS,
        doNotContainList=EXCLUDES,
    )

    if markets.empty:
        print("No markets matched.")
        return

    for _, row in markets.iterrows():
        print(f"{row.get('conditionId')}\t{row.get('question')}")

    lines = [f"{row.get('conditionId')}\t{row.get('question')}" for _, row in markets.iterrows()]
    OUTPUT_PATH.write_text("\n".join(lines) + "\n")

    print(f"\n{len(markets)} match(es). Wrote {len(lines)} row(s) to {OUTPUT_PATH}.")


if __name__ == "__main__":
    main()
