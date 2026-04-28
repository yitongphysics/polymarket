"""Read a condition_ids text file.

This module has no CLI; import :func:`read_condition_ids` from
``polymarket.utils.condition_ids``.
"""

from __future__ import annotations


def read_condition_ids(path: str) -> list[str]:
    """Return the non-empty, non-comment first tokens of each line in ``path``.

    Blank lines and lines beginning with ``#`` are skipped. Trailing columns
    after whitespace are ignored. Raises ``SystemExit`` if no ids remain so
    callers fail fast.
    """
    cids: list[str] = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            cids.append(line.split()[0])
    if not cids:
        raise SystemExit(f"No condition_ids found in {path}")
    return cids
