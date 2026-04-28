"""Parsers for Gamma JSON-string-encoded list fields like ``clobTokenIds``.

This module has no CLI; import the helpers below.
"""

from __future__ import annotations

import json
from typing import Any


def parse_string_list(raw: Any) -> list[str]:
    """Coerce a Gamma list field to ``list[str]``.

    Gamma sometimes returns the field as a JSON-encoded string (e.g.
    ``'["a", "b"]'``) and sometimes as an actual list. Anything else (None,
    malformed JSON) yields ``[]``.
    """
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(x) for x in raw]
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return []
        if isinstance(parsed, list):
            return [str(x) for x in parsed]
    return []


def yes_token_id(clob_token_ids: Any, outcomes: Any) -> str | None:
    """Return the YES leg's CLOB token id, or ``None`` if it can't be verified.

    Polymarket binary markets list outcomes ``["Yes", "No"]`` (case-insensitive)
    and ``clobTokenIds`` in the matching order. We refuse to guess when
    outcomes can't be parsed or the YES leg isn't in the expected position.
    """
    ids = parse_string_list(clob_token_ids)
    labels = [s.upper() for s in parse_string_list(outcomes)]
    if not ids or not labels or len(ids) != len(labels):
        return None
    try:
        idx = labels.index("YES")
    except ValueError:
        return None
    token = ids[idx]
    return token or None
