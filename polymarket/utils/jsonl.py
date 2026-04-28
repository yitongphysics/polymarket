"""JSONL helpers: repair a partial trailing line and stream rows.

This module has no CLI; import the helpers below.
"""

from __future__ import annotations

import json
import os
from typing import Iterator


def repair_jsonl_tail(path: str) -> int:
    """Truncate any trailing partial line so the file ends with a complete record.

    Returns the number of bytes truncated (0 if the file was already clean or
    didn't exist). A "partial line" is one whose final byte isn't ``\\n`` *or*
    whose last line doesn't parse as JSON. We only consider the very last line
    so an old corruption doesn't get masked.
    """
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return 0

    with open(path, "rb+") as f:
        f.seek(0, os.SEEK_END)
        end = f.tell()
        # Walk backwards to the start of the last line.
        pos = end
        while pos > 0:
            pos -= 1
            f.seek(pos)
            ch = f.read(1)
            if ch == b"\n" and pos != end - 1:
                # Found newline that isn't the final byte: line starts at pos+1.
                line_start = pos + 1
                break
            if pos == 0:
                line_start = 0
                break
        else:
            line_start = 0

        f.seek(line_start)
        last = f.read()
        if last.endswith(b"\n"):
            try:
                json.loads(last.decode("utf-8", errors="replace"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass
            else:
                return 0
            # Last full line is corrupt.
            f.seek(line_start)
            f.truncate()
            return end - line_start

        # No trailing newline: this is a partial write. Drop it.
        f.seek(line_start)
        f.truncate()
        return end - line_start


def iter_jsonl(path: str) -> Iterator[dict]:
    """Yield each parseable JSON object in ``path``. Silently skips bad lines."""
    if not os.path.exists(path):
        return
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(obj, dict):
                yield obj
