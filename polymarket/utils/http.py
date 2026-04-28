"""HTTP helpers with a small retry loop for Gamma / Data API calls.

This module has no CLI; import :func:`request_with_retry`.
"""

from __future__ import annotations

import random
import time
from typing import Any

import requests

_RETRY_STATUS = {429, 500, 502, 503, 504}


def request_with_retry(
    method: str,
    url: str,
    *,
    max_attempts: int = 4,
    base_backoff: float = 0.5,
    max_backoff: float = 8.0,
    timeout: float = 15.0,
    **kwargs: Any,
) -> requests.Response:
    """Issue ``method`` against ``url``, retrying transient failures.

    Retries on connection errors, timeouts, and HTTP status in
    ``{429, 500, 502, 503, 504}``. Backoff is exponential with jitter,
    starting at ``base_backoff`` and capped at ``max_backoff``. Raises the
    last ``RequestException`` if all attempts fail; raises ``HTTPError`` for
    non-retryable 4xx (other than 429).
    """
    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            r = requests.request(method, url, timeout=timeout, **kwargs)
        except (requests.ConnectionError, requests.Timeout) as e:
            last_exc = e
        else:
            if r.status_code not in _RETRY_STATUS:
                return r
            last_exc = requests.HTTPError(
                f"{r.status_code} from {url}", response=r
            )

        if attempt == max_attempts:
            break
        sleep_for = min(max_backoff, base_backoff * (2 ** (attempt - 1)))
        sleep_for *= 0.5 + random.random()  # jitter in [0.5, 1.5)
        time.sleep(sleep_for)

    assert last_exc is not None
    raise last_exc


def get_with_retry(url: str, **kwargs: Any) -> requests.Response:
    return request_with_retry("GET", url, **kwargs)
