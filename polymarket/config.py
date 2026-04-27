import json
import os

from py_clob_client.client import ClobClient


CLOB_HOST = "https://clob.polymarket.com"
CHAIN_ID = 137


def load_config(path=None):
    # Load API key and proxy address from config.json next to this package (or path).
    if path is None:
        path = os.path.join(os.path.dirname(__file__), "..", "config.json")
    with open(path, "r") as f:
        cfg = json.load(f)
    return cfg["API_KEY"], cfg["POLYMARKET_PROXY_ADDRESS"]


def create_client(api_key, proxy_address, signature_type=1, use_proxy=None):
    # Build an authenticated ClobClient; optionally route HTTP via use_proxy.
    if use_proxy:
        _patch_clob_proxy(use_proxy)

    client = ClobClient(
        CLOB_HOST,
        key=api_key,
        chain_id=CHAIN_ID,
        signature_type=signature_type,
        funder=proxy_address,
    )
    client.set_api_creds(client.create_or_derive_api_creds())
    return client


def get_auth(client):
    # Credentials payload for authenticated WebSocket user channels.
    creds = client.create_or_derive_api_creds()
    return {
        "apiKey": creds.api_key,
        "secret": creds.api_secret,
        "passphrase": creds.api_passphrase,
    }


def _patch_clob_proxy(proxy_url):
    # Replace py_clob_client HTTP helpers with a Session that uses the given proxy URL.
    import requests as _requests
    from py_clob_client.http_helpers import helpers as clob_helpers
    from py_clob_client.exceptions import PolyApiException

    for var in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
        os.environ.pop(var, None)

    session = _requests.Session()
    session.trust_env = False
    session.proxies = {"http": proxy_url, "https": proxy_url}

    def _request_via_proxy(endpoint, method, headers=None, data=None):
        try:
            headers = clob_helpers.overloadHeaders(method, headers)
            resp = session.request(
                method=method,
                url=endpoint,
                headers=headers,
                json=data if data else None,
            )
            if resp.status_code != 200:
                raise PolyApiException(resp)
            try:
                return resp.json()
            except _requests.JSONDecodeError:
                return resp.text
        except _requests.RequestException:
            raise PolyApiException(error_msg="Request exception!")

    clob_helpers.request = _request_via_proxy
