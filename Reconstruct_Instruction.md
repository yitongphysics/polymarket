# Polymarket Trading Toolkit

## Package Structure

```
polymarket/                     # installable package
├── __init__.py                 # re-exports: load_config, create_client, AsyncWebSocketListenQuotes, etc.
├── config.py                   # load_config(), create_client(), get_auth()
├── api/                        # Data ingestion (REST)
│   ├── markets.py              # Market discovery, slug construction, token ID lookup
│   ├── orderbook.py            # Book summaries, best quotes, price history
│   ├── account.py              # Positions, trade logs, wallet activity
│   └── rewards.py              # Reward scoring & market ranking
├── ws/                         # Live listening (WebSocket)
│   └── listener.py             # AsyncWebSocketListenQuotes, SharedStates, listen_polymarket
├── execution/                  # Order execution
│   └── orders.py               # FAK_Order, GTC_Order, preprocess_Order
├── models/                     # Pricing models
│   └── pricing.py              # Black-Scholes, implied prob, PCHIP interpolation
└── utils/                      # Utilities
    └── time.py                 # Polymarket expiry conversions, date helpers

apps/                           # Runnable applications
├── market_making/
│   ├── main.py                 # Market making entry point
│   ├── state.py                # Heavy SharedStates with orderbook, inventory, queues
│   └── functions.py            # VWAP, fair price, spread, imbalance
├── copy_strategy.py            # Copy-trading bot
├── download_history.py         # Historical price data downloader
├── recording/                  # Active market recording
│   ├── download_account_activity.py    # Account activity to DataFrame
│   ├── download_market_activity.py     # Keyword-search markets and snapshot trades
│   └── listen_market_trades.py         # Poll Data API and append trades to JSONL
├── miscellaneous/              # Misc utilities
│   └── find_condition_id.py            # Search markets and print condition IDs
└── insider_trades/             # Research / analysis notebooks
```

## Quick Start

```python
from polymarket.config import load_config, create_client
from polymarket.api.markets import get_markets_by_slug_keyword
from polymarket.api.orderbook import get_best_quote_with_id

# Initialize
api_key, proxy = load_config()
client = create_client(api_key, proxy)

# Find markets
markets = get_markets_by_slug_keyword(keyWordList=["bitcoin"])

# Get quotes
bid, ask = get_best_quote_with_id(token_id)
```

## Running Apps

```bash
# Market making
python -m apps.market_making.main

# Copy trading
python -m apps.copy_strategy

# Download history
python -m apps.download_history

# Market recording
python -m apps.recording.download_account_activity --wallet 0xYourWallet --limit 500
python -m apps.recording.download_market_activity --keyword bitcoin --days 7
python -m apps.recording.listen_market_trades --condition-ids-file apps/recording/condition_ids.txt
```
