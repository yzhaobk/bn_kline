from typing import Literal

LIMIT = 1000

COL_NAMES = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_asset_volume", "number_of_trades",
    "buy_base_volume", "buy_quote_volume", "ignore"
]

MarketType = Literal["spot", "coin_margin", "usd_margin"]