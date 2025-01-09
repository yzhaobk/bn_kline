from datetime import datetime, timedelta, timezone

from bn_kline.fetcher import fetch_coin_margin_klines, fetch_usd_margin_klines, fetch_spot_klines

if __name__ == '__main__':
    end_time = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_time = int((datetime.now(timezone.utc) - timedelta(days=1)).timestamp() * 1000)

    # Fetch Coin-Margin 合约数据
    symbol_cm = 'BTCUSD_PERP'
    df_cm = fetch_coin_margin_klines(symbol_cm, start_time, end_time)
    if not df_cm.empty:
        print(f"数据范围: {df_cm.iloc[0]['open_time']} 到 {df_cm.iloc[-1]['open_time']}")
        print(df_cm.head().to_string())
    else:
        print("未获取到 Coin-Margin 合约的数据。")

    # Fetch USD-Margin 合约数据
    symbol_um = 'BTCUSDT'
    df_um = fetch_usd_margin_klines(symbol_um, start_time, end_time)
    if not df_um.empty:
        print(f"数据范围: {df_um.iloc[0]['open_time']} 到 {df_um.iloc[-1]['open_time']}")
        print(df_um.head().to_string())
    else:
        print("未获取到 USD-Margin 合约的数据。")

    # Fetch Spot 市场数据
    symbol_spot = 'BTCUSDT'
    df_spot = fetch_spot_klines(symbol_spot, start_time, end_time)
    if not df_spot.empty:
        print(f"数据范围: {df_spot.iloc[0]['open_time']} 到 {df_spot.iloc[-1]['open_time']}")
        print(df_spot.head().to_string())
    else:
        print("未获取到 Spot 市场的数据。")
