import pandas as pd

from downloader import get_path


def load_binance_kline(symbol: str, freq: str, date: str) -> pd.DataFrame:
    path = get_path(symbol, freq, date)

    # 定义小写列名
    col_names = [
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "number_of_trades",
        "buy_base_volume", "buy_quote_volume", "ignore"
    ]

    # 读取数据文件
    df = pd.read_csv(path, header=None, names=col_names)

    # 将时间戳转换为可读日期格式
    df["open_time"] = pd.to_datetime(df["open_time"], unit='ms')
    df["close_time"] = pd.to_datetime(df["close_time"], unit='ms')

    # 计算卖出量（总量 - 买入量）
    df["sell_base_volume"] = df["volume"] - df["buy_base_volume"]
    df["sell_quote_volume"] = df["quote_asset_volume"] - df["buy_quote_volume"]

    return df


if __name__ == "__main__":
    import downloader

    downloader.download_binance_kline("BTCUSDT", "1m", "2024-11-10")
    df = load_binance_kline("BTCUSDT", "1m", "2024-11-10")
    print(df.head(5).to_string())
