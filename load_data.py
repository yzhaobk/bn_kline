import pandas as pd

from downloader import get_path


def load_binance_kline(symbol: str, freq: str, date: str) -> pd.DataFrame:
    """
    从本地文件加载 Binance K 线数据
    :param symbol:
    :param freq:
    :param date:
    :return:

    symbol: BTCUDST 交易对符号
    base: BTC
    quote: USDT

    volume: 交易量 按照 base 计量
    quote_asset_volume: 交易量 按照 quote 计量
    buy_base_volume: 买入量 按照 base 计量
    buy_quote_volume: 买入量 按照 quote 计量
    """
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


def read_binance_kiline(symbol: str, freq: str, date: str) -> pd.DataFrame:
    path = get_path(symbol, freq, date)
    if not path.exists():
        downloader.download_binance_kline(symbol, freq, date)
    df = load_binance_kline(symbol, freq, date)
    return df


def read_kline_range(symbol, freq, start_date, end_date):
    """
    读取一段时间的 K 线数据
    """
    date = start_date
    dfs = []
    while date <= end_date:
        print(f"读取 {date} 的数据")
        df = read_binance_kiline(symbol, freq, date)
        dfs.append(df)
        date = (pd.to_datetime(date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")

    return pd.concat(dfs, ignore_index=True)


if __name__ == "__main__":
    import downloader

    downloader.download_binance_kline("BTCUSDT", "1m", "2024-11-10")
    df = load_binance_kline("BTCUSDT", "1m", "2024-11-10")
    print(df.head(5).to_string())
