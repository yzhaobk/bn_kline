import pandas as pd

from downloader import get_path, download_spot_kline, download_cm_kline, download_um_kline
from model import COL_NAMES, MarketType


def infer_timestamp_unit(timestamp):
    """
    根据时间戳的数值范围推断时间单位。

    :param timestamp: 单个时间戳或时间戳序列
    :return: 推断出的时间单位（'s', 'ms', 'us', 'ns'）
    """
    # 如果输入是序列，取其最小值进行判断
    if isinstance(timestamp, pd.Series) or isinstance(timestamp, list):
        timestamp = min(timestamp)

    if timestamp < 1e10:
        raise ValueError("时间戳过小，可能不是有效的Unix时间戳。")
    elif 1e10 <= timestamp < 1e12:
        return 's'  # 秒
    elif 1e12 <= timestamp < 1e15:
        return 'ms'  # 毫秒
    elif 1e15 <= timestamp < 1e18:
        return 'us'  # 微秒
    else:
        return 'ns'  # 纳秒


def load_binance_kline(market_type: MarketType, symbol: str, date: str, freq: str = '1m') -> pd.DataFrame:
    """
    从本地文件加载 Binance K 线数据
    :param market_type:
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
    path = get_path(market_type, symbol, date, freq)

    # 读取数据文件
    df = pd.read_csv(path)
    df.columns = COL_NAMES

    unit = infer_timestamp_unit(df["open_time"].iloc[0])

    # 将时间戳转换为可读日期格式
    df["open_time"] = pd.to_datetime(df["open_time"], unit=unit)
    df["close_time"] = pd.to_datetime(df["close_time"], unit=unit)

    # 计算卖出量（总量 - 买入量）
    df["sell_base_volume"] = df["volume"] - df["buy_base_volume"]
    df["sell_quote_volume"] = df["quote_asset_volume"] - df["buy_quote_volume"]
    return df


def read_binance_kiline(market_type: MarketType, symbol: str, date: str, freq: str = '1m') -> pd.DataFrame:
    path = get_path(market_type, symbol, freq, date)
    if not path.exists():
        match market_type:
            case "spot":
                download_spot_kline(symbol, date, freq)
            case "coin_margin":
                download_cm_kline(symbol, date, freq)
            case "usd_margin":
                download_um_kline(symbol, date, freq)
            case _:
                raise ValueError(f"未知的市场类型: {market_type}")
    df = load_binance_kline(market_type, symbol, date, freq)
    return df


def read_kline_range(market_type: MarketType, symbol: str, start_date, end_date, freq: str = '1m') -> pd.DataFrame:
    """
    读取一段时间的 K 线数据
    """
    date = start_date
    dfs = []
    while date <= end_date:
        print(f"读取 {date} 的数据")
        df = read_binance_kiline(market_type, symbol, freq, date)
        dfs.append(df)
        date = (pd.to_datetime(date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")

    return pd.concat(dfs, ignore_index=True)


if __name__ == "__main__":
    df = read_binance_kiline("spot", "BTCUSDT", "2025-01-01")
    print(df.head(5).to_string())
    df = read_binance_kiline("coin_margin", "BTCUSD_PERP", "2025-01-01")
    print(df.head(5).to_string())
    df = read_binance_kiline("usd_margin", "BTCUSDT", "2025-01-01")
    print(df.head(5).to_string())
