import asyncio
import logging

import pandas as pd

from .downloader import get_path, download_kline, batch_download
from .model import COL_NAMES, MarketType

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


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


def read_kline_data(market_type: MarketType, symbol: str, start_date, end_date=None, freq: str = '1m') -> pd.DataFrame:
    """
    Reads K-line data for a specified period.

    :param market_type: The type of market (e.g., spot, coin_margin, usd_margin).
    :param symbol: The trading pair symbol (e.g., BTCUSDT).
    :param start_date: The start date for the data retrieval period.
    :param end_date: The end date for the data retrieval period. If None, defaults to start_date.
    :param freq: The frequency of the K-line data (default is '1m' for 1 minute).
    :return: A DataFrame containing the K-line data for the specified period.
    """

    if end_date is None:
        end_date = start_date

    download_list = []
    dates = pd.date_range(start_date, end_date)
    for d in dates:
        # 检查数据，如果不存在则加入下载队列
        date_str = d.strftime("%Y-%m-%d")
        path = get_path(market_type, symbol, date_str, freq)
        if not path.exists():
            download_list.append(date_str)

    if download_list:
        tasks = []
        logger.info(f"需要下载 {len(download_list)} 个文件: {download_list}")
        for date_str in download_list:
            tasks.append(download_kline(market_type, symbol, date_str, freq))
        asyncio.run(batch_download(tasks))
        logger.info("下载完成")

    date = start_date
    dfs = []
    logger.info(f"读取 {market_type}:{symbol} {start_date} => {end_date} 的数据")
    while date <= end_date:
        df = load_binance_kline(market_type, symbol, date, freq)
        dfs.append(df)
        date = (pd.to_datetime(date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")

    return pd.concat(dfs, ignore_index=True)
