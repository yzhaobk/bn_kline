import logging
import time

import pandas as pd
import requests

from .model import LIMIT, COL_NAMES, MarketType

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def to_numeric(df):
    """
    将 DataFrame 中的所有列转换为数值类型。
    无法转换的值将被置为 NaN。
    """
    return df.apply(pd.to_numeric, errors='coerce')


def freq_to_milliseconds(freq):
    """
    将频率字符串转换为毫秒数。

    :param freq: 频率字符串，例如 '1m', '5m', '1h', '1d'
    :return: 对应的毫秒数
    """
    unit = freq[-1]
    amount = int(freq[:-1])
    if unit == 'm':
        return amount * 60 * 1000
    elif unit == 'h':
        return amount * 60 * 60 * 1000
    elif unit == 'd':
        return amount * 24 * 60 * 60 * 1000
    elif unit == 'w':
        return amount * 7 * 24 * 60 * 60 * 1000
    elif unit == 'M':
        return amount * 30 * 24 * 60 * 60 * 1000  # 约定每月30天
    else:
        raise ValueError(f"Unsupported frequency unit: {unit}")


def fetch_klines(market_type: MarketType, symbol, start_time, end_time, freq='1m'):
    """
    获取 Binance 合约的 K线数据。
    :param market_type: 市场类型，例如 'spot', 'coin_margin', 'usd_margin'
    :param symbol: 合约交易对，例如 'ETHUSD_PERP' 或 'ETHUSDT'
    :param start_time: 开始时间戳（毫秒）
    :param end_time: 结束时间戳（毫秒）
    :param freq: 时间间隔，默认 '1m'
    :return: 包含 K线数据的 Pandas DataFrame
    """
    logging.info(f"正在获取 {symbol} 从 {start_time} 到 {end_time} 的数据")
    data = []

    interval = freq_to_milliseconds(freq)  # 频率对应的毫秒数
    total_interval = LIMIT * interval  # 每次请求覆盖的时间范围

    current_start = start_time - interval  # 为了确保包含 start_time

    max_iterations = 1000
    iteration = 0

    match market_type:
        case "spot":
            url = 'https://api.binance.com/api/v3/klines'
        case "coin_margin":
            url = 'https://dapi.binance.com/dapi/v1/klines'
        case "usd_margin":
            url = 'https://fapi.binance.com/fapi/v1/klines'
        case _:
            raise ValueError(f"未知的市场类型: {market_type}")

    headers = {
        'User-Agent': 'Mozilla/5.0 (compatible; BinanceKlinesFetcher/1.0)'
    }

    while current_start < end_time and iteration < max_iterations:
        current_end = current_start + total_interval
        if current_end > end_time:
            current_end = end_time

        params = {
            'symbol': symbol,
            'interval': freq,
            'startTime': current_start,
            'endTime': current_end,
            'limit': LIMIT
        }

        try:
            response = requests.get(url, params=params, headers=headers)
            if response.status_code == 200:
                resp = response.json()
            elif response.status_code == 429:
                logging.warning("Rate limit exceeded. Waiting for 1 minute.")
                time.sleep(60)
                continue
            else:
                logging.error(f"HTTP 错误 {response.status_code}: {response.text}")
                break

            if not isinstance(resp, list):
                logging.error(f"错误响应：{resp} {current_start} => {current_end}")
                break

            if not resp:
                logging.info(f"No data returned for range {current_start} => {current_end}")
                break

            data += resp
            logging.info(f"已获取 {len(resp)} 条 K线数据，从 {current_start} 到 {current_end}")

            if len(resp) < LIMIT:
                break

            # 更新下一个请求的 start_time
            last_timestamp = int(resp[-1][0])
            current_start = last_timestamp - interval  # 下一个请求的开始时间

            iteration += 1

            # 避免触发速率限制
            time.sleep(0.1)

        except Exception as e:
            logging.error(f"请求错误: {e}. 跳过当前时间段 {current_start} => {current_end}.")
            current_start = current_end  # 跳过当前时间段
            iteration += 1

    if data:
        df = pd.DataFrame(data, columns=COL_NAMES)
        df = to_numeric(df)
        # 仅过滤 open_time 以内的数据
        df = df[df['open_time'] < end_time]
        df.drop_duplicates(inplace=True)
        return df
    else:
        logging.info(f"{symbol} 没有获取到数据")
        return pd.DataFrame()


def fetch_coin_margin_klines(symbol, start_time, end_time, freq='1m'):
    """
    获取 Coin-Margin 合约的 K线数据

    :param symbol: 合约交易对，例如 'ETHUSD_PERP'
    :param start_time: 开始时间戳（毫秒）
    :param end_time: 结束时间戳（毫秒）
    :param freq: 时间间隔，默认 '1m'
    :return: 包含 K线数据的 Pandas DataFrame
    """
    return fetch_klines("coin_margin", symbol, start_time, end_time, freq)


def fetch_usd_margin_klines(symbol, start_time, end_time, freq='1m'):
    """
    获取 Binance USD-Margin 合约的 K线数据

    :param symbol: 合约交易对，例如 'ETHUSDT'
    :param start_time: 开始时间戳（毫秒）
    :param end_time: 结束时间戳（毫秒）
    :param freq: 时间间隔，默认 '1m'
    :return: 包含 K线数据的 Pandas DataFrame
    """
    return fetch_klines("usd_margin", symbol, start_time, end_time, freq)


def fetch_spot_klines(symbol, start_time, end_time, freq='1m'):
    """
    获取 Binance Spot 市场的 K线数据

    :param symbol: 交易对，例如 'BTCUSDT'
    :param start_time: 开始时间戳（毫秒）
    :param end_time: 结束时间戳（毫秒）
    :param freq: 时间间隔，默认 '1m'
    :return: 包含 K线数据的 Pandas DataFrame
    """
    return fetch_klines("spot", symbol, start_time, end_time, freq)
