from pathlib import Path

import requests

from fetcher import fetch_coin_margin_klines, fetch_usd_margin_klines
from model import MarketType


def get_path(market_type: MarketType, symbol, freq, date):
    """
    获取用户目录
    """
    # 获取本地用户目录
    user_home = Path.home()
    # 构建文件保存的路径
    match market_type:
        case "spot":
            save_path = user_home / "data" / "market" / "binance" / "kline" / f"{symbol}_{freq}_{date}.zip"
        case "coin_margin":
            save_path = user_home / "data" / "market" / "binancecoinm" / "kline" / f"{symbol}_{freq}_{date}.zip"
        case "usd_margin":
            save_path = user_home / "data" / "market" / "binanceusdm" / "kline" / f"{symbol}_{freq}_{date}.zip"
        case _:
            raise ValueError(f"未知的市场类型: {market_type}")
    return save_path


def download(url, save_path):
    """
    下载文件
    """
    if save_path.exists():
        print(f"文件已存在: {save_path}")
        return

    # 创建保存目录
    save_path.parent.mkdir(parents=True, exist_ok=True)

    # 发送 GET 请求下载文件
    response = requests.get(url)

    # 检查请求是否成功
    if response.status_code == 200:
        # 将下载的数据写入文件
        with open(save_path, "wb") as file:
            file.write(response.content)

        print(f"文件已下载并保存到 {save_path}")
    else:
        print(f"下载失败，HTTP 错误码: {response.status_code}")


def download_spot_kline(symbol: str, date: str, freq: str = "1m"):
    """
    下载 Binance K 线数据并保存到用户目录下

    :param symbol: 交易对符号, 例如 'BTCUSDT'
    :param date: 日期, 格式为 'YYYY-MM-DD', 例如 '2024-11-10'
    :param freq: K 线频率, 例如 '1m'
    """

    # 构建下载 URL
    url = f"https://data.binance.vision/data/spot/daily/klines/{symbol}/{freq}/{symbol}-{freq}-{date}.zip"
    save_path = get_path("spot", symbol, freq, date)
    download(url, save_path)


def download_cm_kline(symbol: str, date: str, freq: str = '1m'):
    """
    下载 CoinMarketCap K 线数据并保存到用户目录下

    :param symbol: 交易对符号, 例如 'BTCUSDT'
    :param date: 日期, 格式为 'YYYY-MM-DD', 例如 '2024-11-10'
    :param freq: K 线频率, 例如 '1m'
    """

    # 构建下载 URL
    start = pd.Timestamp(date)
    end = start + pd.Timedelta(days=1)
    start = int(start.timestamp() * 1000)
    end = int(end.timestamp() * 1000)
    df = fetch_coin_margin_klines(symbol, start, end, freq)
    save_path = get_path("coin_margin", symbol, freq, date)
    df.to_csv(save_path, index=False)


def download_um_kline(symbol: str, date: str, freq: str = '1m'):
    """
    下载 UsdMargin K 线数据并保存到用户目录下

    :param symbol: 交易对符号, 例如 'BTCUSDT'
    :param freq: K 线频率, 例如 '1m'
    :param date: 日期, 格式为 'YYYY-MM-DD', 例如 '2024-11-10'
    """
    # 构建下载 URL
    start = pd.Timestamp(date)
    end = start + pd.Timedelta(days=1)
    start = int(start.timestamp() * 1000)
    end = int(end.timestamp() * 1000)
    df = fetch_usd_margin_klines(symbol, start, end, freq)
    save_path = get_path("usd_margin", symbol, freq, date)
    df.to_csv(save_path, index=False)


if __name__ == "__main__":
    import pandas as pd

    yesterday = pd.Timestamp.today()
    week_ago = yesterday - pd.Timedelta(days=2)
    for date in pd.date_range(week_ago, yesterday):
        download_spot_kline("BTCUSDT", date.strftime("%Y-%m-%d"))
        download_um_kline("BTCUSDT", date.strftime("%Y-%m-%d"))
        download_cm_kline("BTCUSD_PERP", date.strftime("%Y-%m-%d"))
