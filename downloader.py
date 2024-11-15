from pathlib import Path

import requests


def get_path(symbol, freq, date):
    """
    获取用户目录
    """
    # 获取本地用户目录
    user_home = Path.home()
    # 构建文件保存的路径
    save_path = user_home / "data" / "binance" / "spot" / f"{symbol}_{freq}_{date}.zip"
    return save_path


def download_binance_kline(symbol: str, freq: str, date: str):
    """
    下载 Binance K 线数据并保存到用户目录下

    :param symbol: 交易对符号, 例如 'BTCUSDT'
    :param freq: K 线频率, 例如 '1m'
    :param date: 日期, 格式为 'YYYY-MM-DD', 例如 '2024-11-10'
    """

    # 构建下载 URL
    url = f"https://data.binance.vision/data/spot/daily/klines/{symbol}/{freq}/{symbol}-{freq}-{date}.zip"
    save_path = get_path(symbol, freq, date)
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


# 示例调用：
download_binance_kline("BTCUSDT", "1m", "2024-11-10")
