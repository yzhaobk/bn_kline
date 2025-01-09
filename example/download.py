import asyncio
import logging

import pandas as pd
from tqdm.asyncio import tqdm

from bn_kline import download_kline


async def main():
    """
    主函数：定义下载日期范围，创建下载任务，限制并发数，并执行下载。
    """
    # 定义日期范围：2024-01-01 至 2024-12-31
    start_date = '2020-01-01'
    end_date = '2024-12-31'
    dates = pd.date_range(start=start_date, end=end_date)

    tasks = []
    semaphore = asyncio.Semaphore(10)  # 设置并发下载任务数为10

    for date in dates:
        date_str = date.strftime("%Y-%m-%d")
        # 创建下载任务，并传入共享的信号量
        tasks.append(download_kline("spot", "ETHUSDT", date_str, "1m", semaphore))
        tasks.append(download_kline("usd_margin", "ETHUSDT", date_str, "1m", semaphore))
        tasks.append(download_kline("coin_margin", "ETHUSD_PERP", date_str, "1m", semaphore))

    for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Downloading Klines"):
        await f

    logging.info("所有下载任务已完成。")


if __name__ == "__main__":
    asyncio.run(main())
