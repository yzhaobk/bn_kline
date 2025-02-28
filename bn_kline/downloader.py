import asyncio
import logging
from pathlib import Path

import aiofiles
import aiohttp
from tenacity import retry, wait_fixed, stop_after_attempt

from .model import MarketType

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_path(market_type: MarketType, symbol, date, freq='1m'):
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


@retry(wait=wait_fixed(5), stop=stop_after_attempt(3))
async def download(url: str, save_path: Path, semaphore: asyncio.Semaphore = asyncio.Semaphore(10)) -> None:
    """
    异步下载文件并保存到指定路径，使用信号量限制并发数。

    :param url: 下载URL
    :param save_path: 文件保存路径
    :param semaphore: asyncio.Semaphore 实例，默认限制为10
    """
    async with semaphore:
        if save_path.exists():
            logger.info(f"文件已存在: {save_path}")
            return

        # 创建保存目录
        save_path.parent.mkdir(parents=True, exist_ok=True)

        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url) as response:
                    if response.status == 200:
                        data = await response.read()
                        async with aiofiles.open(save_path, "wb") as file:
                            await file.write(data)
                        logger.info(f"文件已下载并保存到 {save_path}")
                    else:
                        logger.error(f"下载失败，HTTP 状态码: {response.status}，URL: {url}")
            except Exception as e:
                logger.error(f"下载错误: {e}，URL: {url}")


async def download_kline(market_type: MarketType, symbol: str, date: str, freq: str = '1m',
                         semaphore: asyncio.Semaphore = None) -> None:
    if semaphore is None:
        semaphore = asyncio.Semaphore(10)

    save_path = get_path(market_type, symbol, date, freq)
    match market_type:
        case "spot":
            url = f"https://data.binance.vision/data/spot/daily/klines/{symbol}/{freq}/{symbol}-{freq}-{date}.zip"
            await download(url, save_path, semaphore)
        case "coin_margin":
            url = f"https://data.binance.vision/data/futures/cm/daily/klines/{symbol}/{freq}/{symbol}-{freq}-{date}.zip"
            await download(url, save_path, semaphore)
        case "usd_margin":
            url = f"https://data.binance.vision/data/futures/um/daily/klines/{symbol}/{freq}/{symbol}-{freq}-{date}.zip"
            await download(url, save_path, semaphore)
        case _:
            raise ValueError(f"未知的市场类型: {market_type}")


async def batch_download(tasks, batch_size=10):
    for i in range(0, len(tasks), batch_size):
        batch = tasks[i:i + batch_size]
        await asyncio.gather(*batch)
        logger.info(f"已完成批次 {i // batch_size + 1}")
