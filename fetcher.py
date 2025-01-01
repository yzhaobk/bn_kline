import time

import pandas as pd
import requests

from model import LIMIT, COL_NAMES


def to_numeric(df):
    for col in df.columns:
        try:
            df[col] = pd.to_numeric(df[col])
        except ValueError as e:
            print(f"Error converting {col} to numeric: {e}")
            pass


def fetch_coin_margin_klines(symbol, start_time, end_time, freq='1m'):
    print(symbol, start_time, end_time)
    data = []
    while True:
        try:
            response = requests.get(
                f'https://dapi.binance.com/dapi/v1/klines?symbol={symbol}&interval={freq}&startTime={start_time}&endTime={end_time}&limit={LIMIT}')
            resp = response.json()
            # 检查返回数据是否为预期格式
            if not isinstance(resp, list):
                print(f"Error response for {symbol}: {resp} {start_time}=>{end_time}")
                break
        except Exception as e:
            print(str(e))
            time.sleep(5)
            continue
        data += resp
        if len(resp) != LIMIT:
            break
        start_time = int(resp[-1][0]) + 60 * 1000  # 更新结束时间为最后一条数据的时间戳
    if data:
        df = pd.DataFrame(data)
        to_numeric(df)
        df.drop_duplicates(inplace=True)
        return df
    else:
        return pd.DataFrame()


def fetch_usd_margin_klines(symbol, start_time, end_time, freq='1m'):
    print(start_time, end_time)
    data = []
    while True:
        try:
            response = requests.get(
                f'https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval={freq}&startTime={start_time}&endTime={end_time}&limit={LIMIT}')
            resp = response.json()
            # 检查返回数据是否为预期格式
            if not isinstance(resp, list):
                print(f"Error response for {symbol}: {resp} {start_time}=>{end_time}")
                break
        except Exception as e:
            print(str(e))
            time.sleep(5)
            continue
        data += resp
        if len(resp) != LIMIT:
            break
        start_time = int(resp[-1][0]) + 60 * 1000  # 更新结束时间为最后一条数据的时间戳
    if data:
        df = pd.DataFrame(data)
        to_numeric(df)
        df.drop_duplicates(inplace=True)
        return df
    else:
        return pd.DataFrame()


if __name__ == '__main__':
    from datetime import datetime, timedelta, timezone

    end_time = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_time = int((datetime.now(timezone.utc) - timedelta(days=1)).timestamp() * 1000)

    symbol = 'BTCUSD_PERP'
    df_cm = fetch_coin_margin_klines(symbol, start_time, end_time)
    df_cm.columns = COL_NAMES
    print(df_cm.head().to_string())

    symbol = 'BTCUSDT'
    df_um = fetch_usd_margin_klines(symbol, start_time, end_time)
    df_um.columns = COL_NAMES
    print(df_um.head().to_string())
