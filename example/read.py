from bn_kline import read_kline_data


def example_usage():
    """
    这是一个示例函数，用来演示如何调用 read_kline_data。
    """
    # 下面的代码就是原先你在 __main__ 里写的三次调用
    df1 = read_kline_data("spot", "BTCUSDT", "2024-01-01")
    print(df1.head(5).to_string())

    df2 = read_kline_data("coin_margin", "BTCUSD_PERP", "2024-01-01")
    print(df2.head(5).to_string())

    df3 = read_kline_data("usd_margin", "BTCUSDT", "2024-01-01")
    print(df3.head(5).to_string())


if __name__ == "__main__":
    # 当直接运行 load_data.py 时，就执行示例函数
    example_usage()