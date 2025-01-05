# setup.py
import pathlib

from setuptools import setup, find_packages

# 获取当前目录路径
HERE = pathlib.Path(__file__).parent

# 读取 README.md 作为长描述
README = (HERE / "README.md").read_text(encoding="utf-8")


def parse_requirements(filename="requirements.txt"):
    with open(HERE / filename) as f:
        # 过滤掉空行和注释行
        return [line.strip() for line in f if line.strip() and not line.startswith("#")]


setup(
    name="bn_kline",  # 包名称
    version="0.1.0",  # 版本号
    author="yzhaobk",
    author_email="yzhaobk@gmail.com",
    description="一个用于下载 Binance K 线数据的工具",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://https://github.com/yzhaobk/bn_kline",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
    install_requires=parse_requirements("requirements.txt"),  # 从 requirements.txt 加载依赖

)
