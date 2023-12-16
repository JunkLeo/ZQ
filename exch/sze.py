# -*- coding:utf-8 -*-
"""
Date: 2023/09/22
Desc: 深交所
"""
import requests
import pandas as pd
from random import random
from decimal import Decimal


class SZE:

    def __init__(self) -> None:
        self.stock = Stock()
        self.bond = Bond()
        self.fund = Fund()
        self.index = Index()
        self.option = Option()
        self.repo = Repo()


class Stock:

    def __init__(self):
        self.eod_url = "https://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=1815_stock_snapshot&TABKEY=tab1&txtBeginDate={date}&txtEndDate={date}&archiveDate=2021-07-01&random={randid}"

        self.eod_columns = ["InstrumentID", "TradingDay", "PreClosePrice", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "Volume", "Turnover"]

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
        }

    def get_eod(self, date: str) -> pd.DataFrame:
        r = requests.get(url=self.eod_url.format(date="-".join([date[:4], date[4:6], date[6:]]), randid=random()), headers=self.headers, timeout=10)
        eod = pd.read_excel(r.content, engine="openpyxl")
        eod = eod[["证券代码", "交易日期", "前收", "开盘", "最高", "最低", "今收", "成交量(万股)", "成交金额(万元)"]]
        eod.columns = self.eod_columns
        eod["InstrumentID"] = eod["InstrumentID"].map(lambda x: str(x).zfill(6))
        eod["TradingDay"] = eod["TradingDay"].map(lambda x: x.replace("-", ""))
        eod["Volume"] = eod["Volume"].map(lambda x: Decimal(x.replace(",", "")) * 10000)
        eod["Turnover"] = eod["Turnover"].map(lambda x: Decimal(x.replace(",", "")) * 10000)
        return eod


class Bond:

    def __init__(self):
        self.eod_url = "https://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=1815_stock_snapshot&TABKEY=tab3&txtBeginDate={date}&txtEndDate={date}&archiveDate=2021-07-01&random={randid}"

        self.eod_columns = ["InstrumentID", "TradingDay", "PreClosePrice", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "Turnover"]

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
        }

    def get_eod(self, date: str) -> pd.DataFrame:
        r = requests.get(url=self.eod_url.format(date="-".join([date[:4], date[4:6], date[6:]]), randid=random()), headers=self.headers, timeout=10)
        eod = pd.read_excel(r.content, engine="openpyxl")
        eod = eod[["证券代码", "交易日期", "前收", "开盘", "最高", "最低", "今收", "成交金额(万元)"]]
        eod.columns = self.eod_columns
        eod["TradingDay"] = eod["TradingDay"].map(lambda x: x.replace("-", ""))
        eod["Turnover"] = eod["Turnover"].map(lambda x: Decimal(x.replace(",", "")) * 10000)
        return eod


class Fund:

    def __init__(self):
        self.eod_url = "https://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=1815_stock_snapshot&TABKEY=tab2&txtBeginDate={date}&txtEndDate={date}&archiveDate=2021-07-01&random={randid}"

        self.eod_columns = ["InstrumentID", "TradingDay", "PreClosePrice", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "Volume", "Turnover"]

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
        }

    def get_eod(self, date: str) -> pd.DataFrame:
        r = requests.get(url=self.eod_url.format(date="-".join([date[:4], date[4:6], date[6:]]), randid=random()), headers=self.headers, timeout=10)
        eod = pd.read_excel(r.content, engine="openpyxl")
        eod = eod[["证券代码", "交易日期", "前收", "开盘", "最高", "最低", "今收", "成交量（万份）", "成交金额(万元)"]]
        eod.columns = self.eod_columns
        eod["TradingDay"] = eod["TradingDay"].map(lambda x: x.replace("-", ""))
        eod["Volume"] = eod["Volume"].map(lambda x: Decimal(x.replace(",", "")) * 10000)
        eod["Turnover"] = eod["Turnover"].map(lambda x: Decimal(x.replace(",", "")) * 10000)
        return eod


class Index:

    def __init__(self):
        self.eod_url = "https://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=1815_stock_snapshot&TABKEY=tab7&txtBeginDate={date}&txtEndDate={date}&archiveDate=2021-07-01&random={randid}"

        self.eod_columns = ["InstrumentID", "TradingDay", "PreClosePrice", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "Turnover"]

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
        }

    def get_eod(self, date: str) -> pd.DataFrame:
        r = requests.get(url=self.eod_url.format(date="-".join([date[:4], date[4:6], date[6:]]), randid=random()), headers=self.headers, timeout=10)
        eod = pd.read_excel(r.content, engine="openpyxl")
        eod = eod[["指数代码", "交易日期", "前收", "开盘", "最高", "最低", "今收", "成交金额(亿元)"]]
        eod.columns = self.eod_columns
        eod["TradingDay"] = eod["TradingDay"].map(lambda x: x.replace("-", ""))
        eod["Turnover"] = eod["Turnover"].map(lambda x: Decimal(x.replace(",", "")) * 100000000)
        return eod


class Option:

    def __init__(self):
        self.ref_url = "https://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=option_drhy&TABKEY=tab1&random={randid}"
        self.eod_url = "https://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=1815_stock_snapshot&TABKEY=tab6&txtBeginDate={date}&txtEndDate={date}&archiveDate=2021-08-02&random={randid}"

        self.eod_columns = ["InstrumentID", "TradingDay", "PreSettlePrice", "ClosePrice", "SettlePrice", "Volume"]

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
        }

    def get_ref(self) -> pd.DataFrame:
        r = requests.get(url=self.ref_url.format(randid=random()), headers=self.headers, timeout=10)
        ref = pd.read_excel(r.content)
        return ref

    def get_eod(self, date: str) -> pd.DataFrame:
        r = requests.get(url=self.eod_url.format(date="-".join([date[:4], date[4:6], date[6:]]), randid=random()), headers=self.headers, timeout=10)
        eod = pd.read_excel(r.content, engine="openpyxl")
        eod = eod[["合约编码", "交易日期", "前结算价", "今收盘价", "今结算价", "成交量（张）"]]
        eod.columns = self.eod_columns
        eod["TradingDay"] = eod["TradingDay"].map(lambda x: x.replace("-", ""))
        return eod


class Repo:

    def __init__(self):
        self.eod_url = "https://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=1815_stock_snapshot&TABKEY=tab4&txtBeginDate={date}&txtEndDate={date}&archiveDate=2021-07-01&random={randid}"

        self.eod_columns = ["InstrumentID", "TradingDay", "PreClosePrice", "ClosePrice", "Turnover"]

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
        }

    def get_eod(self, date: str) -> pd.DataFrame:
        r = requests.get(url=self.eod_url.format(date="-".join([date[:4], date[4:6], date[6:]]), randid=random()), headers=self.headers, timeout=10)
        eod = pd.read_excel(r.content, engine="openpyxl")
        eod = eod[["证券代码", "交易日期", "前收", "今收", "成交金额(万元)"]]
        eod.columns = self.eod_columns
        eod = eod[~eod["InstrumentID"].isna()]
        eod["TradingDay"] = eod["TradingDay"].map(lambda x: x.replace("-", ""))
        eod["Turnover"] = eod["Turnover"].map(lambda x: Decimal(str(x)) * 10000)
        return eod


if __name__ == "__main__":
    sze = SZE()
    import sys
    from datetime import datetime

    from loguru import logger
    date = sys.argv[1] if len(sys.argv) > 1 else datetime.today().strftime("%Y%m%d")
    logger.info(f"Run {date}")
    print(sze.stock.get_eod(date))
    print(sze.bond.get_eod(date))
    print(sze.fund.get_eod(date))
    print(sze.index.get_eod(date))
    print(sze.option.get_eod(date))
    print(sze.repo.get_eod(date))
