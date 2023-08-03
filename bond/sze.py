# -*- coding:utf-8 -*-
"""
Date: 2023/08/01
Desc: 深交所债券每日EOD
"""
import os
import sys
import requests
import pandas as pd
from random import random
from decimal import Decimal


class SZE:

    def __init__(self):
        self.eod_url = "https://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=1815_stock_snapshot&TABKEY=tab3&txtBeginDate={date}&txtEndDate={date}&archiveDate=2021-07-01&random={randid}"

        self.eod_columns = ["InstrumentID", "TradingDay", "PreClosePrice", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "Turnover"]

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
        }

    def get_eod(self, date: str) -> pd.DataFrame:
        r = requests.get(url=self.eod_url.format(date="-".join([date[:4], date[4:6], date[6:]]), randid=random()), headers=self.headers, timeout=10)
        eod = pd.read_excel(r.content)
        eod = eod[["证券代码", "交易日期", "前收", "开盘", "最高", "最低", "今收", "成交金额(万元)"]]
        eod.columns = self.eod_columns
        eod["TradingDay"] = eod["TradingDay"].map(lambda x: x.replace("-", ""))
        eod["Turnover"] = eod["Turnover"].map(lambda x: Decimal(x.replace(",", "")) * 10000)
        return eod


if __name__ == "__main__":
    sze = SZE()
    print(sze.get_eod("20230731"))
