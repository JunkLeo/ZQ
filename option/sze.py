# -*- coding:utf-8 -*-
"""
Date: 2023/08/03
Desc: 深交所期权每日REF/EOD
"""
import requests
import pandas as pd
from random import random


class SZE:

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
        eod = pd.read_excel(r.content)
        eod = eod[["合约编码", "交易日期", "前结算价", "今收盘价", "今结算价", "成交量（张）"]]
        eod.columns = self.eod_columns
        eod["TradingDay"] = eod["TradingDay"].map(lambda x: x.replace("-", ""))
        return eod


if __name__ == "__main__":
    sze = SZE()
    print(sze.get_ref())
    print(sze.get_eod("20230802"))
