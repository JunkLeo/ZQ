# -*- coding:utf-8 -*-
"""
Date: 2023/07/29
Desc: 上交所股票每日EOD
"""
import os
import sys
import requests
import pandas as pd
from pathlib import Path
from decimal import Decimal
parent_path = str(Path(__file__).parents[1])
sys.path.append(parent_path)
from tools.tradingday import CN_TradingDay


class SSE:

    def __init__(self):
        self.eod_url = "http://webapi.cninfo.com.cn/api/sysapi/p_sysapi1007"

        self.eod_columns = ["InstrumentID", "TradingDay", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "Volume", "Turnover"]

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
        }

        self.tradingday = CN_TradingDay()

    def get_eod(self, date: str) -> pd.DataFrame:
        para = {
            "tdate": "-".join([date[:4], date[4:6], date[6:]]),
            "market": "SHE"
        }
        sc = requests.post("http://webapi.cninfo.com.cn/api/spidercheck", headers=self.headers)
        r = requests.post(url=self.eod_url, headers=self.headers, json=para)
        eod = pd.DataFrame(r.json()["records"])
        eod["TradingDay"] = date
        eod = eod[["证券代码", "TradingDay", "开盘价", "最高价", "最低价", "收盘价", "成交数量", "成交金额"]]
        eod.columns = self.eod_columns
        return eod


if __name__ == "__main__":
    sse = SSE()
    print(sse.get_eod("20230728"))
