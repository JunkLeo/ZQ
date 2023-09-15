# -*- coding:utf-8 -*-
"""
Date: 2023/08/03
Desc: 上交所指数每日EOD
"""
import time
import json
import requests
import pandas as pd


class SSE:

    def __init__(self):

        self.eod_url = "http://yunhq.sse.com.cn:32041/v1/sh1/list/exchange/index?callback=jsonpCallback54049231&select=code%2Cprev_close%2Copen%2Chigh%2Clow%2Clast%2Cvolume%2Camount%2C&order=&begin=0&end=-1&_={t}"

        self.eod_columns = ["InstrumentID", "TradingDay", "PreClosePrice", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "Volume", "Turnover"]

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
            "Referer": "http://www.sse.com.cn/"
        }

    # ongoing
    def get_eod(self) -> pd.DataFrame:
        t = str(time.time() * 1000).split(".")[0]
        r = requests.get(self.eod_url.format(t=t), headers=self.headers, timeout=10)
        data = json.loads(r.text.split("(")[1][:-1])
        eod = pd.DataFrame(data["list"], columns=["InstrumentID", "PreClosePrice", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "Volume", "Turnover"])
        eod["TradingDay"] = data["date"]
        eod = eod[self.eod_columns]
        return eod


if __name__ == "__main__":
    sse = SSE()
    print(sse.get_eod())
