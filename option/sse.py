# -*- coding:utf-8 -*-
"""
Date: 2023/08/02
Desc: 上交所基金每日EOD
"""
import os
import sys
import time
import json
import requests
import pandas as pd


class SSE:

    def __init__(self):
        self.ref_url = "http://query.sse.com.cn/commonQuery.do?jsonCallBack=jsonpCallback25531396&sqlId=SSE_ZQPZ_YSP_GGQQZSXT_XXPL_DRHY_SEARCH_L&_={t}"
        self.eod_url = "http://yunhq.sse.com.cn:32041/v1/sho/list/tstyle/{underlying}_{expiremonth}?callback=jsonpCallback65983501&select=contractid%2Clast%2Cpresetpx%2C&order=contractid%2C&_={t}"
        self.expiremonth_url = "http://yunhq.sse.com.cn:32041/v1/sho/list/exchange/stockexpire?callback=jsonpCallback12956269&select=stockid%2Cexpiremonth&_={t}"

        self.eod_columns = ["InstrumentID", "TradingDay", "SettlePrice", "PreSettlePrice"]

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
            "Referer": "http://www.sse.com.cn/"
        }

    def get_ref(self) -> pd.DataFrame:
        t = str(time.time() * 1000).split(".")[0]
        r = requests.get(url=self.ref_url.format(t=t), headers=self.headers, timeout=10)
        data = json.loads(r.text.split("(", 1)[1][:-1])
        ref = pd.DataFrame(data["result"])
        ref.set_index("SECURITY_ID", inplace=True)
        return ref

    def get_underlying_expiremonth(self) -> pd.DataFrame:
        t = str(time.time() * 1000).split(".")[0]
        r = requests.get(self.expiremonth_url.format(t=t), headers=self.headers, timeout=10)
        data = json.loads(r.text.split("(")[1][:-1])
        ue = pd.DataFrame(data["list"], columns=["Underlying", "ExpireMonth"])
        ue["ExpireMonth"] = ue["ExpireMonth"].map(lambda x: str(x)[-2:])
        return ue

    # ongoing
    def get_eod(self) -> pd.DataFrame:
        t = str(time.time() * 1000).split(".")[0]
        ue = self.get_underlying_expiremonth()
        eod = pd.DataFrame()
        for index, row in ue.iterrows():
            r = requests.get(self.eod_url.format(underlying=row["Underlying"], expiremonth=row["ExpireMonth"], t=t), headers=self.headers, timeout=10)
            data = json.loads(r.text.split("(", 1)[1][:-1])
            df = pd.DataFrame(data["list"], columns=["InstrumentID", "SettlePrice", "PreSettlePrice"])
            df["TradingDay"] = data["date"]
            df = df[self.eod_columns]
            eod = pd.concat([eod, df])
        return eod

if __name__ == "__main__":
    sse = SSE()
    print(sse.get_ref())
    print(sse.get_eod())
