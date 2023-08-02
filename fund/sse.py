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
        self.fund_list_url = "http://query.sse.com.cn/commonSoaQuery.do?jsonCallBack=jsonpCallback14059342&sqlId=FUND_LIST&fundType=00%2C10%2C20%2C30%2C40%2C50%2C&order=&_={t}"
        self.fund_eod_url = "http://yunhq.sse.com.cn:32041/v1/sh1/dayk/{fund}?callback=jQuery112409826351965297482_{t}&begin=0&end=-1&period=day&_={t}"

        self.eod_url = "http://yunhq.sse.com.cn:32041/v1/sh1/list/exchange/fwr?callback=jsonpCallback87559922&select=code%2Copen%2Chigh%2Clow%2Clast%2Cvolume%2Camount%2C&order=&begin=0&end=-1&_={t}"

        self.eod_columns = ["InstrumentID", "TradingDay", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "Volume", "Turnover"]

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
            "Referer": "http://www.sse.com.cn/"
        }

    def get_fund_list(self) -> pd.DataFrame:
        t = str(time.time() * 1000).split(".")[0]
        r = requests.get(url=self.fund_list_url.format(t=t), headers=self.headers, timeout=10)
        data = json.loads(r.text.split("(", 1)[1][:-1])
        fund_list = pd.DataFrame(data["result"])
        fund_list.set_index("fundCode", inplace=True)
        return fund_list

    def get_single_fund_eod(self, fund: str, date: str = "all") -> pd.DataFrame:
        t = str(time.time() * 1000).split(".")[0]
        r = requests.get(self.fund_eod_url.format(t=t, fund=fund), headers=self.headers, timeout=10)
        data = json.loads(r.text.split("(")[1][:-1])
        eod = pd.DataFrame(data["kline"], columns=["TradingDay", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "Volume", "Turnover"])
        eod["InstrumentID"] = fund
        eod = eod[self.eod_columns]
        if date != "all":
            eod = eod[eod["TradingDay"] == int(date)]
        return eod

    def get_eod_history(self) -> pd.DataFrame:
        fund_list = self.get_fund_list()
        eod = pd.DataFrame()
        for fund in fund_list.index:
            retry = 0
            while retry < 3:
                try:
                    df = self.get_single_fund_eod(fund)
                    break
                except:
                    retry += 1
                    continue
            eod = pd.concat([eod, df])
        return eod

    # ongoing
    def get_eod(self) -> pd.DataFrame:
        t = str(time.time() * 1000).split(".")[0]
        r = requests.get(self.eod_url.format(t=t), headers=self.headers, timeout=10)
        data = json.loads(r.text.split("(")[1][:-1])
        eod = pd.DataFrame(data["list"], columns=["InstrumentID", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "Volume", "Turnover"])
        eod["TradingDay"] = data["date"]
        eod = eod[self.eod_columns]
        return eod


if __name__ == "__main__":
    sse = SSE()
    #  print(sse.get_fund_list())
    #  print(sse.get_eod_history())
    print(sse.get_eod())
