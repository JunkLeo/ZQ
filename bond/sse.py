# -*- coding:utf-8 -*-
"""
Date: 2023/08/02
Desc: 上交所债券每日EOD
"""
import os
import sys
import time
import json
import requests
import pandas as pd


class SSE:

    def __init__(self):
        self.bond_list_url = "http://query.sse.com.cn/sseQuery/commonSoaQuery.do?jsonCallBack=jsonpCallback64754136&sqlId=CP_ZQ_ZQLB&BOND_TYPE=%E5%85%A8%E9%83%A8&_={t}"
        self.bond_eod_url = "http://yunhq.sse.com.cn:32041/v1/shb1/dayk/{bond}?callback=jQuery112402788803963488542_{t}&begin=0&end=-1&period=day&_={t}"

        self.eod_url = "http://yunhq.sse.com.cn:32041/v1/shb1/list/exchange/all?callback=jsonpCallback62585600&select=code%2Copen%2Chigh%2Clow%2Clast%2Cvolume%2Camount%2C&order=&begin=0&end=-1&_={t}"

        self.eod_columns = ["InstrumentID", "TradingDay", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "Volume", "Turnover"]

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
            "Referer": "http://www.sse.com.cn/"
        }

    def get_bond_list(self) -> pd.DataFrame:
        t = str(time.time() * 1000).split(".")[0]
        r = requests.get(self.bond_list_url.format(t=t), headers=self.headers, timeout=30)
        data = json.loads(r.text.split("(", 1)[1][:-1])
        bond_list = pd.DataFrame(data["result"])
        bond_list.set_index("BOND_CODE", inplace=True)
        return bond_list

    def get_single_bond_eod(self, bond: str, date: str = "all") -> pd.DataFrame:
        t = str(time.time() * 1000).split(".")[0]
        r = requests.get(self.bond_eod_url.format(t=t, bond=bond), headers=self.headers, timeout=10)
        data = json.loads(r.text.split("(")[1][:-1])
        eod = pd.DataFrame(data["kline"], columns=["TradingDay", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "Volume", "Turnover"])
        eod["InstrumentID"] = bond
        eod = eod[self.eod_columns]
        if date != "all":
            eod = eod[eod["TradingDay"] == int(date)]
        return eod

    def get_eod_history(self) -> pd.DataFrame:
        bond_list = self.get_bond_list()
        eod = pd.DataFrame()
        for bond in bond_list.index:
            retry = 0
            while retry < 3:
                try:
                    df = self.get_single_bond_eod(bond)
                    break
                except IndexError:
                    df = pd.DataFrame()
                    break
                except Exception:
                    df = pd.DataFrame()
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
    #  print(sse.get_bond_list())
    #  print(sse.get_eod_history())
    print(sse.get_eod())
