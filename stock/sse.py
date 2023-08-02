# -*- coding:utf-8 -*-
"""
Date: 2023/08/02
Desc: 上交所股票每日EOD
"""
import os
import sys
import time
import json
import requests
import pandas as pd


class SSE:

    def __init__(self):
        self.stock_list_url = "http://query.sse.com.cn//sseQuery/commonExcelDd.do?sqlId=COMMON_SSE_CP_GPJCTPZ_GPLB_GP_L&type=inParams&CSRC_CODE=&STOCK_CODE=&REG_PROVINCE=&STOCK_TYPE={stock_type}&COMPANY_STATUS=2,4,5,7,8"
        self.delist_list_url = "http://query.sse.com.cn//sseQuery/commonExcelDd.do?sqlId=COMMON_SSE_CP_GPJCTPZ_GPLB_ZZGP_L&type=inParams&CSRC_CODE=&STOCK_CODE=&REG_PROVINCE=&STOCK_TYPE={stock_type}&COMPANY_STATUS=3"
        self.stock_eod_url = "http://yunhq.sse.com.cn:32041/v1/sh1/dayk/{stock}?callback=jQuery112409826351965297482_{t}&begin=0&end=-1&period=day&_={t}"

        self.eod_url = "http://yunhq.sse.com.cn:32041/v1/sh1/list/exchange/equity?callback=jsonpCallback62585600&select=code%2Copen%2Chigh%2Clow%2Clast%2Cvolume%2Camount%2C&order=&begin=0&end=-1&_={t}"

        self.eod_columns = ["InstrumentID", "TradingDay", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "Volume", "Turnover"]

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
            "Referer": "http://www.sse.com.cn/"
        }

    def get_stock_list(self) -> pd.DataFrame:
        stock_list = pd.DataFrame()
        for stock_type in ["1", "8"]:
            r = requests.get(url=self.stock_list_url.format(stock_type=stock_type), headers=self.headers, timeout=10)
            tmp = pd.read_excel(r.content)
            stock_list = pd.concat([stock_list, tmp])
        stock_list.set_index("A股代码", inplace=True)
        return stock_list

    def get_delist(self) -> pd.DataFrame:
        delist = pd.DataFrame()
        for stock_type in ["1", "8"]:
            r = requests.get(url=self.delist_list_url.format(stock_type=stock_type), headers=self.headers, timeout=10)
            tmp = pd.read_excel(r.content)
            delist = pd.concat([delist, tmp])
        delist.set_index("原公司代码", inplace=True)
        return delist

    def get_single_stock_eod(self, stock: str, date: str = "all") -> pd.DataFrame:
        t = str(time.time() * 1000).split(".")[0]
        r = requests.get(self.stock_eod_url.format(t=t, stock=stock), headers=self.headers, timeout=10)
        data = json.loads(r.text.split("(")[1][:-1])
        eod = pd.DataFrame(data["kline"], columns=["TradingDay", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "Volume", "Turnover"])
        eod["InstrumentID"] = stock
        eod = eod[self.eod_columns]
        if date != "all":
            eod = eod[eod["TradingDay"] == int(date)]
        return eod

    def get_eod_history(self) -> pd.DataFrame:
        stock_list = self.get_stock_list()
        delist = self.get_delist()
        _all = list(stock_list.index) + list(delist.index)
        eod = pd.DataFrame()
        for stock in _all:
            retry = 0
            while retry < 3:
                try:
                    df = self.get_single_stock_eod(stock)
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
    print(sse.get_single_stock_eod("600000", "20230802"))
    print(sse.get_eod())
    print(sse.get_eod_history())
