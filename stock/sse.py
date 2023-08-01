# -*- coding:utf-8 -*-
"""
Date: 2023/07/29
Desc: 上交所股票每日EOD
"""
import os
import sys
import time
import json
import requests
import pandas as pd
from decimal import Decimal


class SSE:

    def __init__(self):
        self.stock_list_url = "http://query.sse.com.cn//sseQuery/commonExcelDd.do?sqlId=COMMON_SSE_CP_GPJCTPZ_GPLB_GP_L&type=inParams&CSRC_CODE=&STOCK_CODE=&REG_PROVINCE=&STOCK_TYPE={stock_type}&COMPANY_STATUS=2,4,5,7,8"
        self.delist_list_url = "http://query.sse.com.cn//sseQuery/commonExcelDd.do?sqlId=COMMON_SSE_CP_GPJCTPZ_GPLB_ZZGP_L&type=inParams&CSRC_CODE=&STOCK_CODE=&REG_PROVINCE=&STOCK_TYPE={stock_type}&COMPANY_STATUS=3"
        self.stock_eod_url = "http://yunhq.sse.com.cn:32041/v1/sh1/dayk/{stock}?callback=jQuery112409826351965297482_{t}&begin={begin}&end=-1&period=day&_={t}"

        self.eod_url = "http://webapi.cninfo.com.cn/api/sysapi/p_sysapi1007?tdate={date}&market={market}"

        self.eod_columns = ["InstrumentID", "TradingDay", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "Volume", "Turnover"]

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
            "Referer": "http://www.sse.com.cn/"
        }

    def get_stock_list(self) -> pd.DataFrame:
        stock_list = pd.DataFrame()
        for stock_type in ["1", "8"]:
            r = requests.get(url=self.stock_list_url.format(stock_type=stock_type), headers=self.headers)
            tmp = pd.read_excel(r.content)
            stock_list = pd.concat([stock_list, tmp])
        stock_list.set_index("A股代码", inplace=True)
        return stock_list

    def get_delist(self) -> pd.DataFrame:
        delist = pd.DataFrame()
        for stock_type in ["1", "8"]:
            r = requests.get(url=self.delist_list_url.format(stock_type=stock_type), headers=self.headers)
            tmp = pd.read_excel(r.content)
            delist = pd.concat([delist, tmp])
        delist.set_index("原公司代码", inplace=True)
        return delist

    def get_single_stock_eod(self, stock: str, date: str = "all") -> pd.DataFrame:
        t = str(time.time() * 1000).split(".")[0]
        r = requests.get(self.stock_eod_url.format(t=t, stock=stock, begin="0"), headers=self.headers)
        data = json.loads(r.text.split("(")[1][:-1])
        eod = pd.DataFrame(data["kline"], columns=["TradingDay", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "Volume", "Turnover"])
        eod["InstrumentID"] = stock
        eod = eod[self.eod_columns]
        if date != "all":
            eod = eod[eod["TradingDay"] == int(date)]
        return eod

    def get_eod(self, date: str) -> pd.DataFrame:
        self.headers["Referer"] = "http://webapi.cninfo.com.cn/"
        self.headers["Accept-Enckey"] = "J72S24+Q+ErL+Dl0bs3i3g=="
        r = requests.get(url=self.eod_url.format(date="-".join([date[:4], date[4:6], date[6:]]), market="SHE"), headers=self.headers)
        eod = pd.DataFrame(r.json()["records"])
        eod["TradingDay"] = date
        eod = eod[["证券代码", "TradingDay", "开盘价", "最高价", "最低价", "收盘价", "成交数量", "成交金额"]]
        eod.columns = self.eod_columns
        eod["InstrumentID"] = eod["InstrumentID"].map(lambda x: x.split("-")[0])
        return eod


if __name__ == "__main__":
    sse = SSE()
    print(sse.get_single_stock_eod("600000", "20230801"))
    print(sse.get_eod("20230801"))
