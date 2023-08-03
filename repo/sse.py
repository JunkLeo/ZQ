# -*- coding:utf-8 -*-
"""
Date: 2023/08/03
Desc: 上交所债券回购每日EOD
"""
import os
import sys
import time
import json
import requests
import pandas as pd


class SSE:

    def __init__(self):
        self.repo_list_url = "http://query.sse.com.cn/commonQuery.do?jsonCallBack=jsonpCallback68949527&isPagination=true&pageHelp.pageSize=1000&pageHelp.pageNo=1&sqlId=COMMON_SSE_ZQPZ_ZQLB_ZQHGLB_TOTAL&_={t}"

        self.eod_url = "http://yunhq.sse.com.cn:32041/v1/shb1/dayk/{repo}?callback=jQuery11240494446136766604_{t}&begin=0&end=-1&period=day&_={t}"

        self.eod_columns = ["InstrumentID", "TradingDay", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "Volume", "Turnover"]

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
            "Referer": "http://www.sse.com.cn/"
        }

    def get_repo_list(self) -> pd.DataFrame:
        t = str(time.time() * 1000).split(".")[0]
        r = requests.get(url=self.repo_list_url.format(t=t), headers=self.headers, timeout=10)
        data = json.loads(r.text.split("(", 1)[1][:-1])
        repo_list = pd.DataFrame(data["result"])
        repo_list.set_index("BOND_ID", inplace=True)
        return repo_list

    def get_single_repo_eod(self, repo: str, date: str = "all") -> pd.DataFrame:
        t = str(time.time() * 1000).split(".")[0]
        r = requests.get(self.eod_url.format(t=t, repo=repo), headers=self.headers, timeout=10)
        data = json.loads(r.text.split("(")[1][:-1])
        eod = pd.DataFrame(data["kline"], columns=["TradingDay", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "Volume", "Turnover"])
        eod["InstrumentID"] = repo
        eod = eod[self.eod_columns]
        if date != "all":
            eod = eod[eod["TradingDay"] == int(date)]
        return eod

    def get_eod(self, date: str = "all") -> pd.DataFrame:
        repo_list = self.get_repo_list()
        eod = pd.DataFrame()
        for repo in repo_list.index:
            retry = 0
            while retry < 3:
                try:
                    df = self.get_single_repo_eod(repo, date)
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


if __name__ == "__main__":
    sse = SSE()
    #  print(sse.get_repo_list())
    print(sse.get_eod())
