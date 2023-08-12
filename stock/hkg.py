# -*- coding:utf-8 -*-
"""
Date: 2023/08/03
Desc: 港交所股票每日EOD
"""
import os
import re
import sys
import time
import json
import requests
import pandas as pd
from collections import defaultdict
pd.set_option('display.unicode.east_asian_width', True)


class HKG:

    def __init__(self):
        self.ref_url = "https://sc.hkex.com.hk/TuniS/www.hkex.com.hk/chi/services/trading/securities/securitieslists/ListOfSecurities_c.xlsx"
        self.eod_url = "https://sc.hkex.com.hk/gb/www.hkex.com.hk/chi/stat/smstat/dayquot/d{YYMMDD}c.htm"

        self.eod_columns = ["InstrumentID", "TradingDay", "Currency", "PreClosePrice", "BidPrice", "AskPrice", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "Volume", "Turnover"]

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
        }

    def get_ref(self) -> pd.DataFrame:
        r = requests.get(self.ref_url, headers=self.headers, timeout=10)
        ref = pd.read_excel(r.content, skiprows=2)
        return ref

    def get_open(self, resp: requests.Response) -> pd.DataFrame:
        lines = resp.text.split("\r\n")
        sep = "-------------------------------------------------------------------------------"
        sep_index = [index for (index, value) in enumerate(lines) if value == sep]
        raw = [line.replace("</font></pre><pre><font size='1'>", "") for line in lines[sep_index[1] + 4:sep_index[2] - 1]]
        raw_records = defaultdict(str)
        for line in raw:
            if "<" in line and not line.startswith("      "):
                instrumentid = line.split()[0].zfill(5)
            raw_records[instrumentid] += line.lstrip()

        records = []
        for instrumentid, trans in raw_records.items():
            op = "0.0"
            record = []
            trans = trans.replace(",", "")
            for i in trans.split():
                if "-" in i and i.split("-")[1].replace(".", "").isdigit():
                    record.append(i)
            for pv in record:
                if pv[0] in ["P", "D", "Y"]:
                    continue
                op = pv.split("-")[1]
                break
            records.append([instrumentid, op])
        ops = pd.DataFrame(records, columns=["InstrumentID", "OpenPrice"]).set_index("InstrumentID")
        return ops

    # missing open price
    def get_eod(self, date: str) -> pd.DataFrame:
        r = requests.get(self.eod_url.format(YYMMDD=date[2:]), headers=self.headers, timeout=30)
        r.encoding = "gbk"
        ops = self.get_open(r)
        lines = r.text.split("\r\n")
        sep = "---------------------------------------------------------------------------------------------------------"
        sep_index = [index for (index, value) in enumerate(lines) if value == sep]
        raw = [line.lstrip("*").replace("</font></pre><pre><font size='1'>", "") for line in lines[sep_index[-3] + 7:sep_index[-2]]]
        records = []
        for line in raw:
            instrumentid = line.split()[0].strip().split("#")[0].zfill(5)
            tmp = line.split()
            if "TRADING SUSPENDED" in line or "TRADING HALTED" in line:
                record = [instrumentid, date, tmp[-4], "0.0", "0.0", "0.0", "0.0", "0.0", "0.0", "0.0", "0.0"]
            else:
                record = [instrumentid, date] + tmp[-9:]
            records.append(record)
        columns = ["InstrumentID", "TradingDay", "Currency", "PreClosePrice", "BidPrice", "AskPrice", "HighPrice", "LowPrice", "ClosePrice", "Volume", "Turnover"]
        eod = pd.DataFrame(records, columns=columns)
        for column in eod.columns[3:]:
            eod[column] = eod[column].map(lambda x: x.strip().replace("N/A", "0.0").replace("-", "0.0").replace(",", ""))
        eod["OpenPrice"] = eod["InstrumentID"].map(lambda x: ops.loc[x, "OpenPrice"] if x in ops.index else "0.0")
        eod = eod[self.eod_columns]
        eod.to_csv(f"/tmp/{date}.csv", index=False)
        return eod


if __name__ == "__main__":
    hkg = HKG()
    #  print(hkg.get_ref())
    print(hkg.get_eod("20230810"))
