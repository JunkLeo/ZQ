# -*- coding:utf-8 -*-
"""
Date: 2023/07/28
Desc: 广期所期货每日REF/EOD
"""
import requests
import pandas as pd
from decimal import Decimal


class GFEX:

    def __init__(self):
        self.ci_url = "http://www.gfex.com.cn/u/interfacesWebTtQueryContractInfo/loadList"
        self.tp_url = "http://www.gfex.com.cn/u/interfacesWebTtQueryTradPara/loadDayList"
        self.eod_url = "http://www.gfex.com.cn/u/interfacesWebTiDayQuotes/loadList"

        self.ci_columns = ["InstrumentID", "ProductID", "Unit", "TickSize", "FirstTradingDay", "LastTradingDay", "LastDeliveryDay"]
        self.tp_columns = ["InstrumentID", "UpperLimitPrice", "LowerLimitPrice", "PositionLimit"]
        self.ref_columns = ["InstrumentID", "ProductID", "Unit", "TickSize", "UpperLimitPrice", "LowerLimitPrice", "PositionLimit", "FirstTradingDay", "LastTradingDay", "LastDeliveryDay"]
        self.eod_columns = ["InstrumentID", "TradingDay", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "PreSettlePrice", "SettlePrice", "Volume", "Turnover", "OpenInterest"]

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
        }

    def get_contract_info(self) -> pd.DataFrame:
        para = {
            "trade_type": ["0"]
        }
        r = requests.post(url=self.ci_url, headers=self.headers, data=para, timeout=10)
        ci = pd.DataFrame(r.json()["data"])
        ci = ci[["contractId", "varietyOrder", "unit", "tick", "startTradeDate", "endTradeDate", "endDeliveryDate0"]]
        ci.columns = self.ci_columns
        return ci

    def get_trade_para(self) -> pd.DataFrame:
        para = {
            "trade_type": ["0"]
        }
        r = requests.post(url=self.tp_url, headers=self.headers, data=para, timeout=10)
        tp = pd.DataFrame(r.json()["data"])
        tp = tp[tp["tradeType"] == "0"]
        tp = tp[["contractId", "riseLimit", "fallLimit", "clientBuySerLimit"]]
        tp.columns = self.tp_columns
        return tp

    def get_ref(self) -> pd.DataFrame:
        ci = self.get_contract_info()
        tp = self.get_trade_para()
        ref = pd.merge(ci, tp, on="InstrumentID")
        ref = ref[self.ref_columns]
        return ref

    def get_eod(self, date: str) -> pd.DataFrame:
        para = {
            "trade_date": [date],
            "trade_type": ["0"]
        }
        r = requests.post(url=self.eod_url, headers=self.headers, data=para, timeout=10)
        eod = pd.DataFrame(r.json()["data"])
        eod["InstrumentID"] = eod.apply(lambda x: x["varietyOrder"].strip() + x["delivMonth"], axis=1)
        eod["TradingDay"] = date
        eod = eod[["InstrumentID", "TradingDay", "open", "high", "low", "close", "lastClear", "clearPrice", "volumn", "turnover", "openInterest"]]
        eod.columns = self.eod_columns
        eod = eod[~eod["SettlePrice"].isna()]
        eod["Turnover"] = eod["Turnover"].map(lambda x: Decimal(str(x)) * 10000)
        return eod


if __name__ == "__main__":
    gfex = GFEX()
    print(gfex.get_ref())
    print(gfex.get_eod("20230728"))
