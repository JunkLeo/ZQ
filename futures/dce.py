# -*- coding:utf-8 -*-
"""
Date: 2023/07/28
Desc: 大商所每日REF/EOD
"""
import requests
import pandas as pd


class DCE:

    def __init__(self):
        self.ci_url = "http://www.dce.com.cn/publicweb/businessguidelines/queryContractInfo.html"
        self.tp_url = "http://www.dce.com.cn/publicweb/notificationtips/queryDayTradPara.html"
        self.eod_url = "http://www.dce.com.cn/publicweb/quotesdata/dayQuotesCh.html"

        self.ci_columns = ["ProductID", "InstrumentID", "Unit", "TickSize", "FirstTradingDay", "LastTradingDay", "LastDeliveryDay"]
        self.tp_columns = ["InstrumentID", "UpperLimitPrice", "LowerLimitPrice", "PositionLimit"]
        self.ref_columns = ["InstrumentID", "ProductID", "Unit", "TickSize", "UpperLimitPrice", "LowerLimitPrice", "PositionLimit", "FirstTradingDay", "LastTradingDay", "LastDeliveryDay"]
        self.eod_columns = ["InstrumentID", "TradingDay", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "PreSettlePrice", "SettlePrice", "Volume", "Turnover", "OpenInterest"]

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
        }

    def get_contract_info(self) -> pd.DataFrame:
        para = {
            "contractInformation.trade_type": "0"
        }
        r = requests.post(url=self.ci_url, data=para, headers=self.headers)
        ci = pd.read_html(r.text)[0]
        ci.columns = self.ci_columns
        ci["ProductID"] = ci["InstrumentID"].map(lambda x: x[:-4])
        return ci

    def get_trade_para(self) -> pd.DataFrame:
        para = {
            "dayTradingParameters.trade_type": "0"
        }
        r = requests.post(url=self.tp_url, data=para, headers=self.headers)
        tp = pd.read_html(r.text)[0]
        tp.columns = ['_'.join(col) for col in tp.columns.values]
        tp = tp[["合约_合约", "涨跌停板_涨停板价位(元)", "涨跌停板_跌停板价位(元)", "持仓限额(手)_客 户"]]
        tp.columns = self.tp_columns
        return tp

    def get_ref(self, product: str = "all") -> pd.DataFrame:
        ci = self.get_contract_info()
        tp = self.get_trade_para()
        ref = pd.merge(ci, tp, on="InstrumentID")
        ref = ref[self.ref_columns]
        if product != "all":
            ref = ref[ref["ProductID"] == product]
        return ref

    def get_eod(self, date: str, product: str = "all") -> pd.DataFrame:
        para = {
            "dayQuotes.trade_type": "0",
            "year": date[:4],
            "month": str(int(date[4:6]) - 1),
            "day": date[6:],
            "currDate": date
        }
        r = requests.post(url=self.eod_url, data=para, headers=self.headers)
        eod = pd.read_html(r.text)[0]
        columns = [
            "ProductID", "InstrumentID", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "PreSettlePrice", "SettlePrice", "Change", "Change1", "Volume", "OpenInterest", "OpenInterestChange",
            "Turnover"
        ]
        eod.columns = columns
        eod = eod[~eod["SettlePrice"].isna()]
        eod["ProductID"] = eod["InstrumentID"].map(lambda x: x[:-4])
        eod["TradingDay"] = date
        if product != "all":
            eod = eod[eod["ProductID"] == product]
        eod = eod[self.eod_columns]
        return eod


if __name__ == "__main__":
    dce = DCE()
    print(dce.get_ref())
    print(dce.get_eod("20230727"))
