# -*- coding:utf-8 -*-
"""
Date: 2023/09/22
Desc: 大商所
"""
from decimal import Decimal

import pandas as pd
import requests


class DCE:

    def __init__(self) -> None:
        self.futures = Futures()
        self.option = Option()


class Futures:

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
        r = requests.post(url=self.ci_url, data=para, headers=self.headers, timeout=10)
        ci = pd.read_html(r.text)[0]
        ci.columns = self.ci_columns
        ci["ProductID"] = ci["InstrumentID"].map(lambda x: x[:-4])
        return ci

    def get_trade_para(self) -> pd.DataFrame:
        para = {
            "dayTradingParameters.trade_type": "0"
        }
        r = requests.post(url=self.tp_url, data=para, headers=self.headers, timeout=10)
        tp = pd.read_html(r.text)[0]
        tp.columns = ['_'.join(col) for col in tp.columns.values]
        tp = tp[["合约_合约", "涨跌停板_涨停板价位(元)", "涨跌停板_跌停板价位(元)", "持仓限额(手)_客 户"]]
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
            "dayQuotes.trade_type": "0",
            "year": date[:4],
            "month": str(int(date[4:6]) - 1),
            "day": date[6:],
            "currDate": date
        }
        r = requests.post(url=self.eod_url, data=para, headers=self.headers, timeout=10)
        eod = pd.read_html(r.text)[0]
        columns = [
            "ProductID", "InstrumentID", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "PreSettlePrice", "SettlePrice", "Change", "Change1", "Volume", "OpenInterest", "OpenInterestChange",
            "Turnover"
        ]
        eod.columns = columns
        eod = eod[~eod["SettlePrice"].isna()]
        eod["ProductID"] = eod["InstrumentID"].map(lambda x: x[:-4])
        eod["TradingDay"] = date
        eod = eod[self.eod_columns]
        eod["Turnover"] = eod["Turnover"].map(lambda x: Decimal(str(x)) * 10000)
        return eod


class Option:

    def __init__(self):
        self.ci_url = "http://www.dce.com.cn/publicweb/businessguidelines/queryContractInfo.html"
        self.tp_url = "http://www.dce.com.cn/publicweb/notificationtips/queryDayTradPara.html"
        self.eod_url = "http://www.dce.com.cn/publicweb/quotesdata/dayQuotesCh.html"

        self.ci_columns = ["ProductID", "InstrumentID", "Unit", "TickSize", "FirstTradingDay", "LastTradingDay", "LastDeliveryDay"]
        self.tp_columns = ["InstrumentID", "UpperLimitPrice", "LowerLimitPrice", "PositionLimit"]
        self.ref_columns = [
            "InstrumentID", "ProductID", "Unit", "TickSize", "UpperLimitPrice", "LowerLimitPrice", "PositionLimit", "FirstTradingDay", "LastTradingDay", "LastDeliveryDay", "CallPut", "StrikePrice",
            "ExecType", "DeliveryMethod", "Underlying", "Margin"
        ]
        self.eod_columns = ["InstrumentID", "TradingDay", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "PreSettlePrice", "SettlePrice", "Volume", "Turnover", "OpenInterest"]

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
        }

    # TODO: Margin
    def calc_margin(self, row):
        """
            Margin
            期货期权卖方交易保证金的收取标准为下列两者中较大者：
            （一）期权合约结算价×标的期货合约交易单位＋标的期货合约交易保证金－期权合约虚值额的一半
            （二）期权合约结算价×标的期货合约交易单位＋标的期货合约交易保证金的一半
            其中：
            看涨期权合约虚值额=Max（行权价格－标的期货合约结算价，0）×标的期货合约交易单位
            看跌期权合约虚值额=Max（标的期货合约结算价－行权价格，0）×标的期货合约交易单位
        """
        return None

    def get_contract_info(self) -> pd.DataFrame:
        para = {
            "contractInformation.trade_type": "1"
        }
        r = requests.post(url=self.ci_url, data=para, headers=self.headers, timeout=10)
        ci = pd.read_html(r.text)[0]
        ci.columns = self.ci_columns
        ci["ProductID"] = ci["InstrumentID"].map(lambda x: x.split("-")[0][:-4])
        ci["LastDeliveryDay"] = ci.apply(lambda x: x["LastTradingDay"] if x["LastDeliveryDay"] == "-" else x["LastDeliveryDay"], axis=1)
        return ci

    def get_trade_para(self) -> pd.DataFrame:
        para = {
            "dayTradingParameters.trade_type": "1"
        }
        r = requests.post(url=self.tp_url, data=para, headers=self.headers, timeout=10)
        tp = pd.read_html(r.text)[0]
        tp.columns = ['_'.join(col) for col in tp.columns.values]
        tp = tp[["合约_合约", "涨跌停板_涨停板价位(元)", "涨跌停板_跌停板价位(元)", "持仓限额(手)_客 户"]]
        tp.columns = self.tp_columns
        tp["PositionLimit"] = tp["PositionLimit"].map(lambda x: x.split("限额")[1].strip())
        return tp

    def get_ref(self) -> pd.DataFrame:
        ci = self.get_contract_info()
        tp = self.get_trade_para()
        ref = pd.merge(ci, tp, on="InstrumentID")
        ref["CallPut"] = ref["InstrumentID"].map(lambda x: x.split("-")[1])
        ref["StrikePrice"] = ref["InstrumentID"].map(lambda x: x.split("-")[-1])
        ref["Underlying"] = ref["InstrumentID"].map(lambda x: x.split("-")[0])
        ref["ExecType"] = "American"
        ref["DeliveryMethod"] = "Physical"
        ref["Margin"] = ref.apply(self.calc_margin, axis=1)
        ref = ref[self.ref_columns]
        return ref

    def get_eod(self, date: str) -> pd.DataFrame:
        para = {
            "dayQuotes.trade_type": "1",
            "year": date[:4],
            "month": str(int(date[4:6]) - 1),
            "day": date[6:],
            "currDate": date
        }
        r = requests.post(url=self.eod_url, data=para, headers=self.headers, timeout=10)
        eod = pd.read_html(r.text)[0]
        columns = [
            "ProductID", "InstrumentID", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "PreSettlePrice", "SettlePrice", "Change", "Change1", "Delta", "Volume", "OpenInterest",
            "OpenInterestChange", "Turnover", "ExecAmount"
        ]
        eod.columns = columns
        eod = eod[~eod["SettlePrice"].isna()]
        eod["ProductID"] = eod["InstrumentID"].map(lambda x: x.split("-")[0][:-4])
        eod["TradingDay"] = date
        eod = eod[self.eod_columns]
        for volumn in ["OpenPrice", "HighPrice", "LowPrice"]:
            eod[volumn] = eod[volumn].map(lambda x: "0.0" if x == "-" else x)
        eod["Turnover"] = eod["Turnover"].map(lambda x: Decimal(str(x)) * 10000)
        return eod


if __name__ == "__main__":
    dce = DCE()
    import sys
    from datetime import datetime

    from loguru import logger
    date = sys.argv[1] if len(sys.argv) > 1 else datetime.today().strftime("%Y%m%d")
    logger.info(f"Run {date}")
    print(dce.futures.get_ref())
    print(dce.futures.get_eod(date))
    print(dce.option.get_ref())
    print(dce.option.get_eod(date))
