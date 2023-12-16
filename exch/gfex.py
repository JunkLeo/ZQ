# -*- coding:utf-8 -*-
"""
Date: 2023/09/22
Desc: 广期所
"""
import requests
import pandas as pd
from decimal import Decimal


class GFEX:

    def __init__(self) -> None:
        self.futures = Futures()
        self.option = Option()


class Futures:

    def __init__(self):
        self.ci_url = "http://www.gfex.com.cn/u/interfacesWebTtQueryContractInfo/loadList"
        self.tp_url = "http://www.gfex.com.cn/u/interfacesWebTtQueryTradPara/loadDayList"
        self.eod_url = "http://www.gfex.com.cn/u/interfacesWebTiDayQuotes/loadList"

        self.ci_columns = ["InstrumentID", "ProductID", "Unit", "TickSize", "FirstTradingDay", "LastTradingDay", "LastDeliveryDay"]
        self.tp_columns = ["InstrumentID", "UpperLimitPrice", "LowerLimitPrice", "PositionLimit"]
        self.ref_columns = [
            "InstrumentID",
            "ProductID",
            "Unit",
            "TickSize",
            "UpperLimitPrice",
            "LowerLimitPrice",
            "PositionLimit",
            "FirstTradingDay",
            "LastTradingDay",
            "LastDeliveryDay"
        ]
        self.eod_columns = [
            "InstrumentID",
            "TradingDay",
            "OpenPrice",
            "HighPrice",
            "LowPrice",
            "ClosePrice",
            "PreSettlePrice",
            "SettlePrice",
            "Volume",
            "Turnover",
            "OpenInterest"
        ]

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


class Option:

    def __init__(self):
        self.ci_url = "http://www.gfex.com.cn/u/interfacesWebTtQueryContractInfo/loadList"
        self.tp_url = "http://www.gfex.com.cn/u/interfacesWebTtQueryTradPara/loadDayList"
        self.eod_url = "http://www.gfex.com.cn/u/interfacesWebTiDayQuotes/loadList"

        self.ci_columns = ["InstrumentID", "ProductID", "Unit", "TickSize", "FirstTradingDay", "LastTradingDay"]
        self.tp_columns = ["InstrumentID", "UpperLimitPrice", "LowerLimitPrice", "PositionLimit"]
        self.ref_columns = [
            "InstrumentID",
            "ProductID",
            "Unit",
            "TickSize",
            "UpperLimitPrice",
            "LowerLimitPrice",
            "PositionLimit",
            "FirstTradingDay",
            "LastTradingDay",
            "CallPut",
            "StrikePrice",
            "ExecType",
            "DeliveryMethod",
            "Underlying",
            "Margin"
        ]
        self.eod_columns = [
            "InstrumentID",
            "TradingDay",
            "OpenPrice",
            "HighPrice",
            "LowPrice",
            "ClosePrice",
            "PreSettlePrice",
            "SettlePrice",
            "Volume",
            "Turnover",
            "OpenInterest"
        ]

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
            "trade_type": ["1"]
        }
        r = requests.post(url=self.ci_url, headers=self.headers, data=para, timeout=10)
        ci = pd.DataFrame(r.json()["data"])
        ci = ci[["contractId", "varietyOrder", "unit", "tick", "startTradeDate", "endTradeDate"]]
        ci.columns = self.ci_columns
        return ci

    def get_trade_para(self) -> pd.DataFrame:
        para = {
            "trade_type": ["1"]
        }
        r = requests.post(url=self.tp_url, headers=self.headers, data=para, timeout=10)
        tp = pd.DataFrame(r.json()["data"])
        tp = tp[tp["tradeType"] == "1"]
        tp = tp[["contractId", "riseLimit", "fallLimit", "clientBuySerLimit"]]
        tp.columns = self.tp_columns
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
            "trade_date": [date],
            "trade_type": ["1"]
        }
        r = requests.post(url=self.eod_url, headers=self.headers, data=para, timeout=10)
        eod = pd.DataFrame(r.json()["data"])
        eod["TradingDay"] = date
        eod = eod[["delivMonth", "TradingDay", "open", "high", "low", "close", "lastClear", "clearPrice", "volumn", "turnover", "openInterest"]]
        eod.columns = self.eod_columns
        eod = eod[~eod["SettlePrice"].isna()]
        eod["Turnover"] = eod["Turnover"].map(lambda x: Decimal(str(x)) * 10000)
        return eod


if __name__ == "__main__":
    gfex = GFEX()
    import sys
    from datetime import datetime

    from loguru import logger
    date = sys.argv[1] if len(sys.argv) > 1 else datetime.today().strftime("%Y%m%d")
    logger.info(f"Run {date}")
    print(gfex.futures.get_ref())
    print(gfex.futures.get_eod(date))
    print(gfex.option.get_ref())
    print(gfex.option.get_eod(date))
