# -*- coding:utf-8 -*-
"""
Date: 2023/09/22
Desc: 郑商所
"""
import sys
import re
import pandas as pd
from decimal import Decimal
from pathlib import Path
parent_path = str(Path(__file__).parents[0])
sys.path.append(parent_path)
from tools.helper import new_round


class CZCE:

    def __init__(self) -> None:
        self.futures = Futures()
        self.option = Option()


class Futures:

    def __init__(self):
        self.ref_url = "http://www.czce.com.cn/cn/DFSStaticFiles/Future/{year}/{date}/FutureDataReferenceData.htm"
        self.eod_url = "http://www.czce.com.cn/cn/DFSStaticFiles/Future/{year}/{date}/FutureDataDaily.htm"

        self.ref_columns = ["InstrumentID", "ProductID", "Unit", "TickSize", "UpperLimitPrice", "LowerLimitPrice", "PositionLimit", "FirstTradingDay", "LastTradingDay", "LastDeliveryDay"]
        self.eod_columns = ["InstrumentID", "TradingDay", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "PreSettlePrice", "SettlePrice", "Volume", "Turnover", "OpenInterest"]

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
        }

    def get_ref(self, date: str) -> pd.DataFrame:
        ref = pd.read_html(self.ref_url.format(year=date[:4], date=date))[0]
        ref.columns = [col[0] for col in ref.columns.values]
        ref = ref.iloc[:-1, :]
        ref = ref[["合约代码", "产品代码", "交易单位", "最小变动价位", "涨跌停板", "日持仓限额", "第一交易日", "最后交易日", "最后交割日"]]
        ref.columns = ["InstrumentID", "ProductID", "Unit", "TickSize", "UpperLimit", "PositionLimit", "FirstTradingDay", "LastTradingDay", "LastDeliveryDay"]
        ref["Unit"] = ref["Unit"].map(lambda x: re.match("(\d+)(\D+)", x)[1])
        ref["TickSize"] = ref["TickSize"].map(lambda x: re.match("(\d+\.?\d+)(\D+)", x)[1])
        ref["UpperLimit"] = ref["UpperLimit"].map(lambda x: new_round(Decimal(x[1:-1]) / 100))
        ref["PositionLimit"] = ref["PositionLimit"].map(lambda x: re.match("(\D+)(\d+)(\D+)", x)[2])
        for column in ["FirstTradingDay", "LastTradingDay", "LastDeliveryDay"]:
            ref[column] = ref[column].map(lambda x: x.replace("-", ""))
        return ref

    def get_eod(self, date: str) -> pd.DataFrame:
        eod = pd.read_html(self.eod_url.format(year=date[:4], date=date))[0]
        eod = eod.iloc[:-1, :]
        eod["TradingDay"] = date
        columns = ["合约代码", "TradingDay", "今开盘", "最高价", "最低价", "今收盘", "昨结算", "今结算", "成交量(手)", "成交额(万元)", "持仓量"]
        eod = eod[columns]
        eod.columns = self.eod_columns
        eod = eod[~eod["SettlePrice"].isna()]
        eod["Turnover"] = eod["Turnover"].map(lambda x: Decimal(str(x)) * 10000)
        return eod


class Option:

    def __init__(self):
        self.ref_url = "http://www.czce.com.cn/cn/DFSStaticFiles/Option/{year}/{date}/OptionDataReferenceData.htm"
        self.eod_url = "http://www.czce.com.cn/cn/DFSStaticFiles/Option/{year}/{date}/OptionDataDaily.htm"

        self.ref_columns = [
            "InstrumentID", "ProductID", "Unit", "TickSize", "PositionLimit", "FirstTradingDay", "LastTradingDay", "LastDeliveryDay", "CallPut", "StrikePrice", "ExecType", "DeliveryMethod",
            "Underlying", "Margin"
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

    def get_ref(self, date: str) -> pd.DataFrame:
        ref = pd.read_html(self.ref_url.format(year=date[:4], date=date))[0]
        ref.columns = [col[0] for col in ref.columns.values]
        ref = ref[["合约代码", "产品代码", "交易单位", "最小变动价位", "日持仓限额", "第一交易日", "最后交易日", "结算日", "看涨/看跌", "行权价", "行权类别", "结算方式"]]
        ref.columns = self.ref_columns[:-2]
        ref.drop(index=ref[ref["InstrumentID"] == ref["ProductID"]].index, inplace=True)
        ref["Unit"] = ref["Unit"].map(lambda x: re.match("(\d+)(\D+)", x)[1])
        ref["TickSize"] = ref["TickSize"].map(lambda x: re.match("(\d+\.?\d+)(\D+)", x)[1])
        ref["PositionLimit"] = ref["PositionLimit"].map(lambda x: re.match("(\D+)(\d+)(\D+)", x)[2])
        ref["CallPut"] = ref["CallPut"].map(lambda x: "C" if x.strip() == "看涨" else "P")
        ref["ExecType"] = ref["ExecType"].map(lambda x: "American" if x.strip() == "美式" else "European")
        ref["DeliveryMethod"] = ref["ExecType"].map(lambda x: "Physical" if x.strip() == "实物" else "Cash")
        ref["Underlying"] = ref.apply(lambda x: x["InstrumentID"][:len(x["ProductID"]) + 3], axis=1)
        ref["Margin"] = ref.apply(self.calc_margin, axis=1)
        for column in ["FirstTradingDay", "LastTradingDay", "LastDeliveryDay"]:
            ref[column] = ref[column].map(lambda x: x.replace("-", ""))
        return ref

    def get_eod(self, date: str) -> pd.DataFrame:
        eod = pd.read_html(self.eod_url.format(year=date[:4], date=date))[0]
        eod = eod.iloc[:-1, :]
        eod["TradingDay"] = date
        columns = ["合约代码", "TradingDay", "今开盘", "最高价", "最低价", "今收盘", "昨结算", "今结算", "成交量(手)", "成交额(万元)", "持仓量"]
        eod = eod[columns]
        eod.columns = self.eod_columns
        eod = eod[~eod["SettlePrice"].isna()]
        eod["Turnover"] = eod["Turnover"].map(lambda x: Decimal(str(x)) * 10000)
        return eod


if __name__ == "__main__":
    czce = CZCE()
    print(czce.futures.get_ref("20230922"))
    print(czce.futures.get_eod("20230922"))
    print(czce.option.get_ref("20230922"))
    print(czce.option.get_eod("20230922"))
