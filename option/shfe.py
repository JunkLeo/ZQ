# -*- coding:utf-8 -*-
"""
Date: 2023/07/31
Desc: 上期所期权每日REF/EOD
"""
import os
import sys
import requests
import pandas as pd
from pathlib import Path
from decimal import Decimal
from math import ceil, floor
parent_path = str(Path(__file__).parents[1])
sys.path.append(parent_path)
from tools.tradingday import CN_TradingDay
from tools.helper import new_round
config_path = os.path.join(parent_path, "config")


class SHFE:

    def __init__(self):
        self.config_file = os.path.join(config_path, "shfe.csv")
        self.ci_url = "https://www.shfe.com.cn/data/instrument/option/ContractBaseInfo{date}.dat"
        self.tp_url = "https://www.shfe.com.cn/data/instrument/option/ContractDailyTradeArgument{date}.dat"
        self.eod_url = "https://www.shfe.com.cn/data/dailydata/option/kx/kx{date}.dat"

        self.ci_columns = ["InstrumentID", "ProductID", "Unit", "TickSize", "FirstTradingDay", "LastTradingDay"]
        self.tp_columns = ["InstrumentID", "UpperLimitPrice", "LowerLimitPrice"]
        self.ref_columns = [
            "InstrumentID", "ProductID", "Unit", "TickSize", "UpperLimitPrice", "LowerLimitPrice", "PositionLimit", "FirstTradingDay", "LastTradingDay", "CallPut", "StrikePrice", "ExecType",
            "DeliveryMethod", "Underlying", "Margin"
        ]
        self.eod_columns = ["InstrumentID", "TradingDay", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice", "PreSettlePrice", "SettlePrice", "Volume", "Turnover", "OpenInterest"]

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
        }

        self.tradingday = CN_TradingDay()

    def calc_margin(self, row):
        """
            Margin TODO
            期货期权卖方交易保证金的收取标准为下列两者中较大者：
            （一）期权合约结算价×标的期货合约交易单位＋标的期货合约交易保证金－期权合约虚值额的一半
            （二）期权合约结算价×标的期货合约交易单位＋标的期货合约交易保证金的一半
            其中：
            看涨期权合约虚值额=Max（行权价格－标的期货合约结算价，0）×标的期货合约交易单位
            看跌期权合约虚值额=Max（标的期货合约结算价－行权价格，0）×标的期货合约交易单位
        """
        return None

    def get_contract_info(self, date: str) -> pd.DataFrame:
        r = requests.get(url=self.ci_url.format(date=date), headers=self.headers, timeout=10)
        ci = pd.DataFrame(r.json()["OptionContractBaseInfo"])
        ci = ci[["INSTRUMENTID", "COMMODITYID", "TRADEUNIT", "PRICETICK", "OPENDATE", "EXPIREDATE"]]
        ci["PRICETICK"] = ci["PRICETICK"].astype(float)
        ci.columns = self.ci_columns
        return ci

    def get_trade_para(self, date: str) -> pd.DataFrame:
        r = requests.get(url=self.tp_url.format(date=date), headers=self.headers, timeout=10)
        tp = pd.DataFrame(r.json()["OptionContractDailyTradeArgument"])
        tp = tp[["INSTRUMENTID", "UPPERVALUE", "LOWERVALUE"]]
        tp["UPPERVALUE"] = tp["UPPERVALUE"].astype(float)
        tp["LOWERVALUE"] = tp["LOWERVALUE"].astype(float)
        tp.columns = self.tp_columns
        return tp

    def get_ref(self, date: str) -> pd.DataFrame:
        ci = self.get_contract_info(date)
        tp = self.get_trade_para(date)
        ref = pd.merge(ci, tp, on="InstrumentID")
        ref["PositionLimit"] = None
        ref["CallPut"] = ref["InstrumentID"].map(lambda x: "C" if "C" in x else "P")
        ref["StrikePrice"] = ref["InstrumentID"].map(lambda x: x.split("C")[1] if "C" in x else x.split("P")[1])
        ref["Underlying"] = ref["InstrumentID"].map(lambda x: x.split("C")[0] if "C" in x else x.split("P")[0])
        ref["ExecType"] = "American"
        ref["DeliveryMethod"] = "Physical"
        ref["Margin"] = ref.apply(self.calc_margin, axis=1)
        ref = ref[self.ref_columns]
        return ref

    def get_eod(self, date: str) -> pd.DataFrame:
        r = requests.get(url=self.eod_url.format(date=date), headers=self.headers, timeout=10)
        eod = pd.DataFrame(r.json()["o_curinstrument"])
        eod["TradingDay"] = date
        eod = eod[["INSTRUMENTID", "TradingDay", "OPENPRICE", "HIGHESTPRICE", "LOWESTPRICE", "CLOSEPRICE", "PRESETTLEMENTPRICE", "SETTLEMENTPRICE", "VOLUME", "TURNOVER", "OPENINTEREST"]]
        eod.columns = self.eod_columns
        eod = eod[eod["SettlePrice"].str.len() != 0]
        eod["InstrumentID"] = eod["InstrumentID"].map(lambda x: x.strip())
        for volumn in ["OpenPrice", "HighPrice", "LowPrice"]:
            eod[volumn] = eod[volumn].map(lambda x: "0.0" if not x else x)
        eod["Turnover"] = eod["Turnover"].map(lambda x: Decimal(str(x)) * 10000)
        return eod


if __name__ == "__main__":
    shfe = SHFE()
    print(shfe.get_ref("20230728"))
    print(shfe.get_eod("20230728"))
